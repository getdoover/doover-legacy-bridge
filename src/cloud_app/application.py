import logging
import time
from zoneinfo import ZoneInfo

from pydoover.cloud.processor import (
    Application,
    MessageCreateEvent,
)
from pydoover.cloud.processor.data_client import (
    ConnectionStatus,
    ConnectionDetermination,
)
from pydoover.cloud.processor.types import (
    ScheduleEvent,
    ConnectionType,
    ConnectionConfig,
)
from pydoover.cloud.api import Client
from datetime import datetime, timezone, timedelta

from legacy_bridge_common.utils import get_connection_info
from .app_config import DooverLegacyBridgeConfig

log = logging.getLogger()

# this uuid is the equivalent of "doover 2.0 is watching the device"
# when handling legacy ui_state@wss_connection channel
UI_FASTMODE_AGENT_KEY = "df4108e0-7bef-459c-8b58-5c26360517e7"


class DooverLegacyBridgeApplication(Application):
    config: DooverLegacyBridgeConfig

    @property
    def legacy_client(self):
        if self._legacy_client is None:
            self._legacy_client = Client(
                token=self.config.legacy_api_key.value,
                base_url=self.config.legacy_api_url.value,
            )
        return self._legacy_client

    async def setup(self):
        if self.config.legacy_agent_key.value == "placeholder":
            log.info("Legacy agent key not set, not connecting to Doover 1.0.")
            return

        # only make this when we actually need it because it takes forever (~2s)...
        self._legacy_client = None

        num_imported_messages = await self.get_tag("imported_messages")
        if num_imported_messages is None:
            await self.set_tag("imported_messages", 0)

    async def close(self):
        if self._legacy_client:
            self._legacy_client.session.close()

    async def pre_hook_filter(self, event):
        if isinstance(event, MessageCreateEvent):
            if (
                event.channel_name == "ui_cmds"
                and "doover_legacy_bridge_at" in event.message.diff
            ):
                # ignore any messages originating from doover 1.0
                return False

            if (
                event.channel_name == "ui_state-wss_connections"
                and "doover_legacy_bridge_at" not in event.message.diff
            ):
                # we only care about messages originating from doover 1.0
                return False

            if event.channel_name == "ui_state" and not (
                get_connection_info(event.message.data)
                and "doover_legacy_bridge_at" in event.message.diff
            ):
                # if this is a processor-based application we need to reach into ui_state and fetch any connection info
                # so we can update the connection status
                # we also only care about any messages from doover 1.0
                log.info(
                    "Message was for ui_state but no connection config was defined. Skipping..."
                )
                return False

        return True

    #
    # async def on_ingestion_endpoint(self, event: IngestionEndpointEvent):
    #     if event.payload["event_name"] != "relay_channel_message":
    #         log.info(f"Unknown event: {event.payload}.")
    #         return
    #
    #     legacy_agent_id = event.payload["agent_key"]
    #     if legacy_agent_id == self.config.legacy_agent_key.value:
    #         agent_id = self.agent_id
    #     else:
    #         log.error(f"Unknown legacy agent key: {legacy_agent_id}")
    #         return
    #
    #     channel_name = event.payload["channel_name"]
    #     payload = event.payload["data"]
    #     if not isinstance(payload, dict):
    #         log.error(
    #             f"Message '{payload}' not of correct type (object): '{type(payload)}'"
    #         )
    #         return
    #     payload["doover_legacy_bridge_at"] = time.time() * 1000
    #
    #     await self.api.publish_message(
    #         agent_id,
    #         channel_name,
    #         data=payload,
    #         timestamp=event.payload["timestamp"],
    #         record_log=event.payload["record_log"],
    #         is_diff=event.payload["is_diff"],
    #     )
    #     await self.set_tag(
    #         "imported_messages", await self.get_tag("imported_messages") + 1
    #     )
    #
    #     ts = event.payload["timestamp"] or datetime.now(tz=timezone.utc).timestamp()
    #     if ts > await self.get_tag(f"last_message_{channel_name}", 0):
    #         await self.set_tag(f"last_message_{channel_name}", ts)
    #     await self.set_tag(
    #         f"last_sync_{channel_name}", datetime.now(timezone.utc).timestamp()
    #     )
    #
    #     log.info("Successfully forwarded message from Doover 1.0.")

    async def on_message_create(self, event: MessageCreateEvent):
        # if self.config.import_mode.value is True:
        #     # this should never get here because we shouldn't be subscribed to messages
        #     # when import mode is enabled
        #     log.info("Import mode enabled, not processing messages.")
        #     return
        if self.config.legacy_agent_key.value == "placeholder":
            log.info("Legacy agent key not set, not running processor.")
            return

        payload = event.message.data
        log.info(f"Received new message on channel: {event.channel_name}")

        if event.channel_name == "ui_state":
            # if this is a processor-based application we need to reach into ui_state and fetch any connection info
            # so we can update the connection status
            config = ConnectionConfig.from_v1(get_connection_info(event.message.data))
            if config.connection_type is not ConnectionType.continuous:
                # doover 1.0 gets the 'last ping' from the 'last ui state publish' for non-continuous connections
                # so just mimic that here.
                log.info("Detected ui_state message on period connection. Pinging...")
                await self.ping_connection()

            if config == self.connection_config:
                log.info("Connection config unchanged. Skipping...")
                return

            # we could probably combine this with the ping above
            await self.api.update_connection_config(self.agent_id, config)

        if event.channel_name == "ui_state-wss_connections":
            # there's no amazing way to do this, so just do it how doover 1.0 does it - sync based on wss_conn channel
            try:
                online = event.message.data["connections"][
                    self.config.legacy_agent_key.value
                ]
            except KeyError:
                online = False

            # hmm... this works for docker / wss based devices, but not schedules / processors.
            # fixme: fix above
            if online:
                status, determination = (
                    ConnectionStatus.continuous_online,
                    ConnectionDetermination.online,
                )
            else:
                status, determination = (
                    ConnectionStatus.continuous_offline,
                    ConnectionDetermination.offline,
                )

            await self.api.ping_connection_at(
                self.agent_id,
                datetime.now(timezone.utc),
                status,
                determination,
                user_agent="doover-legacy-bridge;forwarded-message",
                organisation_id=self.organisation_id,
            )

        if event.channel_name == "ui_cmds":
            if "doover_legacy_bridge_at" in event.message.diff:
                log.info("Ignoring message originally synced from Doover 1.0")
                return

            log.info(
                f"Forwarding message to Doover 1.0: agent: {self.config.legacy_agent_key.value}, channel: {event.channel_name}, diff: {event.message.diff}"
            )
            # this sucks but doover 1.0 wraps everything inside a "cmds" struct, so just replicate that...
            message = {
                "cmds": event.message.diff,
                "doover_legacy_bridge2_at": time.time() * 1000,
            }

            if self.config.read_only.value:
                log.info("Read only mode enabled, not writing message to Doover 1.0.")
                return

            self.legacy_client.publish_to_channel_name(
                self.config.legacy_agent_key.value,
                event.channel_name,
                message,
            )

        if event.channel_name == "trigger_manual_sync":
            log.info("Manual sync requested")
            await self.sync_channel("ui_state")
            await self.sync_channel("ui_cmds")
            await self.determine_online_status()

        if event.channel_name == "doover_ui_fastmode":
            now = datetime.now(timezone.utc)
            if any(
                datetime.fromtimestamp(v / 1000, timezone.utc) - now
                < timedelta(minutes=2)
                for v in payload.values()
            ):
                # set user as connected to ui_state@wss_connections and ui_cmds@wss_connections
                data = {"connections": {UI_FASTMODE_AGENT_KEY: True}}
                run_fastmode = True
            else:
                # unset them as not connected
                data = {"connections": {UI_FASTMODE_AGENT_KEY: None}}
                run_fastmode = False

            current_value = await self.get_tag("legacy_fastmode_sync_enabled")

            if current_value is None or current_value != run_fastmode:
                log.info(f"Publishing run_fastmode: {run_fastmode} to wss_connections.")

                if self.config.read_only.value:
                    log.info(
                        "Read only mode enabled, not writing message to Doover 1.0."
                    )
                    return

                self.legacy_client.publish_to_channel_name(
                    self.config.legacy_agent_key.value, "ui_state@wss_connections", data
                )
                # self.legacy_client.publish_to_channel_name(
                #     self.config.legacy_agent_key.value, "ui_cmds@wss_connections", data
                # )

                # important to set and publish this before we go running the sync loop to make sure
                # it doesn't get run more than once
                await self.set_tag("legacy_fastmode_sync_enabled", run_fastmode)
                await self.api.publish_message(
                    self.agent_id, "tag_values", self._tag_values
                )

    async def on_schedule(self, event: ScheduleEvent):
        if self.config.legacy_agent_key.value == "placeholder":
            log.info("Legacy agent key not set, not running processor.")
            return

        # don't run on a schedule any quicker than 5min
        last_ran = await self.get_last_ran()
        if last_ran and last_ran - datetime.now(timezone.utc) > timedelta(minutes=5):
            return

        await self.sync_channel("ui_state")
        await self.sync_channel("ui_cmds")
        await self.set_tag("last_ui_sync", datetime.now(timezone.utc).timestamp())
        await self.determine_online_status()

    async def sync_channel(self, channel_name):
        channel_id = await self.get_or_fetch_channel_id(channel_name)
        start_time = await self.get_last_message_timestamp(channel_name)
        log.info(f"Syncing channel: {channel_name} since {start_time}")

        start = start_time
        end = min(datetime.now(timezone.utc), start_time + timedelta(days=1))
        # if this is within today it'll report 0 which is no loop...
        for day in range((datetime.now(timezone.utc) - start_time).days or 1):
            log.info(
                f"Getting messages for channel {channel_name} from start: {start} to end: {end}"
            )

            # pretty sure Doover 1.0 operates with `.now()` which uses AEST because the lambda is in ap-southeast-2
            start_aest = start.astimezone(ZoneInfo("Australia/Sydney"))
            end_aest = end.astimezone(ZoneInfo("Australia/Sydney"))

            messages = self.legacy_client.get_channel_messages_in_window(
                channel_id, start_aest, end_aest
            )

            log.info(f"Processing {len(messages)} messages...")
            for message in messages:
                # .fetch_payload() won't do an api call because the endpoint above returns the message content
                # inject in another key so we know not to publish it back to Doover 1.0
                payload = message.fetch_payload()
                if isinstance(payload, dict):
                    payload["doover_legacy_bridge_at"] = int(
                        datetime.now(timezone.utc).timestamp() * 1000
                    )

                await self.api.publish_message(
                    self.agent_id,
                    channel_name,
                    payload,
                    message.timestamp,
                )

            start = end
            end = min(datetime.now(timezone.utc), end + timedelta(days=1))

            if len(messages) > 0:
                await self.set_tag(
                    "imported_messages",
                    await self.get_tag("imported_messages") + len(messages),
                )
                await self.set_tag(
                    f"last_message_{channel_name}",
                    max(m.timestamp for m in messages).timestamp(),
                )
                await self.set_tag(
                    f"last_sync_{channel_name}", datetime.now(timezone.utc).timestamp()
                )
                await self.api.publish_message(
                    self.agent_id, "tag_values", self._tag_values
                )

    async def determine_online_status(self):
        # first step, see if it's a DDA device with a websocket channel
        channel_id = await self.get_or_fetch_channel_id("ui_state@wss_connections")
        data = self.legacy_client.client.get_channel(channel_id).fetch_aggregate()
        try:
            data[self.config.legacy_agent_key]
        except KeyError:
            pass
        else:
            await self.ping_connection()

        # otherwise just set it to whatever the last ui_state message was
        await self.ping_connection(await self.get_last_message_timestamp("ui_state"))

    async def get_or_fetch_channel_id(self, channel_name):
        # these will never change so we should be good to cache them, it'll save an api request in future.
        channel_id = await self.get_tag(f"{channel_name}_channel_id")
        if not channel_id:
            channel = self.legacy_client.get_channel_named(
                channel_name, self.config.legacy_agent_key.value
            )
            channel_id = channel.id
            await self.set_tag(f"{channel_name}_channel_id", channel_id)
        return channel_id

    async def get_last_message_timestamp(self, channel_name):
        last_message = await self.get_tag(f"last_message_{channel_name}")
        if not last_message:
            # don't sync anything more than a month ago if in import mode, or since today otherwise.
            if self.config.import_mode.value:
                days = 30
            else:
                days = 1

            last_message = datetime.now(timezone.utc) - timedelta(days=days)
        else:
            last_message = datetime.fromtimestamp(last_message, timezone.utc)

        return last_message

    async def get_last_ran(self):
        last_ran = await self.get_tag("last_ui_sync")
        if last_ran:
            last_ran = datetime.fromtimestamp(last_ran, timezone.utc)
        return last_ran
