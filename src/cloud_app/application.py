import logging
import re
import time

from pydoover.cloud.processor import (
    Application,
    MessageCreateEvent,
)
from pydoover.cloud.processor.types import (
    ConnectionStatus,
    ConnectionDetermination,
    ScheduleEvent,
    ConnectionType,
    ConnectionConfig,
)
from pydoover.cloud.api import Client, NotFound
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
                get_connection_info(event.message.data.get("state", {}))
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

    async def on_message_create(self, event: MessageCreateEvent):
        if self.config.legacy_agent_key.value == "placeholder":
            log.info("Legacy agent key not set, not running processor.")
            return

        payload = event.message.data
        log.info(f"Received new message on channel: {event.channel_name}")

        if event.channel_name == "ui_state":
            # if this is a processor-based application we need to reach into ui_state and fetch any connection info
            # so we can update the connection status
            config = ConnectionConfig.from_v1(
                get_connection_info(payload.get("state", {}))
            )
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
            if self.connection_config.connection_type is ConnectionType.periodic:
                log.info(
                    "Detected ui_state-wss_connections message on period connection. Skipping..."
                )
                return

            # there's no amazing way to do this, so just do it how doover 1.0 does it - sync based on wss_conn channel
            try:
                online = event.message.data["connections"][
                    self.config.legacy_agent_key.value
                ]
            except KeyError:
                online = False

            if online:
                # we need to do no ping because otherwise doover 2.0 will mark the connection as offline
                # if a message doesn't get published to this channel at least once every few minutes.
                status, determination = (
                    ConnectionStatus.continuous_online_no_ping,
                    ConnectionDetermination.online,
                )
            elif (
                self.connection_config.connection_type
                is ConnectionType.periodic_continuous
            ):
                status, determination = (
                    ConnectionStatus.continuous_pending,
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
            log.info(f"Manual sync requested. Token: {self.api.session.headers['Authorization']}")
            await self.on_manual_invoke()

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
        # don't do anything on a schedule, we should get 100% of the messages from doover 1.0
        # and the user can request a manual sync if they want
        return

    async def on_manual_invoke(self):
        try:
            agent = self.legacy_client.client.get_agent(
                self.config.legacy_agent_key.value
            )
        except NotFound:
            log.info("Legacy agent not found, skipping...")
            return

        for channel in agent.channels:
            await self.sync_channel(channel.name)

    async def sync_channel(self, channel_name):
        try:
            ch = self.legacy_client.client.get_channel_named(
                channel_name, self.config.legacy_agent_key.value
            )
        except NotFound:
            log.info(f"Channel {channel_name} not found, skipping...")
            return


        channel_name = re.sub(r'[^a-zA-Z0-9_-]', '-', channel_name)

        data = ch.fetch_aggregate()
        if not isinstance(data, dict):
            logging.info("Discarding message since doover data will just discard it...")
            return

        log.info(f"Fetched channel {channel_name} from doover 1.0, sending to doover 2.0...")

        if channel_name == "tag_values":
            if self.app_key not in data:
                data[self.app_key] = {}
            # if we don't do this we'll overwrite the existing counters!
            data[self.app_key]["last_manual_sync"] = datetime.now(timezone.utc).timestamp()
            data[self.app_key]["num_messages_synced"] = (await self.get_tag("num_messages_synced", 0)) + 1
            data[self.app_key]["last_message_dt"] = await self.get_tag("last_message_dt")

        # do a hard sync, record the log and don't diff.
        await self.api.publish_message(
            self.agent_id,
            channel_name,
            data,
            record_log=True,
            is_diff=False,
        )

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
