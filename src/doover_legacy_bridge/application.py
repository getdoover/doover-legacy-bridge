import asyncio
import logging

from pydoover.cloud.processor import (
    Application,
    MessageCreateEvent,
)
from pydoover.cloud.processor.types import ScheduleEvent
from pydoover.cloud.api import Client
from datetime import datetime, timezone, timedelta

from .app_config import DooverLegacyBridgeConfig

log = logging.getLogger()

# this uuid is the equivalent of "doover 2.0 is watching the device"
# when handling legacy ui_state@wss_connection channel
UI_FASTMODE_AGENT_KEY = "df4108e0-7bef-459c-8b58-5c26360517e7"

class DooverLegacyBridgeApplication(Application):
    config: DooverLegacyBridgeConfig

    async def setup(self):
        self.legacy_client = Client()

    async def close(self):
        # update device as being online
        # await self.ping_connection(max(t.timestamp for t in self.device.tag_frames))

        self.legacy_client.session.close()

    async def on_message_create(self, message: MessageCreateEvent):
        payload = message.message.data
        if message.channel_name in ("ui_state", "ui_cmds"):
            self.legacy_client.publish_to_channel_name(self.config.legacy_agent_key.value, message.channel_name, message.message.data)

        if message.channel_name == "doover_ui_fastmode":
            now = datetime.now(timezone.utc)
            if any(datetime.fromtimestamp(v / 1000) - now > timedelta(minutes=2) for v in payload.values()):
                # set user as connected to ui_state@wss_connections and ui_cmds@wss_connections
                data = {UI_FASTMODE_AGENT_KEY: True}
                run_fastmode = True
            else:
                # unset them as not connected
                data = {UI_FASTMODE_AGENT_KEY: False}
                run_fastmode = False

            self.legacy_client.publish_to_channel_name(self.config.legacy_agent_key.value, "ui_state@wss_connections", data)
            self.legacy_client.publish_to_channel_name(self.config.legacy_agent_key.value, "ui_cmds@wss_connections", data)

            if run_fastmode:
                for _ in range(28):
                    # this is so hacky, potentially expensive and definitely bad practice (sleeping in lambda)
                    # but it'll hopefully do...
                    # update ui every 10 seconds for 4min 50sec (to allow cleanup before 5min limit)
                    # if the user wants to keep watching after the 3min they need to
                    # click out and click back into the device.
                    await asyncio.sleep(10)
                    last_fetched = await self.get_tag("last_ui_sync")
                    await self.sync_channel("ui_state", last_fetched)
                    await self.sync_channel("ui_cmds", last_fetched)
                    await self.set_tag(
                        "last_ui_sync", datetime.now(timezone.utc).timestamp()
                    )

    async def on_schedule(self, event: ScheduleEvent):
        last_fetched = await self.get_tag("last_ui_sync")
        if not last_fetched:
            # don't sync anything more than a year ago
            last_fetched = datetime.now(timezone.utc) - timedelta(days=365)
        else:
            last_fetched = datetime.fromtimestamp(last_fetched)

        # don't run on a schedule any quicker than 5min
        if last_fetched - datetime.now(timezone.utc) > timedelta(minutes=5):
            return

        await self.sync_channel("ui_state", last_fetched)
        await self.sync_channel("ui_cmds", last_fetched)
        await self.set_tag("last_ui_sync", datetime.now(timezone.utc).timestamp())

    async def sync_channel(self, channel_name, start_time: datetime):
        channel_id = await self.get_or_fetch_channel_id(channel_name)

        start = start_time
        end = min(datetime.now(timezone.utc), start_time + timedelta(days=1))
        for day in range((datetime.now(timezone.utc) - start_time).days):
            messages = self.legacy_client.get_channel_messages_in_window(channel_id, start, end)
            for message in messages:
                # .fetch_payload() won't do an api call because the endpoint above returns the message content
                await self.api.publish_message(self.agent_id, channel_name, message.fetch_payload())

            start = end
            end = min(datetime.now(timezone.utc), end + timedelta(days=1))

    async def get_or_fetch_channel_id(self, channel_name):
        # these will never change so we should be good to cache them, it'll save an api request in future.
        channel_id = await self.get_tag(f"{channel_name}_channel_id")
        if not channel_id:
            channel = self.legacy_client.get_channel_named(self.config.legacy_agent_key.value, channel_name)
            channel_id = channel.id
            await self.set_tag(f"{channel_name}_channel_id", channel_id)
        return channel_id
