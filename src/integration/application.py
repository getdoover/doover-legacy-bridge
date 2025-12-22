import logging
import time

from datetime import datetime, timezone

from pydoover.cloud.processor import Application, IngestionEndpointEvent

from legacy_bridge_common.utils import parse_file, nested_find_replace

log = logging.getLogger()


class DooverLegacyBridgeApplication(Application):
    async def setup(self):
        self._record_tag_update = False

    async def on_ingestion_endpoint(self, event: IngestionEndpointEvent):
        if event.payload["event_name"] != "relay_channel_message":
            log.info(f"Unknown event: {event.payload}.")
            return

        legacy_agent_id = event.payload["agent_key"]
        org_config = await self.get_tag("legacy_agent_lookup", {})
        try:
            agent_id = org_config[legacy_agent_id]
        except KeyError:
            log.info(f"Payload: {event.payload}")
            log.error(f"Unknown legacy agent key: {legacy_agent_id}")
            return

        channel_name = event.payload["channel_name"]
        payload = event.payload["data"]

        if channel_name == "ui_cmds":
            # basically, doover 2.0 doesn't want the 'cmds' nested key.
            # it also adds it back in when publishing to doover 1.0 because it knows we need it here.
            # however, we should be nice and remove the cmds key for now
            try:
                payload = payload["cmds"]
            except KeyError:
                pass

        if channel_name == "ui_state":
            try:
                state = payload["state"]
            except KeyError:
                pass
            else:
                nested_find_replace(state, "componentUrl", "https://getdoover.github.io/cameras/HLSLiveView.js", "https://getdoover.github.io/cameras/LiveViewV2.js")
            # payload = nested_find_replace(payload, "componentUrl", "https://getdoover.github.io/cameras/HLSLiveView.js", "https://getdoover.github.io/cameras/LiveViewV2.js")

        payload["doover_legacy_bridge_at"] = time.time() * 1000

        try:
            ts_data = event.payload["timestamp"]
        except KeyError:
            ts = datetime.now(timezone.utc)
        else:
            ts = datetime.fromtimestamp(ts_data / 1000.0).astimezone(timezone.utc)

        record_log = event.payload["record_log"]
        is_diff = event.payload["is_diff"]

        log.info(
            f"Relaying, agent: {agent_id}, channel: {channel_name}, message: {payload}, ts: {ts}, record_log: {record_log}, is_diff: {is_diff}"
        )

        if "output_type" in payload and "output" in payload:
            payload, file = parse_file(channel_name, payload)
            await self.api.publish_message(
                agent_id,
                channel_name,
                message=payload,
                files=[file],
                timestamp=ts,
                record_log=True,
                is_diff=False,
            )
        else:
            await self.api.publish_message(
                agent_id,
                channel_name,
                message=payload,
                timestamp=ts,
                record_log=record_log,
                is_diff=is_diff,
            )

        num_messages = await self.get_tag(f"num_messages_synced_{agent_id}", 0)
        await self.set_tag(f"num_messages_synced_{agent_id}", num_messages + 1)
        await self.api.publish_message(
            agent_id,
            "tag_values",
            message={
                self.app_key: {
                    "num_messages_synced": num_messages + 1,
                    "last_message_dt": datetime.now(timezone.utc).timestamp(),
                }
            },
        )
        await self.set_tag(
            "imported_messages", (await self.get_tag("imported_messages", 0)) + 1
        )
        log.info("Successfully forwarded message from Doover 1.0.")
