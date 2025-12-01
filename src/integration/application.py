import logging
import time

from datetime import datetime, timezone

from pydoover.cloud.processor import Application, IngestionEndpointEvent

log = logging.getLogger()


class DooverLegacyBridgeApplication(Application):
    async def on_ingestion_endpoint(self, event: IngestionEndpointEvent):
        if event.payload["event_name"] != "relay_channel_message":
            log.info(f"Unknown event: {event.payload}.")
            return

        legacy_agent_id = event.payload["agent_key"]
        org_config = await self.get_tag("legacy_agent_lookup", {})
        try:
            agent_id = org_config[legacy_agent_id]
        except KeyError:
            log.error(f"Unknown legacy agent key: {legacy_agent_id}")
            return

        channel_name = event.payload["channel_name"]
        payload = event.payload["data"]
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
        log.info(f"Token: {self.api.session.headers['Authorization']}")
        await self.api.publish_message(
            agent_id,
            channel_name,
            message=payload,
            timestamp=ts,
            record_log=record_log,
            is_diff=is_diff,
        )
        await self.set_tag(
            "imported_messages", (await self.get_tag("imported_messages", 0)) + 1
        )
        log.info("Successfully forwarded message from Doover 1.0.")
