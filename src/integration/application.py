import logging

from pydoover.cloud.processor import Application, IngestionEndpointEvent

log = logging.getLogger()


class DooverLegacyBridgeApplication(Application):
    async def on_ingestion_endpoint(self, event: IngestionEndpointEvent):
        if event.payload["event_name"] != "relay_channel_message":
            log.info(f"Unknown event: {event.payload}.")
            return

        legacy_agent_id = event.payload["agent_key"]

        org_config = await self.get_tag("agent_lookup", {})
        try:
            agent_id = org_config[legacy_agent_id]
        except KeyError:
            log.error(f"Unknown legacy agent key: {legacy_agent_id}")
            return

        channel_name = event.payload["channel_name"]
        payload = event.payload["data"]
        payload["imported_via_integration"] = True

        await self.api.publish_message(
            agent_id,
            channel_name,
            data=payload,
            timestamp=event.payload["timestamp"],
            record_log=event.payload["record_log"],
            is_diff=event.payload["is_diff"],
        )
        await self.set_tag(
            "imported_messages", (await self.get_tag("imported_messages")) + 1
        )
        log.info("Successfully forwarded message from Doover 1.0.")
