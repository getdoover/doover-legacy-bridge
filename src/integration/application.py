import base64
import logging
import time

from datetime import datetime, timezone

from pydoover.cloud.processor import Application, IngestionEndpointEvent

log = logging.getLogger()


class DooverLegacyBridgeApplication(Application):
    async def handle_camera_message(
        self, agent_id, channel_name, payload: dict, timestamp: datetime
    ):
        log.info("Handling camera message.")

        data = payload["output"]
        del payload["output"]

        cam_name = payload["camera_name"]
        file_type = payload["output_type"]

        data_bytes = base64.b64decode(data)

        match file_type:
            case "mp4":
                content_type = "video/mp4"
            case "jpg" | "jpeg":
                content_type = "image/jpeg"
            case _:
                # generic binary, this really shouldn't happen
                content_type = "application/octet-stream"

        file = (f"{cam_name}.{file_type}", data_bytes, content_type)

        await self.api.publish_message(
            agent_id,
            channel_name,
            message=payload,
            files=[file],
            timestamp=timestamp,
            record_log=True,
            is_diff=False,
        )

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

        if "camera_name" in payload and payload.get("output_type") in (
            "jpeg",
            "jpg",
            "mp4",
        ):
            await self.handle_camera_message(agent_id, channel_name, payload, ts)
        else:
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
