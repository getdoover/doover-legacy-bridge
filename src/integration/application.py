import logging
import time

from datetime import datetime, timezone
from typing import Any

from pydoover.cloud.processor import Application, IngestionEndpointEvent

from legacy_bridge_common.utils import parse_file, nested_find_replace, find_element

log = logging.getLogger()


class DooverLegacyBridgeApplication(Application):
    async def setup(self):
        self._record_tag_update = False

    @staticmethod
    def extract_update(key, new_value, element) -> str | None:
        if element.get("showActivity", True) is False:
            return None

        name = element.get("verboseString") or element.get("displayString") or key
        name = name.lower()

        # type should probably always be there?
        match element.get("type"):
            case "uiStateCommand":
                formatted = "set {name} to {new}"
                try:
                    new_value = element["userOptions"][new_value]["displayString"].lower()
                except (KeyError, AttributeError):
                    log.info(f"Error converting state command key to display string: {element}, {new_value}")

            case "uiAction":
                return None
            case "uiTextParam":
                # do we need to str-ify this?
                if len(str(new_value)) > 50 or "\n" in str(new_value):
                    formatted = "changed {name}"
                else:
                    formatted = "set {name} to {new}"
            case "uiDatetimeParam":
                formatted = "changed {name}"
            case _:
                formatted = "set {name} to {new}"

        # .format() is an option but exposes an eval security vulnerability, so just do a find replace...
        return formatted.replace("{name}", name).replace("{new}", new_value)

    async def handle_ui_cmds_update(self, agent_id: int, actor: dict[str, str], diff: dict[str, Any], ui_state):
        updates = [self.extract_update(key, value, find_element(key, ui_state)) for key, value in diff.items()]
        message = " and ".join(n for n in updates if n)
        payload = {
            "timestamp": datetime.now(tz=timezone.utc).timestamp() * 1000,
            "actor": actor,
            "subject": {
                "id": agent_id,
            },
            "message": message,
            "type": "ui_cmds_extract",
        }

        await self.api.publish_message(agent_id, "activity_logs", payload)

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

            if event.payload["agent_key"] != event.payload["author_key"]:
                # ui_cmds, the user has updated something substantial so let's see if we need to add it to activity log
                ui_state = await self.api.get_channel(agent_id, "ui_state")
                actor = {
                    "name": payload["author_name"],
                    "legacy_agent_key": payload["author_key"],
                }
                await self.handle_ui_cmds_update(agent_id, actor, payload, ui_state.aggregate)

        if channel_name == "ui_state":
            try:
                state = payload["state"]
            except KeyError:
                pass
            else:
                nested_find_replace(
                    state,
                    "componentUrl",
                    "https://getdoover.github.io/cameras/HLSLiveView.js",
                    "https://getdoover.github.io/cameras/LiveViewV2.js",
                )
            # payload = nested_find_replace(payload, "componentUrl", "https://getdoover.github.io/cameras/HLSLiveView.js", "https://getdoover.github.io/cameras/LiveViewV2.js")

        if channel_name == "activity_logs":
            try:
                message = payload["activity_log"]["action_string"]
            except KeyError:
                if "actor" not in payload and "action" not in payload:
                    log.info(
                        f"Ignoring malformed activity log from Doover 1.0: {payload}"
                    )
                    return
            else:
                actor = {
                    "name": payload["author_name"],
                    "legacy_agent_key": payload["author_key"],
                }
                try:
                    actor["id"] = org_config[payload["author_key"]]
                except KeyError:
                    # not to worry, we'll just use the name instead. Just provides less options going forward
                    # for 'hover for more info via id' or whatever
                    pass

                payload = {
                    "timestamp": time.time() * 1000,
                    "actor": actor,
                    "subject": {
                        "id": agent_id,
                    },
                    "action": message,
                }

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

        # num_messages = await self.get_tag(f"num_messages_synced_{agent_id}", 0)
        # await self.set_tag(f"num_messages_synced_{agent_id}", num_messages + 1)
        # await self.api.publish_message(
        #     agent_id,
        #     "tag_values",
        #     message={
        #         self.app_key: {
        #             "num_messages_synced": num_messages + 1,
        #             "last_message_dt": datetime.now(timezone.utc).timestamp(),
        #         }
        #     },
        # )
        # await self.set_tag(
        #     "imported_messages", (await self.get_tag("imported_messages", 0)) + 1
        # )
        log.info("Successfully forwarded message from Doover 1.0.")
