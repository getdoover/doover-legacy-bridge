"""Backfill a Doover 1.0 channel's message history onto the matching Doover 2.0 agent.

The legacy bridge only forwards messages as they happen - anything published to
Doover 1.0 before the bridge was installed never makes it across.  This script
reads the message log of a channel (``ui_state`` by default) from a 1.0 agent,
runs each payload through the same transforms the bridge applies on a manual
sync, and re-publishes them to the 2.0 agent with their *original* timestamps so
graphs and history line up.

Messages are written to the 2.0 channel's message log in batches of up to 50 via
``POST /agents/messages``, each item carrying an explicit ``ts``.  Note the 2.0
aggregate is a separate resource - a manual sync (or the next live message) is
what keeps it current, this script only backfills history.

pydoover exposes the same endpoint as ``DataClient.batch_create_messages`` from
the release that follows 1.11.2; this script talks to it directly so it keeps
working against older pinned pydoover versions.

Usage::

    # dry run against one device - fetch, transform and report, write nothing
    python scripts/import_ui_state_history.py --agent 184882486604499210

    # same again, actually writing to 2.0
    python scripts/import_ui_state_history.py --agent 184882486604499210 --commit

    # every device in a 2.0 group
    python scripts/import_ui_state_history.py --group 184857328980363520 --commit

Credentials and ids are resolved from the device itself wherever possible:
  * Doover 2.0 - an access token from a ``~/.doover/config`` profile that has a
    ``BASE_DATA_URL`` set (``--profile``, default ``default``).  Run
    ``doover login`` if it has expired.
  * Doover 1.0 - the api key/url and the 1.0 agent uuid all come from the legacy
    bridge app's own config in the 2.0 ``deployment_config`` aggregate
    (``legacy_api_key`` / ``legacy_api_url`` / ``agent_key``), so we talk to 1.0
    exactly like the bridge does.  Override with ``--legacy-token``,
    ``--legacy-url`` and ``--legacy-agent``.

Re-running is safe: messages already present on the 2.0 channel at the same
second are skipped, so an interrupted import can simply be run again.
"""

import argparse
import json
import logging
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

sys.path.insert(
    0, str(Path(__file__).parents[1] / "packages" / "legacy_bridge_common" / "src")
)

from pydoover.cloud.api import Client  # noqa: E402
from pydoover.utils.snowflake import DOOVER_EPOCH, generate_snowflake_id_at  # noqa: E402

from legacy_bridge_common.utils import (  # noqa: E402
    normalize_reported_desired,
    replace_units_add_requires_confirm,
    replace_widget_urls,
)

log = logging.getLogger("import_ui_state_history")

DEFAULT_APP_KEY = "doover_legacy_bridge_cloud_app"
CONFIG_PATH = Path.home() / ".doover" / "config"
# the data api caps a single page well below this, we just page until it dries up
PAGE_SIZE = 500
# the 1.0 api's `messages` route silently defaults to 5 messages despite the
# pydoover docstring claiming it fetches everything - always ask for a number.
LEGACY_FETCH_ALL = 1_000_000
# `POST /agents/messages` accepts 1-50 items and rejects anything larger
MAX_BATCH = 50


def load_v2_profile(profile: str, check_expiry: bool = True) -> dict[str, str]:
    """Pull a Doover 2.0 profile out of ~/.doover/config.

    pydoover's own ConfigManager chokes on this file once the 2.0 CLI has written
    to it (the 2.0 sections have no USERNAME/PASSWORD lines and profile names are
    duplicated across the 1.0 and 2.0 halves), so parse it leniently here and
    keep only sections that look like 2.0 ones.
    """
    if not CONFIG_PATH.exists():
        raise SystemExit(f"No doover config at {CONFIG_PATH}. Run `doover login`.")

    parts = re.split(r"(?m)^\[profile=(.+?)\]$", CONFIG_PATH.read_text())
    it = iter(parts[1:])
    matches = []
    for name, body in zip(it, it):
        if name != profile:
            continue
        entry = dict(
            line.split("=", 1) for line in body.strip().splitlines() if "=" in line
        )
        if entry.get("BASE_DATA_URL") and entry.get("TOKEN"):
            matches.append(entry)

    if not matches:
        raise SystemExit(
            f"No Doover 2.0 profile named '{profile}' (with a BASE_DATA_URL) in {CONFIG_PATH}."
        )

    # duplicated section names: last one wins, same as the CLI's own read order
    entry = matches[-1]
    expires = entry.get("TOKEN_EXPIRES")
    if check_expiry and expires:
        expires_at = datetime.fromtimestamp(float(expires), timezone.utc)
        if expires_at < datetime.now(timezone.utc):
            raise SystemExit(
                f"Token for profile '{profile}' expired at {expires_at}. Run `doover login`."
            )
    return entry


class DooverV2:
    def __init__(self, profile: str, entry: dict[str, str]):
        self.profile = profile
        self.base_url = entry["BASE_URL"].rstrip("/")
        self.data_base_url = entry["BASE_DATA_URL"].rstrip("/")
        self.session = requests.Session()
        self._set_token(entry["TOKEN"])

    def _set_token(self, token: str):
        self.session.headers["Authorization"] = f"Bearer {token}"

    def refresh_token(self) -> bool:
        """Get a fresh access token for a long-running import.

        Doover 2.0 access tokens last about an hour, which a multi-device import
        can outlive.  Rather than handle (and risk rotating) the refresh token
        ourselves, run a trivial CLI command - the CLI refreshes and rewrites
        ``~/.doover/config`` itself - then pick the new token back up.
        """
        log.info("Access token expired, refreshing via the doover CLI...")
        try:
            subprocess.run(
                ["doover", "--render", "json", "agent", "list"],
                capture_output=True,
                timeout=180,
                check=True,
            )
        except (OSError, subprocess.SubprocessError) as e:
            log.error("Token refresh failed (%s). Run `doover login`.", e)
            return False

        entry = load_v2_profile(self.profile, check_expiry=False)
        self._set_token(entry["TOKEN"])
        return True

    def _request(self, method: str, path: str, *, data_url: bool = True, **kwargs):
        root = self.data_base_url if data_url else self.base_url
        for attempt in (1, 2):
            resp = self.session.request(method, f"{root}{path}", timeout=60, **kwargs)
            if resp.status_code == 401 and attempt == 1 and self.refresh_token():
                continue
            break
        if not resp.ok:
            raise RuntimeError(f"{method} {path} -> {resp.status_code}: {resp.text}")
        return resp.json()

    def get_aggregate(self, agent_id: int, channel_name: str):
        return self._request("GET", f"/agents/{agent_id}/channels/{channel_name}/aggregate")

    def get_agents_in_group(self, group_id: int) -> list[dict]:
        """Every unarchived device agent belonging to a group.

        The agents endpoint has no server-side group filter, so pull the list and
        match on each agent's ``group`` field the way the CLI does.
        """
        data = self._request("GET", "/agents", data_url=False)
        agents = data["agents"] if isinstance(data, dict) else data
        return [
            a
            for a in agents
            if str(a.get("group")) == str(group_id)
            and a.get("type") == "device"
            and not a.get("archived")
        ]

    def iter_message_ids(self, agent_id: int, channel_name: str, after: int):
        """Yield every message id on a channel published after a snowflake."""
        cursor = after
        while True:
            page = self._request(
                "GET",
                f"/agents/{agent_id}/channels/{channel_name}/messages",
                params={"after": cursor, "limit": PAGE_SIZE},
            )
            if not page:
                return
            for message in page:
                yield int(message["id"])
            if len(page) < PAGE_SIZE:
                return
            cursor = int(page[-1]["id"])

    def publish_message(
        self, agent_id: int, channel_name: str, data: dict, timestamp: float
    ):
        return self._request(
            "POST",
            f"/agents/{agent_id}/channels/{channel_name}/messages",
            json={"data": data, "ts": int(timestamp * 1000)},
        )

    def publish_messages(
        self, agent_id: int, channel_name: str, batch: list[tuple[float, dict]]
    ) -> list[str]:
        """Create up to ``MAX_BATCH`` messages in one request.

        Returns the error strings of any items the server rejected - a batch can
        partially succeed and is never rolled back, so only failures come back
        for the caller to retry.
        """
        payload = {
            "items": [
                {
                    "agent_id": str(agent_id),
                    "channel_name": channel_name,
                    "data": data,
                    "ts": int(ts * 1000),
                }
                for ts, data in batch
            ]
        }
        result = self._request("POST", "/agents/messages", json=payload)
        return [
            item.get("error") or "unknown error"
            for item in result["items"]
            if not item.get("success")
        ]


def snowflake_to_timestamp(snowflake: int) -> float:
    """Unix seconds a snowflake id was minted at."""
    return ((snowflake >> 22) + DOOVER_EPOCH) / 1000


class BridgeNotConfigured(Exception):
    """The legacy bridge app isn't installed/configured on a 2.0 agent."""


def read_bridge_config(client: DooverV2, agent_id: int, app_key: str) -> dict:
    """Read the legacy bridge app's config out of the 2.0 deployment_config.

    Holds everything we need to talk to 1.0: ``legacy_api_key``,
    ``legacy_api_url`` and ``agent_key`` (the 1.0 agent uuid).
    """
    data = client.get_aggregate(agent_id, "deployment_config")["data"]
    try:
        return data["applications"][app_key]
    except KeyError:
        installed = ", ".join(data.get("applications", {})) or "none"
        raise BridgeNotConfigured(
            f"no '{app_key}' app config on agent {agent_id} (installed: {installed})"
        )


def transform(channel_name: str, payload: dict) -> dict | None:
    """Apply the same transforms the bridge's manual sync applies.

    Returns ``None`` for payloads that should be skipped.  Positions are
    deliberately *not* assigned - element ordering belongs to the live
    aggregate, not to backfilled history.
    """
    if not isinstance(payload, dict):
        return None

    if "output_type" in payload and "output" in payload:
        # file/attachment message - the history import doesn't handle uploads
        return None

    if channel_name == "ui_state":
        # drops the 1.0 `desired` block (that's ui_cmds' history, not ui_state's)
        normalize_reported_desired(payload)
        if not payload.get("state"):
            # a desired-only shadow diff - ui_cmds history, not ui_state's
            return None
        payload = replace_units_add_requires_confirm(payload)
        if isinstance(payload.get("state"), dict):
            replace_widget_urls(payload["state"])

    elif channel_name == "ui_cmds":
        payload = payload.get("cmds", payload)

    return payload


def parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def import_agent(
    v2: DooverV2,
    agent_id: int,
    label: str,
    args: argparse.Namespace,
) -> dict:
    """Backfill one 2.0 agent's channel from its 1.0 counterpart."""
    stats = {
        "label": label,
        "agent": agent_id,
        "published": 0,
        "existing": 0,
        "unimportable": 0,
        "duplicate_ts": 0,
        "failed": 0,
        "bytes": 0,
        "error": None,
    }

    legacy_token, legacy_url = args.legacy_token, args.legacy_url
    legacy_agent = args.legacy_agent

    if not (legacy_token and legacy_agent):
        try:
            bridge = read_bridge_config(v2, agent_id, args.app_key)
        except BridgeNotConfigured as e:
            stats["error"] = str(e)
            log.warning("%s: skipping - %s", label, e)
            return stats

        legacy_token = legacy_token or bridge.get("legacy_api_key")
        legacy_url = bridge.get("legacy_api_url") or legacy_url
        legacy_agent = legacy_agent or bridge.get("agent_key")

    if not legacy_token or not legacy_agent:
        stats["error"] = "no legacy api key / agent key configured"
        log.warning("%s: skipping - %s", label, stats["error"])
        return stats

    legacy = Client(token=legacy_token, base_url=legacy_url)
    try:
        channel = legacy.get_channel_named(args.channel, legacy_agent)
    except Exception as e:
        stats["error"] = f"legacy lookup failed: {e}"
        log.warning("%s: skipping - %s", label, stats["error"])
        return stats

    if not channel:
        stats["error"] = f"no '{args.channel}' channel on legacy agent {legacy_agent}"
        log.warning("%s: skipping - %s", label, stats["error"])
        return stats

    # 1.0's time-window route 500s on high-volume channels (it falls over well
    # under Pile 5's ~19k ui_state messages), so always pull the message list and
    # apply the window ourselves. Listing is one request and stays fast at that
    # size; only the per-message payload fetches are slow.
    windowed = bool(args.since or args.until)
    # a --limit can only be pushed to the api when we aren't filtering afterwards
    fetch_count = LEGACY_FETCH_ALL if windowed else (args.limit or LEGACY_FETCH_ALL)
    messages = legacy.get_channel_messages(channel.id, num_messages=fetch_count)
    if len(messages) == LEGACY_FETCH_ALL:
        log.warning(
            "%s: hit the %s message fetch ceiling - there may be older history.",
            label,
            LEGACY_FETCH_ALL,
        )

    if windowed:
        since = (args.since or datetime(2015, 1, 1, tzinfo=timezone.utc)).timestamp()
        until = (
            args.until or datetime.now(timezone.utc) + timedelta(days=1)
        ).timestamp()
        messages = [m for m in messages if since <= m._timestamp <= until]

    # the 1.0 api returns newest first; apply --limit to the newest, then go oldest
    # first so the last thing we publish is the most recent message
    if args.limit:
        messages = messages[: args.limit]
    messages = sorted(messages, key=lambda m: m._timestamp)

    if not messages:
        log.info("%s: no messages on 1.0 '%s', nothing to do", label, args.channel)
        return stats

    oldest = datetime.fromtimestamp(messages[0]._timestamp, timezone.utc)
    newest = datetime.fromtimestamp(messages[-1]._timestamp, timezone.utc)
    log.info(
        "%s: %s messages on 1.0 '%s' spanning %s -> %s",
        label,
        len(messages),
        args.channel,
        oldest,
        newest,
    )

    existing: set[int] = set()
    if not args.no_skip_existing:
        after = generate_snowflake_id_at(oldest - timedelta(seconds=1))
        existing = {
            # a message published at the same second is treated as the same message
            int(snowflake_to_timestamp(sid))
            for sid in v2.iter_message_ids(agent_id, args.channel, after)
        }
        log.info("%s: 2.0 already has %s messages in that window", label, len(existing))

    batch: list[tuple[float, dict]] = []
    seen_millis: set[int] = set()

    def flush(force: bool = False):
        """Publish the pending batch once it's full (or at the end)."""
        if not batch or (len(batch) < args.batch_size and not force):
            return

        chunk, batch[:] = batch[:], []
        first = datetime.fromtimestamp(chunk[0][0], timezone.utc)
        last = datetime.fromtimestamp(chunk[-1][0], timezone.utc)

        try:
            errors = v2.publish_messages(agent_id, args.channel, chunk)
        except Exception as e:
            stats["failed"] += len(chunk)
            log.warning("%s batch of %s failed: %s", label, len(chunk), e)
            return

        stats["published"] += len(chunk) - len(errors)
        stats["failed"] += len(errors)
        log.info(
            "%s published %s/%s messages %s -> %s",
            label,
            len(chunk) - len(errors),
            len(chunk),
            first,
            last,
        )
        for error in errors[:5]:
            log.warning("%s batch item rejected: %s", label, error)
        if args.delay:
            time.sleep(args.delay)

    for i, message in enumerate(messages, start=1):
        ts = message._timestamp
        when = datetime.fromtimestamp(ts, timezone.utc)
        prefix = f"{label} [{i}/{len(messages)}]"

        if int(ts) in existing:
            stats["existing"] += 1
            log.debug("%s %s already present, skipping", prefix, when)
            continue

        # 2.0 derives a message's id from its `ts`, so two messages sharing a
        # millisecond collide: the second silently overwrites the first and the
        # api still reports both as created. Drop the duplicate ourselves so the
        # published count reflects what actually lands.
        millis = int(ts * 1000)
        if millis in seen_millis:
            stats["duplicate_ts"] += 1
            log.debug("%s %s duplicate millisecond, skipping", prefix, when)
            continue
        seen_millis.add(millis)

        try:
            payload = message.fetch_payload()
        except Exception as e:
            stats["failed"] += 1
            log.warning("%s %s failed to fetch payload: %s", prefix, when, e)
            continue

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except ValueError:
                pass

        data = transform(args.channel, payload)
        if not data:
            stats["unimportable"] += 1
            log.debug("%s %s payload not importable, skipping", prefix, when)
            continue

        stats["bytes"] += len(json.dumps(data))

        if not args.commit:
            stats["published"] += 1
            log.debug("%s would publish %s", prefix, when)
            continue

        batch.append((ts, data))
        flush()

    flush(force=True)

    if not args.commit:
        log.info("%s: would publish %s messages", label, stats["published"])

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--agent", type=int, help="Doover 2.0 agent id")
    target.add_argument(
        "--group", type=int, help="Doover 2.0 group id - import every device in it"
    )
    parser.add_argument(
        "--legacy-agent",
        help="Doover 1.0 agent uuid (default: the bridge's own `agent_key`). "
        "Only valid with --agent.",
    )
    parser.add_argument("--channel", default="ui_state", help="channel to import")
    parser.add_argument(
        "--profile", default="default", help="~/.doover/config profile for Doover 2.0"
    )
    parser.add_argument("--legacy-token", help="Doover 1.0 api key")
    parser.add_argument("--legacy-url", default="https://my.doover.dev")
    parser.add_argument(
        "--app-key",
        default=DEFAULT_APP_KEY,
        help="bridge app key to read legacy credentials from",
    )
    parser.add_argument("--since", type=parse_dt, help="only import messages after this (ISO)")
    parser.add_argument("--until", type=parse_dt, help="only import messages before this (ISO)")
    parser.add_argument(
        "--limit", type=int, help="import at most this many (most recent) messages per device"
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="publish even if 2.0 already has a message at that timestamp",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=MAX_BATCH,
        help=f"messages per batch request (max {MAX_BATCH})",
    )
    parser.add_argument(
        "--delay", type=float, default=0.05, help="seconds to sleep between batches"
    )
    parser.add_argument(
        "--commit", action="store_true", help="actually write to Doover 2.0"
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.legacy_agent and args.group:
        parser.error("--legacy-agent only makes sense with a single --agent")

    if not 1 <= args.batch_size <= MAX_BATCH:
        parser.error(f"--batch-size must be between 1 and {MAX_BATCH}")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    v2 = DooverV2(args.profile, load_v2_profile(args.profile))

    if args.group:
        agents = v2.get_agents_in_group(args.group)
        if not agents:
            raise SystemExit(f"No unarchived device agents in group {args.group}.")
        targets = [(int(a["id"]), a.get("display_name") or a["name"]) for a in agents]
        # natural sort so "Pile 2" comes before "Pile 10" in the logs
        targets.sort(
            key=lambda t: [
                int(p) if p.isdigit() else p.lower()
                for p in re.split(r"(\d+)", t[1])
            ]
        )
        log.info(
            "Group %s: %s devices - %s",
            args.group,
            len(targets),
            ", ".join(label for _, label in targets),
        )
    else:
        targets = [(args.agent, str(args.agent))]

    results = [import_agent(v2, agent_id, label, args) for agent_id, label in targets]

    verb = "Published" if args.commit else "Would publish"
    log.info("--- summary ---")
    for r in results:
        note = f" ({r['error']})" if r["error"] else ""
        log.info(
            "%-16s %s %s, %s already present, %s unimportable, %s same-ms duplicates, %s failed%s",
            r["label"],
            verb.lower(),
            r["published"],
            r["existing"],
            r["unimportable"],
            r["duplicate_ts"],
            r["failed"],
            note,
        )
    log.info(
        "%s %s messages (%.1f MB) across %s devices, %s already present, "
        "%s same-ms duplicates, %s failed, %s skipped devices",
        verb,
        sum(r["published"] for r in results),
        sum(r["bytes"] for r in results) / 1024 / 1024,
        len(results),
        sum(r["existing"] for r in results),
        sum(r["duplicate_ts"] for r in results),
        sum(r["failed"] for r in results),
        sum(1 for r in results if r["error"]),
    )
    if not args.commit:
        log.info("Dry run - nothing was written. Re-run with --commit to import.")

    return 1 if any(r["failed"] or r["error"] for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
