import os
import asyncio
import json
import logging
import random
import re
import time
import discord
from logging.handlers import RotatingFileHandler
from discord.ext import commands, tasks
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# Use uvloop's event loop when available. It's a drop-in replacement for
# asyncio's default loop with materially lower scheduling latency, which
# matters here specifically because every trivia attempt has to fit inside
# Discord's fixed 15s window -- shaving even a few ms per await adds up to
# more retry attempts fitting in that budget. Purely optional: falls back
# to the standard asyncio loop if uvloop isn't installed (e.g. on Windows,
# where uvloop isn't supported).
try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Setup and Configuration Constants
# ---------------------------------------------------------------------------
load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
TARGET_BOT_ID = int(os.getenv("TARGET_BOT_ID", "0"))
DAILY_ID = int(os.getenv("DAILY_ID", "0"))
ALERT_ID = int(os.getenv("ALERT_ID", "0"))

STATE_FILE = "farm_state.json"
PROFILE_FILE = "farm_profiles.json"
TZ_GMT1 = timezone(timedelta(hours=1))

MESSAGE_CHUNK_LIMIT = 1900

import queue as _queue_module
from logging.handlers import QueueHandler, QueueListener

logger = logging.getLogger("farm_bot")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
_console = logging.StreamHandler()
_console.setFormatter(_fmt)
LOG_FILE_PATH = "farm_bot.log"
_file = RotatingFileHandler(LOG_FILE_PATH, maxBytes=1_000_000, backupCount=3)
_file.setFormatter(_fmt)

# RotatingFileHandler does a *synchronous* disk write on every single
# logger.info() call. The trivia critical path (button click, modal
# listener, submit) logs heavily on purpose for debugging, and each of
# those blocking writes stalls the asyncio event loop for a few ms --
# including the modal/interaction listeners themselves, which need to run
# the instant Discord dispatches the event. Routing both handlers through
# a QueueHandler means logger.info() only ever enqueues (microseconds);
# a background thread (QueueListener) does the actual file/console I/O
# off the event loop entirely.
_log_queue = _queue_module.Queue(-1)
_queue_handler = QueueHandler(_log_queue)
logger.addHandler(_queue_handler)
_queue_listener = QueueListener(_log_queue, _console, _file, respect_handler_level=True)
_queue_listener.start()

MAX_BACKOFF = 60
MAX_RETRIES = 8
MAX_CRASHES_PER_WINDOW = 5
CRASH_WINDOW_SECONDS = 300

MODULAR_SLOT_IDS = tuple(range(1, 16))  # slots 1-15

DEFAULT_CONFIG = {
    "is_paused": False,
    # Emergency anti-spam lock triggered by the target bot's 5/6 Fail Count warning.
    # Stored as an absolute Unix timestamp so the 62-minute pause is exact and survives restarts.
    "fail_count_pause_until": 0.0,
    "fail_count_pause_prev_is_paused": False,
    "loop_interval": 3.0,
    "loop_jitter": 0.5,

    "break_interval": 7200,
    "break_duration": 600,
    "last_break_time": 0.0,
    
    "last_error": None,

    "plant_interval": 960,
    "plant_repeats": 12,
    "plant_material": "attainment_scroll",
    "plant_quantity": 9,
    "plant_enabled": True,
    "last_plant_run": 0.0,
    "plant_step_gap": 2.0,
    "first_plant_done": False,

    "refine_enabled": False,
    "refine_interval": 300,
    "refine_recipe_id": "Guts Gu",
    "last_refine_run": 0.0,
    "refine_max_consecutive_failures": 3,
    "refine_consecutive_failures": 0,

    "fight_horde_enabled": False,
    "fight_horde_interval": 60,
    "last_fight_horde_run": 0.0,
    "stats_fight_horde_count": 0,
    "stats_fight_horde_failures": 0,
    "fight_horde_batch_size": 1,
    "fight_horde_batch_gap": 0.5,
    "fight_horde_max_consecutive_failures": 3,
    "fight_horde_consecutive_failures": 0,

    # NEW: re-clicks Attack whenever "Horde dodged!" shows up in the result,
    # runs a Gu-swap heal routine when player HP drops below the threshold,
    # and deposits to the vault when a fight doesn't drop Primeval Essence.
    "fight_horde_low_hp_threshold": 100,
    "gu1_name": "Blade Appraisal Gu",
    "gu2_name": "Sword Body Restoration Gu",
    "active_gu": "gu1",
    "fight_horde_heal_use_times": 2,
    "fight_horde_heal_step_gap": 1.5,
    "fight_horde_essence_amount": 50,
    "fight_horde_essence_keyword": "Primeval Essence",
    "fight_horde_essence_mode": "Primeval Essence",
    "fight_horde_vault_deposit_amount": 2000,

    "hunt_enabled": False,
    "hunt_interval": 60,
    "last_hunt_run": 0.0,
    "stats_hunt_count": 0,
    "stats_hunt_failures": 0,
    "hunt_batch_size": 1,
    "hunt_batch_gap": 0.5,
    "hunt_max_consecutive_failures": 3,
    "hunt_consecutive_failures": 0,

    "modular_chain_enabled": False,
    "modular_chain_interval": 300,
    "modular_chain_step_gap": 2.0,
    "modular_chain_sequence": "1,2,3",
    "last_modular_chain_run": 0.0,

    "sleep_enabled": False,
    "sleep_start_hour": 23,
    "sleep_duration_hours": 7,

    "hq_gather_enabled": False,
    "hq_gather_message": "hq gather",
    "hq_gather_interval": 60,
    "hq_gather_channel_id": 0,
    "last_hq_gather_time": 0.0,
    "stats_hq_gather_count": 0,

    "crash_times": [],
    "log_summary_interval_loops": 500,

    # ---- Daily summary report (channel comes from DAILY_ID env var) ----
    "daily_report_interval_hours": 24,
    "daily_report_last_sent": 0.0,
    "daily_report_baseline": {},
    "session_paused_seconds": 0.0,
    "pause_started_at": 0.0,
    "session_errors_count": 0,

    # ---- Alert channel (channel comes from ALERT_ID env var) ----
    "alert_on_all_errors": False,           # False = only severe events (crashes, auto-disables)
    "last_alert_sent_at": 0.0,

    "stats_start_time": 0.0,
    "stats_plant_count": 0,
    "stats_harvest_count": 0,
    "stats_refine_count": 0,
    "stats_refine_failures": 0,
    "stats_loops_completed": 0,
    "stats_total_break_time": 0,
    "command_refresh_interval": 21600,

    "voice_channel_id": 0,
    "voice_enabled": False,

    # ---- per-feature "!" prefix support for built-in commands ----
    "cmd_prefix_plant": "/",
    "cmd_prefix_harvest_plots": "/",
    "cmd_prefix_refine": "/",
    "cmd_prefix_fight_horde": "/",
    "cmd_prefix_hunt": "/",
}

for _slot in MODULAR_SLOT_IDS:
    DEFAULT_CONFIG[f"slot{_slot}_enabled"] = False
    DEFAULT_CONFIG[f"slot{_slot}_command"] = ""
    DEFAULT_CONFIG[f"slot{_slot}_param1_name"] = ""
    DEFAULT_CONFIG[f"slot{_slot}_param1_value"] = ""
    DEFAULT_CONFIG[f"slot{_slot}_param2_name"] = ""
    DEFAULT_CONFIG[f"slot{_slot}_param2_value"] = ""
    DEFAULT_CONFIG[f"slot{_slot}_param3_name"] = ""
    DEFAULT_CONFIG[f"slot{_slot}_param3_value"] = ""
    DEFAULT_CONFIG[f"slot{_slot}_interval"] = 300
    DEFAULT_CONFIG[f"last_slot{_slot}_run"] = 0.0
    DEFAULT_CONFIG[f"stats_slot{_slot}_count"] = 0
    DEFAULT_CONFIG[f"stats_slot{_slot}_failures"] = 0
    # Per-slot dispatch mode: "/" = slash-command interaction (default), "!" = plain text "!command ..." message
    DEFAULT_CONFIG[f"cmd_prefix_slot{_slot}"] = "/"
    # Auto-pause this slot after N consecutive failures (resets on any success)
    DEFAULT_CONFIG[f"slot{_slot}_max_consecutive_failures"] = 5
    DEFAULT_CONFIG[f"slot{_slot}_consecutive_failures"] = 0

SETTABLE = {
    "loop_interval": float,
    "loop_jitter": float,
    "break_interval": int,
    "break_duration": int,
    "plant_interval": int,
    "plant_repeats": int,
    "plant_material": str,
    "plant_quantity": int,
    "plant_step_gap": float,
    "refine_interval": int,
    "refine_recipe_id": str,
    "refine_max_consecutive_failures": int,
    "fight_horde_interval": int,
    "fight_horde_batch_size": int,
    "fight_horde_batch_gap": float,
    "fight_horde_max_consecutive_failures": int,
    "fight_horde_low_hp_threshold": int,
    "gu1_name": str,
    "gu2_name": str,
    "fight_horde_heal_use_times": int,
    "fight_horde_heal_step_gap": float,
    "fight_horde_essence_amount": int,
    "fight_horde_essence_keyword": str,
    "fight_horde_essence_mode": str,
    "fight_horde_vault_deposit_amount": int,
    "hunt_interval": int,
    "hunt_batch_size": int,
    "hunt_batch_gap": float,
    "hunt_max_consecutive_failures": int,
    "modular_chain_interval": int,
    "modular_chain_step_gap": float,
    "modular_chain_sequence": str,
    "log_summary_interval_loops": int,
    "hq_gather_message": str,
    "hq_gather_interval": int,
    "hq_gather_channel_id": int,
    "sleep_enabled": bool,
    "sleep_start_hour": int,
    "sleep_duration_hours": int,
    "command_refresh_interval": int,

    "daily_report_interval_hours": float,
    "alert_on_all_errors": bool,

    "cmd_prefix_plant": str,
    "cmd_prefix_harvest_plots": str,
    "cmd_prefix_refine": str,
    "cmd_prefix_fight_horde": str,
    "cmd_prefix_hunt": str,
}

for _slot in MODULAR_SLOT_IDS:
    SETTABLE[f"slot{_slot}_command"] = str
    SETTABLE[f"cmd_prefix_slot{_slot}"] = str
    SETTABLE[f"slot{_slot}_param1_name"] = str
    SETTABLE[f"slot{_slot}_param1_value"] = str
    SETTABLE[f"slot{_slot}_param2_name"] = str
    SETTABLE[f"slot{_slot}_param2_value"] = str
    SETTABLE[f"slot{_slot}_param3_name"] = str
    SETTABLE[f"slot{_slot}_param3_value"] = str
    SETTABLE[f"slot{_slot}_interval"] = int
    SETTABLE[f"slot{_slot}_max_consecutive_failures"] = int

# All intervals updated to permit 1s minimum per user request
MIN_VALUES = {
    "fight_horde_essence_amount": 1,
    "loop_interval": 1.0,
    "loop_jitter": 0.0,
    "break_interval": 1,
    "break_duration": 1,
    "plant_interval": 1,
    "plant_repeats": 1,
    "plant_quantity": 1,
    "plant_step_gap": 0.0,
    "refine_interval": 1,
    "refine_max_consecutive_failures": 1,
    "fight_horde_interval": 1,
    "fight_horde_batch_size": 1,
    "fight_horde_batch_gap": 0.0,
    "fight_horde_low_hp_threshold": 0,
    "fight_horde_heal_use_times": 1,
    "fight_horde_heal_step_gap": 0.0,
    "fight_horde_vault_deposit_amount": 0,
    "hunt_interval": 1,
    "hunt_batch_size": 1,
    "hunt_batch_gap": 0.0,
    "modular_chain_interval": 1,
    "modular_chain_step_gap": 0.0,
    "log_summary_interval_loops": 0,
    "hq_gather_interval": 1,
    "sleep_start_hour": 0,
    "sleep_duration_hours": 1,
    "command_refresh_interval": 1,
}

for _slot in MODULAR_SLOT_IDS:
    MIN_VALUES[f"slot{_slot}_interval"] = 1

TOGGLE_TARGETS = {
    "plant": ("plant_enabled", None, None, "🌱", "Planting & harvesting sequence"),
    "refine": ("refine_enabled", "last_refine_run", "refine_interval", "🧬", "Refine sequence"),
    "fight_horde": ("fight_horde_enabled", "last_fight_horde_run", "fight_horde_interval", "⚔️", "Fight Horde Sequence"),
    "hunt": ("hunt_enabled", "last_hunt_run", "hunt_interval", "🏹", "Hunt Sequence"),
    "modular_chain": ("modular_chain_enabled", "last_modular_chain_run", "modular_chain_interval", "⛓️", "Modular Chain Sequence"),
}
for _slot in MODULAR_SLOT_IDS:
    TOGGLE_TARGETS[f"slot{_slot}"] = (
        f"slot{_slot}_enabled", f"last_slot{_slot}_run", f"slot{_slot}_interval",
        "🧩", f"Modular Slot {_slot}",
    )

PREVIEWABLE_STEPS = ["plant", "harvest_plots", "refine", "fight_horde", "hunt"]
MODULAR_SLOT_NAMES = tuple(f"slot{_slot}" for _slot in MODULAR_SLOT_IDS)

PREFIX_TARGETS = {
    "plant": "cmd_prefix_plant",
    "harvest_plots": "cmd_prefix_harvest_plots",
    "refine": "cmd_prefix_refine",
    "fight_horde": "cmd_prefix_fight_horde",
    "hunt": "cmd_prefix_hunt",
}
for _slot in MODULAR_SLOT_IDS:
    PREFIX_TARGETS[f"slot{_slot}"] = f"cmd_prefix_slot{_slot}"

DESCRIPTIONS = {
    "loop_interval": "Base delay between automation ticks (seconds).",
    "loop_jitter": "Random +/- spread applied to delays (seconds).",
    "break_interval": "How long between simulated human breaks (seconds).",
    "break_duration": "How long each simulated break lasts (seconds).",
    "plant_interval": "Cooldown between planting cycles (seconds).",
    "plant_step_gap": "Delay between individual plot actions.",
    "plant_repeats": "Number of plots planted per cycle.",
    "plant_material": "Material name passed to the `plant` action.",
    "plant_quantity": "Quantity planted per plot.",
    "refine_interval": "Fixed cooldown between `refine` runs (seconds). No jitter.",
    "refine_recipe_id": "recipe_id value passed to the `refine` action.",
    "refine_max_consecutive_failures": "Auto-disable refine after this many failures in a row.",
    "fight_horde_interval": "Cooldown between /fight_horde runs (seconds).",
    "fight_horde_batch_size": "How many times /fight_horde is fired back-to-back before clicking each response's button. 1 = old single-shot behavior.",
    "fight_horde_batch_gap": "Delay between each fire-off and each click within a fight_horde batch (seconds).",
    "hunt_interval": "Cooldown between /hunt runs (seconds).",
    "hunt_batch_size": "How many times /hunt is fired back-to-back before clicking each response's button. 1 = old single-shot behavior.",
    "hunt_batch_gap": "Delay between each fire-off and each click within a hunt batch (seconds).",
    "modular_chain_interval": "Cooldown for the linked modular loop chain (seconds).",
    "modular_chain_step_gap": "Delay between individual commands in the modular chain.",
    "modular_chain_sequence": "Comma-separated list of slot IDs to run in the chain (e.g., '1,2' or '1,3,4'). Order matters.",
    "log_summary_interval_loops": "Write a one-line progress summary to the log every N loops (0 = off).",
    "hq_gather_message": "Plain text message sent repeatedly by ~~hq-gather.",
    "hq_gather_interval": "Delay between repeated ~~hq-gather sends (seconds).",
    "hq_gather_channel_id": "Channel ID to send hq-gather messages to. 0 = use the main farm channel.",
    "sleep_enabled": "Whether nightly sleep mode is active.",
    "sleep_start_hour": "Hour (0-23, GMT+1) sleep mode begins.",
    "sleep_duration_hours": "How many hours sleep mode lasts.",
    "command_refresh_interval": "How often the target bot's slash commands are re-scanned (seconds).",
    "fight_horde_max_consecutive_failures": "Auto-disable fight_horde after this many failures in a row.",
    "fight_horde_low_hp_threshold": "If the player's HP parsed from a fight_horde message drops below this, run the heal Gu-swap sequence.",
    "gu1_name": "Name of Gu slot 1 (default: Blade Appraisal Gu). This is the 'normal'/outside-combat Gu; also used as the heal sequence's unequip/re-equip target.",
    "gu2_name": "Name of Gu slot 2 (default: Sword Body Restoration Gu). This is the healing Gu equipped and used while HP is low.",
    "fight_horde_heal_use_times": "How many times /use_gu is called on the healing Gu per low-HP trigger. Default: 2.",
    "fight_horde_heal_step_gap": "Delay between each step of the equip/use_gu/unequip heal sequence (seconds).",
    "fight_horde_essence_amount": "Expected amount shown in the fight_horde loot line (e.g. +50 Primeval Essence). Change with `~~essence <amount>`.",
    "fight_horde_essence_keyword": "Current essence name searched in fight_horde loot (Primeval Essence or Immortal Essence).",
    "fight_horde_essence_mode": "Current essence detector mode. `~~toggle_essence` switches between Primeval Essence and Immortal Essence.",
    "fight_horde_vault_deposit_amount": "Amount sent via '!vault deposit <amount>' when a fight_horde result doesn't contain the essence keyword.",
    "hunt_max_consecutive_failures": "Auto-disable hunt after this many failures in a row.",
    "daily_report_interval_hours": "How often the automatic daily report fires (hours; channel set via DAILY_ID env var). It's also always sent on `~~stop`.",
    "alert_on_all_errors": "If true, every logged error is also forwarded to the alert channel (ALERT_ID env var, rate-limited), not just severe ones.",
    "cmd_prefix_plant": "Dispatch mode for /plant + /harvest_plots-triggering plant step: '/' = slash command (default), '!' = plain text '!plant ...' message.",
    "cmd_prefix_harvest_plots": "Dispatch mode for the harvest_plots step: '/' = slash command (default), '!' = plain text '!harvest_plots' message.",
    "cmd_prefix_refine": "Dispatch mode for /refine: '/' = slash command (default), '!' = plain text '!refine ...' message.",
    "cmd_prefix_fight_horde": "Dispatch mode for /fight_horde: '/' = slash command (default), '!' = plain text '!fight_horde' message.",
    "cmd_prefix_hunt": "Dispatch mode for /hunt: '/' = slash command (default), '!' = plain text '!hunt' message.",
}
for _slot in MODULAR_SLOT_IDS:
    DESCRIPTIONS[f"slot{_slot}_command"] = f"Slash command name to call for modular slot {_slot} (e.g. 'sect essence_deposit')."
    DESCRIPTIONS[f"slot{_slot}_param1_name"] = f"First parameter name for slot {_slot}. Leave blank to omit."
    DESCRIPTIONS[f"slot{_slot}_param1_value"] = f"Value for slot {_slot}'s first parameter."
    DESCRIPTIONS[f"slot{_slot}_param2_name"] = f"Second parameter name for slot {_slot}. Leave blank to omit."
    DESCRIPTIONS[f"slot{_slot}_param2_value"] = f"Value for slot {_slot}'s second parameter."
    DESCRIPTIONS[f"slot{_slot}_param3_name"] = f"Third parameter name for slot {_slot}. Leave blank to omit."
    DESCRIPTIONS[f"slot{_slot}_param3_value"] = f"Value for slot {_slot}'s third parameter."
    DESCRIPTIONS[f"slot{_slot}_interval"] = f"Cooldown between slot {_slot} runs (seconds). Overridden if grouped inside modular_chain."
    DESCRIPTIONS[f"cmd_prefix_slot{_slot}"] = f"Dispatch mode for slot {_slot}: '/' = slash-command interaction (default), '!' = plain text '!command ...' message. Set with `~~set cmd_prefix_slot{_slot} !` or `~~set cmd_prefix_slot{_slot} /`."
    DESCRIPTIONS[f"slot{_slot}_max_consecutive_failures"] = f"Auto-disable slot {_slot} after this many failures in a row."


# ---------------------------------------------------------------------------
# Independent Helpers
# ---------------------------------------------------------------------------
def calculate_bell_curve_delay(base_target, maximum_jitter):
    if maximum_jitter <= 0:
        return max(0.1, base_target)
    generated = random.gauss(base_target, maximum_jitter / 2.0)
    return max(base_target - maximum_jitter, min(base_target + maximum_jitter, generated))

def format_countdown(target_ts, now_ts):
    if not target_ts or target_ts <= 0:
        return "DUE / RUNNING"
    remaining = target_ts - now_ts
    if remaining <= 0:
        return "DUE / RUNNING"
    minutes, seconds = divmod(int(remaining), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s remaining"
    return f"{minutes}m {seconds}s remaining"

def is_valid_settable(key, val):
    if key not in SETTABLE:
        return False
    expected_type = SETTABLE[key]
    if expected_type == bool and not isinstance(val, bool):
        return False
    if expected_type in (int, float) and (isinstance(val, bool) or not isinstance(val, (int, float))):
        return False
    if expected_type == str and not isinstance(val, str):
        return False
    if key in MIN_VALUES and val < MIN_VALUES[key]:
        return False
    if key == "sleep_start_hour" and not (0 <= val <= 23):
        return False
    if key.startswith("cmd_prefix_") and val not in ("/", "!"):
        return False
    return True

def chunk_blocks(blocks, limit=MESSAGE_CHUNK_LIMIT):
    chunks = []
    current = ""
    for block in blocks:
        if len(block) > limit:
            if current:
                chunks.append(current)
                current = ""
            for i in range(0, len(block), limit):
                chunks.append(block[i:i + limit])
            continue
        if current and len(current) + len(block) + 1 > limit:
            chunks.append(current)
            current = block
        else:
            current = f"{current}\n{block}" if current else block
    if current:
        chunks.append(current)
    return chunks

def coerce_slot_value(raw):
    if raw == "":
        return None
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        return raw[1:-1]
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


# ---------------------------------------------------------------------------
# Fight Horde helpers (dodge re-click / HP heal / loot check)
# ---------------------------------------------------------------------------
def _gather_message_text(msg):
    """Collects every bit of visible text off a message: raw content plus
    every embed's title/description/footer/author/field name+value. This is
    what "Horde dodged!", the player HP, and the Primeval Essence loot line
    all get searched in, since any of them could land in content or in an
    embed depending on how the target bot formats its response."""
    parts = [getattr(msg, "content", "") or ""]
    for embed in getattr(msg, "embeds", None) or []:
        if getattr(embed, "title", None):
            parts.append(str(embed.title))
        if getattr(embed, "description", None):
            parts.append(str(embed.description))
        footer = getattr(embed, "footer", None)
        if footer and getattr(footer, "text", None):
            parts.append(str(footer.text))
        author = getattr(embed, "author", None)
        if author and getattr(author, "name", None):
            parts.append(str(author.name))
        for field in getattr(embed, "fields", None) or []:
            if getattr(field, "name", None):
                parts.append(str(field.name))
            if getattr(field, "value", None):
                parts.append(str(field.value))
    return "\n".join(p for p in parts if p)


# Best-effort patterns for a "current/max" HP display. Covers the common
# shapes bots use: "HP: 46/300", "❤️ 46/300", "Health 46 / 300", etc.
# NOTE: this is a guess since the exact fight_horde HP format wasn't given —
# share a real example message and this can be tightened to match exactly.
_HP_PATTERNS = [
    # Exact player HP display used by fight_horde, e.g. ``95/200 HP``.
    re.compile(r"(\d+)\s*/\s*(\d+)\s*HP\b", re.IGNORECASE),
    # Fallbacks for common formats such as ``HP: 95/200`` or ``❤️ 95/200``.
    re.compile(r"(?:❤️|💗|💚|💙|🩸|hp|health)\W{0,20}(\d+)\s*/\s*(\d+)", re.IGNORECASE),
]


def _extract_player_hp(text):
    """Return ``(current_hp, max_hp)`` from a fight_horde status line.

    The fight message can render HP as ``95/200 HP`` after a custom heart
    emoji/progress bar, so matching the fraction itself is more reliable than
    requiring a Unicode heart immediately before the numbers.
    """
    for pattern in _HP_PATTERNS:
        m = pattern.search(text)
        if m:
            try:
                return int(m.group(1)), int(m.group(2))
            except ValueError:
                continue
    return None


def _select_attack_button(msg):
    """Same robust Attack-button scoring fight_horde already used for its
    first click, factored out so the dodge-reclick / post-trivia-continuation
    loop can reuse it against later messages too."""
    buttons = []
    for row in getattr(msg, "components", None) or []:
        children = getattr(row, "children", None)
        candidates = children if children is not None else [row]
        for child in candidates:
            if hasattr(child, "click") and not getattr(child, "disabled", False):
                buttons.append(child)
    if not buttons:
        return None

    def _norm(value):
        return re.sub(r"\s+", " ", str(value or "").strip().lower())

    def _component_text(component):
        parts = [
            _norm(getattr(component, "label", None)),
            _norm(getattr(component, "custom_id", None)),
            _norm(getattr(component, "emoji", None)),
        ]
        emoji = getattr(component, "emoji", None)
        if emoji is not None:
            parts.extend([
                _norm(getattr(emoji, "name", None)),
                _norm(getattr(emoji, "id", None)),
                _norm(getattr(emoji, "unicode_emoji", None)),
            ])
        try:
            raw = component.to_dict()
            if isinstance(raw, dict):
                for key in ("label", "custom_id", "emoji"):
                    value = raw.get(key)
                    if isinstance(value, dict):
                        parts.extend(_norm(value.get(k)) for k in ("name", "id", "unicode_emoji"))
                    else:
                        parts.append(_norm(value))
        except Exception:
            pass
        return " ".join(p for p in parts if p)

    strong_tokens = ("attack", "atacar", "ataque", "fight", "horde_attack", "attack_button", "btn_attack", "action_attack")
    attack_emojis = ("⚔", "⚔️", "🗡", "🗡️", "🔪", "🥊", "👊", "crossed_swords", "crossed swords")
    negative_tokens = ("flee", "escape", "run", "back", "cancel", "leave", "defend", "defense", "defence", "heal", "inventory")

    scored = []
    for candidate in buttons:
        haystack = _component_text(candidate)
        label = _norm(getattr(candidate, "label", None))
        custom_id = _norm(getattr(candidate, "custom_id", None))
        score = 0
        if "attack" in label:
            score += 150
        if label in ("atacar", "ataque"):
            score += 100
        if custom_id in ("attack", "atacar", "ataque", "fight"):
            score += 90
        for token in strong_tokens:
            if token in haystack:
                score += 40
        for emoji in attack_emojis:
            if emoji in haystack:
                score += 35
        for token in negative_tokens:
            if token in haystack:
                score -= 100
        if score > 0:
            scored.append((score, candidate))

    if scored:
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1]
    if len(buttons) > 1:
        # Multiple active buttons and none of them scored as a recognizable
        # Attack button -- make the ambiguity explicit in logs instead of
        # silently guessing, mirroring the same case in
        # _execute_and_click_first_button's inline selection logic.
        logger.warning(
            "[FIGHT_HORDE] _select_attack_button: ambiguous buttons, none "
            "scored as Attack; falling back to first active button. buttons=%s",
            [
                {
                    "label": getattr(b, "label", None),
                    "custom_id": getattr(b, "custom_id", None),
                    "emoji": str(getattr(b, "emoji", None)),
                }
                for b in buttons
            ],
        )
    return buttons[0]


# ---------------------------------------------------------------------------
# Core Farming Automation Cog
# ---------------------------------------------------------------------------
class FarmingEngine(commands.Cog):
    STAT_KEYS = {
        "plant": "stats_plant_count",
        "harvest_plots": "stats_harvest_count",
        "refine": "stats_refine_count",
        "fight_horde": "stats_fight_horde_count",
        "hunt": "stats_hunt_count",
    }

    def __init__(obj, bot):
        obj.bot = bot
        obj.cached_commands = {}
        obj.cached_app_command_list = []
        obj.active_sequence = "idle"
        # Tracks whether ~~stop was called deliberately, so the watchdog
        # below can tell "user wants it off" apart from "it silently died".
        obj._user_stopped = False
        # Tracks whether the watchdog has already alerted about a closed
        # gateway connection, so it sends one alert per outage instead of
        # spamming the alert channel every 60s while it stays closed.
        obj._gateway_closed_alerted = False

        obj.config = dict(DEFAULT_CONFIG)
        obj.config["crash_times"] = list(DEFAULT_CONFIG["crash_times"])
        obj.load_state_sync()

        obj.STEP_SPECS = {
            "plant": lambda: {"material": obj.config["plant_material"], "quantity": obj.config["plant_quantity"]},
            "harvest_plots": lambda: {},
            "refine": lambda: {"recipe_id": obj.config["refine_recipe_id"]},
            "fight_horde": lambda: {},
            "hunt": lambda: {},
        }

    async def cog_check(obj, ctx):
        return ctx.author.id == obj.bot.user.id

    async def cog_unload(obj):
        logger.info("Emergency save protocol triggered via structural shutdown event.")
        await obj.save_state()

    # -----------------------------------------------------------------------
    # Persistence
    # -----------------------------------------------------------------------
    def load_state_sync(obj):
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)

            for key, expected_type in SETTABLE.items():
                if key not in data:
                    continue
                if not is_valid_settable(key, data[key]):
                    continue
                obj.config[key] = data[key]

            meta_keys = [k for k in DEFAULT_CONFIG if k not in SETTABLE and k != "crash_times"]
            for key in meta_keys:
                if key in data:
                    obj.config[key] = data[key]

            if "crash_times" in data and isinstance(data["crash_times"], list):
                now = time.time()
                obj.config["crash_times"] = [t for t in data["crash_times"] if now - t <= CRASH_WINDOW_SECONDS]
        except Exception as e:
            logger.error(f"Configuration engine encountered unreadable data structures: {e}")

    def _sync_write(obj, payload):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.error(f"Threaded storage commit failure: {e}")

    async def save_state(obj):
        await asyncio.to_thread(obj._sync_write, dict(obj.config))

    def _load_profiles_sync(obj):
        if not os.path.exists(PROFILE_FILE):
            return {}
        try:
            with open(PROFILE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Profile store unreadable: {e}")
            return {}

    def _save_profiles_sync(obj, profiles):
        try:
            with open(PROFILE_FILE, "w") as f:
                json.dump(profiles, f, indent=2)
        except Exception as e:
            logger.error(f"Profile store write failure: {e}")

    async def reply(obj, ctx, title, description=None):
        content = f"**{title}**\n"
        if description:
            content += f"{description}\n"
        await ctx.send(content.strip())

    async def resolve_channel(obj, channel_id):
        """
        Resolves a channel by ID, falling back to an API fetch when it's not
        in the client's cache yet (get_channel only checks the cache, which
        can miss the channel right after startup, after a permission change,
        or simply if CHANNEL_ID is misconfigured). Logs and records the
        specific reason on failure instead of failing silently.
        """
        if not channel_id:
            msg = "CHANNEL_ID is not set (0) — check your environment variable / .env file."
            logger.error(msg)
            obj.config["last_error"] = msg
            if channel_id == CHANNEL_ID:
                await obj.send_alert("🛑 Main Channel Not Set", msg, severe=True)
            return None

        channel = obj.bot.get_channel(channel_id)
        if channel:
            return channel

        try:
            channel = await obj.bot.fetch_channel(channel_id)
            return channel
        except discord.NotFound:
            msg = f"Channel `{channel_id}` not found — the ID is wrong or the channel was deleted."
        except discord.Forbidden:
            msg = f"Channel `{channel_id}` found but access is forbidden — check that this account can see it."
        except Exception as e:
            msg = f"Channel `{channel_id}` could not be resolved: {e}"

        logger.error(msg)
        obj.config["last_error"] = msg
        # Only alert for the main farm channel failing — avoids a pointless
        # recursive attempt to alert-about-the-alert-channel if it's the one
        # that failed to resolve.
        if channel_id == CHANNEL_ID:
            await obj.send_alert("🛑 Main Channel Unresolved", msg, severe=True)
        return None

    async def notify_channel(obj, title, description=None):
        channel = await obj.resolve_channel(CHANNEL_ID)
        if channel:
            try:
                await channel.send(f"**{title}**\n{description if description else ''}".strip())
            except Exception:
                pass

    async def notify_alert_channel(obj, title, description=None):
        """Sends to ALERT_ID specifically. Silently does nothing if that env
        var isn't set (0) — by design, per user request: alerting is opt-in
        and its absence is never treated as an error."""
        if not ALERT_ID:
            return
        channel = await obj.resolve_channel(ALERT_ID)
        if channel:
            try:
                await channel.send(f"**{title}**\n{description if description else ''}".strip())
            except Exception:
                pass

    ALERT_MIN_GAP_SECONDS = 15  # cooldown between non-severe (alert_on_all_errors) alerts, to avoid flooding

    async def send_alert(obj, title, description=None, severe=True):
        """
        Central entry point for the alert channel.
        - severe=True: always sent (crashes, watchdog restarts, a sequence
          getting auto-disabled from repeated failures). These are rare and
          important enough that no rate limiting is applied.
        - severe=False: only sent when the user has opted into
          `alert_on_all_errors`, and rate-limited so a burst of transient
          errors doesn't flood the alert channel.
        Also bumps session_errors_count, which feeds the daily report.
        """
        obj.config["session_errors_count"] = obj.config.get("session_errors_count", 0) + 1

        if not severe:
            if not obj.config.get("alert_on_all_errors", False):
                return
            last_sent = obj.config.get("last_alert_sent_at", 0.0)
            if time.time() - last_sent < obj.ALERT_MIN_GAP_SECONDS:
                return
            obj.config["last_alert_sent_at"] = time.time()

        await obj.notify_alert_channel(title, description)

    DAILY_REPORT_STAT_KEYS = {
        "Plant": "stats_plant_count",
        "Harvest": "stats_harvest_count",
        "Refine": "stats_refine_count",
        "Fight Horde": "stats_fight_horde_count",
        "Hunt": "stats_hunt_count",
    }

    async def _build_daily_report_text(obj):
        cfg = obj.config
        baseline = cfg.get("daily_report_baseline", {}) or {}

        action_lines = []
        for label, key in obj.DAILY_REPORT_STAT_KEYS.items():
            delta = cfg.get(key, 0) - baseline.get(key, 0)
            if delta:
                action_lines.append(f"• {label}: +{delta:,}")

        slot_delta_total = sum(
            cfg.get(f"stats_slot{s}_count", 0) - baseline.get(f"stats_slot{s}_count", 0)
            for s in MODULAR_SLOT_IDS
        )
        if slot_delta_total:
            action_lines.append(f"• Modular Slots (combined): +{slot_delta_total:,}")

        if not action_lines:
            action_lines.append("• No actions completed this period.")

        paused_seconds = cfg.get("session_paused_seconds", 0.0)
        if cfg.get("is_paused") and cfg.get("pause_started_at"):
            paused_seconds += time.time() - cfg["pause_started_at"]

        period_start = cfg.get("daily_report_last_sent") or cfg.get("stats_start_time") or time.time()
        period_str = str(timedelta(seconds=int(max(0, time.time() - period_start))))
        paused_str = str(timedelta(seconds=int(paused_seconds)))

        return (
            f"**Period:** {period_str}\n\n"
            f"**Actions completed:**\n" + "\n".join(action_lines) + "\n\n"
            f"**Paused time:** {paused_str}\n"
            f"**Errors logged:** {cfg.get('session_errors_count', 0):,}\n"
            f"**Last error:** {cfg.get('last_error') or 'none'}"
        )

    async def _send_daily_report(obj, reason="scheduled"):
        """
        Builds and sends the session summary to DAILY_ID.
        If that env var isn't set (0), this is a deliberate no-op —
        per user request, an unset report channel should never be treated
        as an error, it just means the feature is off.
        """
        if DAILY_ID:
            text = await obj._build_daily_report_text()
            channel = await obj.resolve_channel(DAILY_ID)
            if channel:
                try:
                    await channel.send(f"**📅 Daily Summary ({reason})**\n{text}")
                except Exception as e:
                    logger.error(f"Failed sending daily report: {e}")

        # Reset the session baseline/trackers regardless of whether a channel
        # was configured, so the numbers always reflect "since the last
        # reset point" rather than growing forever.
        obj.config["daily_report_baseline"] = {
            key: obj.config.get(key, 0) for key in obj.DAILY_REPORT_STAT_KEYS.values()
        }
        for s in MODULAR_SLOT_IDS:
            obj.config["daily_report_baseline"][f"stats_slot{s}_count"] = obj.config.get(f"stats_slot{s}_count", 0)
        obj.config["session_paused_seconds"] = 0.0
        obj.config["session_errors_count"] = 0
        obj.config["daily_report_last_sent"] = time.time()
        await obj.save_state()

    async def get_commands(obj, channel, force_refresh=False):
        if force_refresh or not obj.cached_commands:
            try:
                app_commands = await channel.application_commands()
                app_commands = [c for c in app_commands if c.application_id == TARGET_BOT_ID]
                obj.cached_app_command_list = app_commands
                obj.cached_commands = {
                    "plant": discord.utils.get(app_commands, name="plant"),
                    "harvest_plots": discord.utils.get(app_commands, name="harvest_plots"),
                    "refine": discord.utils.get(app_commands, name="refine"),
                    "fight_horde": discord.utils.get(app_commands, name="fight_horde"),
                    "hunt": discord.utils.get(app_commands, name="hunt"),
                }
            except Exception as e:
                logger.error(f"Failed command cache matrix extraction: {e}")
        return obj.cached_commands

    def resolve_dynamic_command(obj, name):
        """Resolves slash commands, smoothly traversing `.children` for subcommands like 'sect essence_deposit'."""
        if not name:
            return None
        parts = name.strip().split()
        cmd = discord.utils.get(obj.cached_app_command_list, name=parts[0])
        if not cmd:
            return None
        
        # Traverse any children for subcommands (e.g. ['sect', 'essence_deposit'])
        for part in parts[1:]:
            if not hasattr(cmd, 'children'):
                return None
            child = discord.utils.get(cmd.children, name=part)
            if not child:
                return None
            cmd = child
        return cmd

    def build_slot_kwargs(obj, slot_id):
        kwargs = {}
        for p in (1, 2, 3):
            pname = obj.config.get(f"slot{slot_id}_param{p}_name", "").strip()
            if not pname:
                continue
            pvalue_raw = obj.config.get(f"slot{slot_id}_param{p}_value", "")
            coerced = coerce_slot_value(pvalue_raw)
            if coerced is None:
                continue
            kwargs[pname] = coerced
        return kwargs

    # -----------------------------------------------------------------------
    # NEW: "!" prefix command dispatch (per-slot)
    # -----------------------------------------------------------------------
    async def dispatch_modular_command(obj, channel, slot_id, command_name, kwargs):
        """
        Dispatches a modular slot/chain command using whichever style is
        configured for THAT SPECIFIC SLOT via `cmd_prefix_slot{slot_id}`:
          - "/" (default, unchanged behavior): resolves and invokes the
            target bot's slash command via resolve_dynamic_command().
          - "!": sends a plain text message like "!command value1 value2"
            instead of a slash-command interaction.
        Each slot has its own independent setting, e.g.:
          ~~set cmd_prefix_slot4 !      -> only slot 4 uses "!" dispatch
          ~~set cmd_prefix_slotall !    -> sets every slot's prefix at once
        Returns the result on success, or None on failure/not-found — same
        contract the old inline logic used, so callers don't need to change.
        """
        prefix = obj.config.get(f"cmd_prefix_slot{slot_id}", "/")

        if prefix == "!":
            parts = [f"!{command_name}"] + [str(v) for v in kwargs.values()]
            text = " ".join(parts)
            return await obj.safe_execute(channel.send, text)

        cmd = obj.resolve_dynamic_command(command_name)
        if not cmd:
            return None
        return await obj.safe_execute(cmd, channel, **kwargs)

    async def safe_execute(obj, func, *args, **kwargs):
        delay = 1
        func_name = getattr(func, "__name__", None) or getattr(func, "name", None) or repr(func)
        for attempt in range(MAX_RETRIES):
            try:
                # Trivia has priority over farming. Any new farming action waits
                # until all currently queued/active trivias are finished.
                await _TRIVIA_IDLE_EVENT.wait()
                await asyncio.sleep(random.uniform(0.1, 0.35))
                return await func(*args, **kwargs)
            except discord.HTTPException as e:
                if e.status == 429:
                    backoff_time = delay + random.uniform(0, delay * 0.25)
                    logger.warning(f"safe_execute[{func_name}]: rate limited (429), backing off {backoff_time:.1f}s (attempt {attempt + 1}/{MAX_RETRIES}).")
                    await asyncio.sleep(backoff_time)
                    delay = min(delay * 2, MAX_BACKOFF)
                else:
                    msg = f"HTTP Error Status: {e.status}"
                    logger.error(f"safe_execute[{func_name}] failed: {msg} — {e.text if hasattr(e, 'text') else e}")
                    obj.config["last_error"] = msg
                    await obj.send_alert(f"⚠️ {func_name} failed", msg, severe=False)
                    return None
            except Exception as e:
                logger.error(f"safe_execute[{func_name}] raised {type(e).__name__}: {e}", exc_info=True)
                obj.config["last_error"] = str(e)
                await obj.send_alert(f"⚠️ {func_name} raised {type(e).__name__}", str(e), severe=False)
                return None
        logger.error(f"safe_execute[{func_name}]: exhausted {MAX_RETRIES} retries (rate limiting), giving up.")
        obj.config["last_error"] = f"{func_name}: exhausted retries after repeated rate limiting"
        await obj.send_alert(f"⚠️ {func_name} gave up", "Exhausted retries after repeated rate limiting.", severe=False)
        return None

    async def _retry_click_after_validation_failure(obj, channel, msg_id, select_button, context_label, max_attempts=2):
        """
        RELIABILITY FIX: COMPONENT_VALIDATION_FAILED (50035) is a known,
        intermittent Discord-side quirk (it also hits official bots) that
        usually clears up once the message/component is re-fetched. This
        used to be a single bounded retry, duplicated three times across the
        codebase (initial click, dodge re-click, post-interruption re-click)
        with only the first getting a retry at all. This shared helper gives
        every click site the same recovery: up to `max_attempts` refetch +
        re-click tries, with a short growing backoff so two back-to-back
        hiccups don't both need to clear in the same instant.
        Returns (clicked_ok, fresh_message_or_None).
        """
        fresh = None
        for attempt in range(1, max_attempts + 1):
            try:
                await asyncio.sleep(0.3 * attempt)
                fresh = await channel.fetch_message(msg_id)
                retry_btn = select_button(fresh)
                if retry_btn is not None:
                    await retry_btn.click()
                    logger.info(
                        "[%s] Recovered from COMPONENT_VALIDATION_FAILED via refetch (attempt %s/%s).",
                        context_label, attempt, max_attempts,
                    )
                    return True, fresh
            except Exception as retry_exc:
                logger.warning(
                    "[%s] Retry %s/%s after COMPONENT_VALIDATION_FAILED also failed: %s",
                    context_label, attempt, max_attempts, retry_exc,
                )
        return False, fresh

    async def run_step(obj, channel, commands_dict, step_name):
        prefix = obj.config.get(f"cmd_prefix_{step_name}", "/")
        kwargs = obj.STEP_SPECS[step_name]()

        if prefix == "!":
            parts = [f"!{step_name}"] + [str(v) for v in kwargs.values()]
            text = " ".join(parts)
            result = await obj.safe_execute(channel.send, text)
        else:
            cmd = commands_dict.get(step_name)
            if not cmd:
                msg = f"Command `{step_name}` not found in cache (target bot may have changed its commands)."
                logger.warning(msg)
                obj.config["last_error"] = msg
                return None
            result = await obj.safe_execute(cmd, channel, **kwargs)

        if result is not None:
            stat_target = obj.STAT_KEYS.get(step_name)
            if stat_target:
                obj.config[stat_target] += 1
        return result

    def _fail_count_pause_active(obj):
        """Return True while the automatic 5/6 Fail Count emergency pause is active.

        The timestamp is absolute, so the lock lasts exactly 62 minutes from
        the warning message instead of depending on loop ticks. When it expires,
        restore the pause state that existed before the warning.
        """
        until = float(obj.config.get("fail_count_pause_until", 0.0) or 0.0)
        if until <= 0:
            return False
        now = time.time()
        if now < until:
            return True

        # Expired: clear the emergency lock and restore the previous manual pause state.
        obj.config["fail_count_pause_until"] = 0.0
        obj.config["is_paused"] = bool(obj.config.get("fail_count_pause_prev_is_paused", False))
        obj.config["fail_count_pause_prev_is_paused"] = False
        try:
            asyncio.create_task(obj.save_state())
        except Exception:
            pass
        logger.info("[FIGHT_HORDE] 5/6 Fail Count emergency pause expired; automation may resume.")
        return False

    async def _activate_fail_count_pause(obj, channel=None):
        """Pause all automation for exactly 62 minutes after a 5/6 warning."""
        now = time.time()
        pause_until = now + 62 * 60
        current_until = float(obj.config.get("fail_count_pause_until", 0.0) or 0.0)

        # Do not shorten an already-active lock.
        if current_until > now:
            pause_until = max(current_until, pause_until)
        else:
            obj.config["fail_count_pause_prev_is_paused"] = bool(obj.config.get("is_paused", False))

        obj.config["fail_count_pause_until"] = pause_until
        obj.config["is_paused"] = True
        obj.active_sequence = "fail_count_emergency_pause"
        await obj.save_state()

        remaining = max(0, int(round(pause_until - now)))
        logger.warning(
            "[FIGHT_HORDE] Detected Fail Count 5/6 within 60 minutes. "
            "ALL automation paused for exactly 62 minutes (%ss).", remaining
        )
        try:
            await obj.notify_channel(
                "🛑 Fight Horde Safety Pause",
                "Detected **Fail Count 5/6 within 60 minutes**. All automation is paused for exactly **62 minutes**.",
            )
        except Exception:
            pass
        return pause_until

    def _still_active(obj):
        if obj._fail_count_pause_active():
            return False
        return obj.farm_loop.is_running() and not obj.config.get("is_paused", False)

    async def _jitter_sleep(obj, base_gap):
        # Do not advance farming while Trivia has priority.
        await _TRIVIA_IDLE_EVENT.wait()
        await asyncio.sleep(calculate_bell_curve_delay(base_gap, obj.config["loop_jitter"]))

    def _retime(obj):
        next_tick = calculate_bell_curve_delay(obj.config["loop_interval"], obj.config["loop_jitter"])
        obj.farm_loop.change_interval(seconds=max(1.0, next_tick))

    def _log_progress_if_due(obj):
        interval = obj.config.get("log_summary_interval_loops", 0)
        n = obj.config["stats_loops_completed"]
        if interval <= 0 or n == 0 or n % interval != 0:
            return
        uptime = (
            str(timedelta(seconds=int(time.time() - obj.config["stats_start_time"])))
            if obj.config["stats_start_time"]
            else "n/a"
        )
        logger.info(
            f"[Progress] loops={n:,} uptime={uptime} "
            f"refine_fail_streak={obj.config['refine_consecutive_failures']} "
            f"last_error={obj.config['last_error'] or 'none'}"
        )

    # -----------------------------------------------------------------------
    # Tick handlers
    # -----------------------------------------------------------------------
    async def _handle_sleep(obj) -> bool:
        if not obj.config.get("sleep_enabled", False):
            return False

        current_hour = datetime.now(TZ_GMT1).hour
        start_hour = obj.config["sleep_start_hour"]
        wake_hour = (start_hour + obj.config["sleep_duration_hours"]) % 24
        is_sleeping = (
            start_hour <= current_hour < wake_hour
            if start_hour < wake_hour
            else (current_hour >= start_hour or current_hour < wake_hour)
        )
        if not is_sleeping:
            return False

        obj.farm_loop.change_interval(seconds=900.0)
        return True

    async def _handle_break(obj, now) -> bool:
        if now - obj.config["last_break_time"] < obj.config["break_interval"]:
            return False

        actual_break = calculate_bell_curve_delay(obj.config["break_duration"], obj.config["loop_jitter"] * 12)
        await obj.notify_channel("☕ Fatigue Protocol Engaged", f"Simulating human break sequence. Idling for {actual_break:.0f}s.")
        await asyncio.sleep(max(10, actual_break))

        obj.config["stats_total_break_time"] += int(actual_break)
        obj.config["last_break_time"] = time.time()
        await obj.save_state()
        obj._retime()
        return True

    async def _handle_planting(obj, channel, commands_dict, now) -> bool:
        if not obj.config["plant_enabled"]:
            return False

        eff_last_plant = obj.config["last_plant_run"] or (now - obj.config["plant_interval"])
        time_until_plant = obj.config["plant_interval"] - (now - eff_last_plant)
        if time_until_plant > 5:
            return False
        if time_until_plant > 0:
            await asyncio.sleep(time_until_plant)

        obj.active_sequence = "plant"
        try:
            if obj.config.get("first_plant_done", False):
                await obj.run_step(channel, commands_dict, "harvest_plots")
                await obj._jitter_sleep(obj.config["plant_step_gap"])

            for _ in range(obj.config["plant_repeats"]):
                if not obj._still_active():
                    return True
                await obj.run_step(channel, commands_dict, "plant")
                await obj._jitter_sleep(obj.config["plant_step_gap"])

            obj.config["first_plant_done"] = True
            obj.config["last_plant_run"] = time.time()
            obj.config["stats_loops_completed"] += 1
            await obj.save_state()
            obj._retime()
            obj._log_progress_if_due()
            return True
        finally:
            obj.active_sequence = "idle"

    async def _handle_refine(obj, channel, commands_dict, now) -> bool:
        if not obj.config["refine_enabled"]:
            return False

        eff_last_refine = obj.config["last_refine_run"] or (now - obj.config["refine_interval"])
        if now - eff_last_refine < obj.config["refine_interval"]:
            return False

        obj.active_sequence = "refine"
        try:
            if not obj._still_active():
                return True

            result = await obj.run_step(channel, commands_dict, "refine")
            obj.config["last_refine_run"] = now

            if result is None:
                obj.config["stats_refine_failures"] += 1
                obj.config["refine_consecutive_failures"] += 1
                if obj.config["refine_consecutive_failures"] >= obj.config["refine_max_consecutive_failures"]:
                    obj.config["refine_enabled"] = False
                    msg = f"`/refine` failed {obj.config['refine_consecutive_failures']}x in a row. Check materials, then `~~toggle refine` to re-enable."
                    await obj.notify_channel("⚠️ Refine Auto-Disabled", msg)
                    await obj.send_alert("⚠️ Refine Auto-Disabled", msg, severe=True)
                    obj.config["refine_consecutive_failures"] = 0
            else:
                obj.config["refine_consecutive_failures"] = 0

            obj.config["stats_loops_completed"] += 1
            await obj.save_state()
            obj._retime()
            obj._log_progress_if_due()
            return True
        finally:
            obj.active_sequence = "idle"

    # -----------------------------------------------------------------------
    # Interactive Component Logic (Fight Horde / Hunt)
    # -----------------------------------------------------------------------
    async def _collect_component_responses(obj, check_msg, count, timeout) -> list:
        """Collect up to `count` distinct messages satisfying check_msg within
        `timeout` seconds, using a PERSISTENT listener (registered once, for
        the whole window) backed by an asyncio.Queue.

        BUGFIX: the previous implementation (_wait_for_component_message)
        registered a fresh `bot.wait_for` pair, waited for ONE match, then
        looped back and registered a brand-new pair for the NEXT match.
        Between one iteration finishing and the next one registering, there
        is a real gap where no listener is active at all. discord.py only
        delivers an event to whatever listeners exist at the exact moment
        it's dispatched -- anything arriving during that gap is dropped
        forever, it is never redelivered. When firing a batch of commands
        back-to-back, several responses can land within milliseconds of
        each other, so it was easy for every response except whichever one
        happened to arrive after the listener was re-armed (typically the
        last one) to be silently missed. Registering the listeners ONCE for
        the entire collection window removes that gap entirely.
        """
        queue: asyncio.Queue = asyncio.Queue()

        async def on_msg(m):
            if check_msg(m):
                await queue.put(m)

        async def on_edit(before, after):
            if check_msg(after):
                await queue.put(after)

        obj.bot.add_listener(on_msg, name="on_message")
        obj.bot.add_listener(on_edit, name="on_message_edit")
        try:
            collected = []
            seen_ids = set()
            deadline = time.time() + timeout
            while len(collected) < count:
                # Trivia has priority. Keep the collector/listeners alive so
                # responses arriving during Trivia are not lost; simply pause
                # consumption until every active Trivia is finished.
                await _TRIVIA_IDLE_EVENT.wait()

                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                try:
                    # Use short polling slices so Trivia priority can be
                    # re-checked frequently, but DO NOT treat an empty 0.25s
                    # slice as the end of the collection window. The previous
                    # code broke here and therefore gave fight_horde/hunt only
                    # ~250 ms to receive a response, which caused the repeated
                    # "only 0/1 response(s)" warnings.
                    msg = await asyncio.wait_for(queue.get(), timeout=min(remaining, 0.25))
                except asyncio.TimeoutError:
                    continue
                if msg.id in seen_ids:
                    continue
                seen_ids.add(msg.id)
                collected.append(msg)
            return collected
        finally:
            obj.bot.remove_listener(on_msg, name="on_message")
            obj.bot.remove_listener(on_edit, name="on_message_edit")

    async def _execute_and_click_first_button(obj, channel, commands_dict, cmd_name, stat_key, last_run_key, now, batch_size=1, batch_gap_key=None, prefix_key=None, feature_key=None) -> bool:
        # NEW: per-feature "!" prefix support (e.g. prefix_key="cmd_prefix_fight_horde")
        prefix = obj.config.get(prefix_key, "/") if prefix_key else "/"

        async def _record_failure():
            # NEW: consecutive-failure auto-pause, mirroring refine's existing
            # pattern. Applies whether the command was missing from the cache
            # OR every attempt in the batch failed — either way, "kept
            # failing in silence" is exactly what this replaces.
            if not feature_key:
                return
            fail_stat = f"stats_{feature_key}_failures"
            consec_key = f"{feature_key}_consecutive_failures"
            max_key = f"{feature_key}_max_consecutive_failures"
            enabled_key = f"{feature_key}_enabled"
            obj.config[fail_stat] = obj.config.get(fail_stat, 0) + 1
            obj.config[consec_key] = obj.config.get(consec_key, 0) + 1
            if obj.config[consec_key] >= obj.config.get(max_key, 3):
                obj.config[enabled_key] = False
                fail_msg = f"`{cmd_name}` failed {obj.config[consec_key]}x in a row. Check materials/cooldowns, then `~~toggle {feature_key}` to re-enable."
                await obj.notify_channel(f"⚠️ {feature_key.replace('_', ' ').title()} Auto-Disabled", fail_msg)
                await obj.send_alert(f"⚠️ {feature_key.replace('_', ' ').title()} Auto-Disabled", fail_msg, severe=True)
                obj.config[consec_key] = 0

        def _record_success():
            if feature_key:
                obj.config[f"{feature_key}_consecutive_failures"] = 0

        cmd = None
        if prefix == "/":
            cmd = commands_dict.get(cmd_name)
            if not cmd:
                cmd = obj.resolve_dynamic_command(cmd_name)

            if not cmd:
                msg = f"Command `/{cmd_name}` not found in cache."
                logger.warning(msg)
                obj.config["last_error"] = msg
                await _record_failure()
                # BUGFIX: this used to `return True` here WITHOUT updating
                # last_run_key. Since the due-check compares against
                # last_run_key, never advancing it meant this branch would be
                # hit again on the VERY NEXT tick, and every tick after that,
                # forever -- and because this handler returns True (claiming
                # the pipeline slot for the tick), every OTHER handler checked
                # after it in the pipeline (plant, refine, modular_chain, the
                # independent slots, whichever this one wasn't) would never run
                # again either. Advancing the timer here means a genuinely
                # missing/misnamed command just gets retried once per interval
                # (and logged), instead of permanently starving the rest of the
                # pipeline.
                obj.config[last_run_key] = time.time()
                obj.config["stats_loops_completed"] += 1
                await obj.save_state()
                obj._retime()
                obj._log_progress_if_due()
                return True

        batch_size = max(1, int(batch_size))
        gap = obj.config.get(batch_gap_key, 0.5) if batch_gap_key else 0.5
        batch_started_at = time.time()

        def check_msg(m):
            # Only scope by author/channel here. A response may arrive once
            # without components and then receive its buttons through an edit;
            # the collector's on_message_edit path handles that second event.
            if getattr(getattr(m, "author", None), "id", None) != TARGET_BOT_ID:
                return False
            if getattr(getattr(m, "channel", None), "id", None) != channel.id:
                return False
            inter = getattr(m, "interaction", None)
            if inter and getattr(inter, "user", None):
                if getattr(inter.user, "id", None) != obj.bot.user.id:
                    return False
            return bool(getattr(m, "components", None))

        # BUGFIX: the collector is now armed BEFORE we fire anything, and
        # stays armed for the whole batch, so there's no window where a
        # fast response could arrive before we're listening for it.
        collect_timeout = min(90.0, 15.0 * batch_size)
        collector_task = asyncio.ensure_future(
            obj._collect_component_responses(check_msg, batch_size, collect_timeout)
        )
        # Give the collector one event-loop turn to register its listeners
        # before the command is fired. This closes the last scheduling race.
        await asyncio.sleep(0)

        fired = 0
        for i in range(batch_size):
            if not obj._still_active():
                break
            if prefix == "!":
                result = await obj.safe_execute(channel.send, f"!{cmd_name}")
            else:
                result = await obj.safe_execute(cmd, channel)
            if result is not None:
                obj.config[stat_key] += 1
                fired += 1
            if i < batch_size - 1:
                await obj._jitter_sleep(gap)

        if fired == 0:
            collector_task.cancel()
            try:
                await collector_task
            except (asyncio.CancelledError, Exception):
                pass
            await _record_failure()
            obj.config[last_run_key] = time.time()
            obj.config["stats_loops_completed"] += 1
            await obj.save_state()
            obj._retime()
            obj._log_progress_if_due()
            return True

        _record_success()
        collected = await collector_task

        # Gateway events are normally sufficient, but a component response can
        # occasionally be missed while listeners are being attached/removed or
        # when the target bot edits its original response. As a low-frequency
        # safety net, inspect a small recent history window when the collector
        # did not receive every fired response. This does not replace the live
        # listener; it only recovers responses that are already in the channel.
        if len(collected) < fired:
            try:
                recovered_ids = {getattr(m, "id", None) for m in collected}
                history_limit = max(10, min(50, fired * 10))
                # Only consider messages created at/after this batch started.
                # This prevents an old fight/hunt message with buttons from
                # being mistaken for the response to the current command.
                history_after = datetime.fromtimestamp(
                    batch_started_at - 1.0, tz=timezone.utc
                )
                async for recent in channel.history(limit=history_limit, after=history_after):
                    if getattr(recent, "id", None) in recovered_ids:
                        continue
                    recent_created_at = getattr(recent, "created_at", None)
                    if recent_created_at is not None:
                        try:
                            if recent_created_at.timestamp() < batch_started_at - 1.0:
                                continue
                        except Exception:
                            pass
                    if getattr(getattr(recent, "author", None), "id", None) != TARGET_BOT_ID:
                        continue
                    if not getattr(recent, "components", None):
                        continue
                    inter = getattr(recent, "interaction", None)
                    if inter and getattr(inter, "user", None):
                        if getattr(inter.user, "id", None) != obj.bot.user.id:
                            continue
                    collected.append(recent)
                    recovered_ids.add(getattr(recent, "id", None))
                    if len(collected) >= fired:
                        break
                if len(collected) >= fired:
                    logger.info(
                        "[%s] Recovered missing component response(s) from channel history: %s/%s.",
                        cmd_name.upper(), len(collected), fired,
                    )
            except Exception as exc:
                logger.debug(
                    "[%s] History fallback unavailable: %s",
                    cmd_name.upper(), exc,
                )

        if len(collected) < fired:
            msg = (
                f"`/{cmd_name}` batch: only {len(collected)}/{fired} response(s) with buttons arrived in time "
                f"(the rest may have hit a cooldown or other rejection on the target bot)."
            )
            logger.warning(msg)
            obj.config["last_error"] = msg

        for idx, resp in enumerate(collected):
            if not obj._still_active():
                break

            await _TRIVIA_IDLE_EVENT.wait()

            btn = None
            buttons = []
            for row in resp.components:
                children = getattr(row, "children", None)
                candidates = children if children is not None else [row]
                for child in candidates:
                    if hasattr(child, 'click') and not getattr(child, 'disabled', False):
                        buttons.append(child)

            if cmd_name == "fight_horde":
                # Robust Attack selection for fight_horde only.
                # Prefer semantic evidence from label/custom_id/emoji/component
                # data, rather than Discord's component order.
                def _norm(value):
                    value = str(value or "").strip().lower()
                    return re.sub(r"\s+", " ", value)

                def _component_text(component):
                    parts = [
                        _norm(getattr(component, "label", None)),
                        _norm(getattr(component, "custom_id", None)),
                        _norm(getattr(component, "emoji", None)),
                    ]

                    emoji = getattr(component, "emoji", None)
                    if emoji is not None:
                        parts.extend([
                            _norm(getattr(emoji, "name", None)),
                            _norm(getattr(emoji, "id", None)),
                            _norm(getattr(emoji, "unicode_emoji", None)),
                        ])

                    # Some discord.py-self component objects expose their raw
                    # payload through to_dict(). Include it when available so
                    # opaque/custom component representations can still be
                    # inspected without changing the component itself.
                    try:
                        raw = component.to_dict()
                        if isinstance(raw, dict):
                            for key in ("label", "custom_id", "emoji"):
                                value = raw.get(key)
                                if isinstance(value, dict):
                                    parts.extend(
                                        _norm(value.get(k))
                                        for k in ("name", "id", "unicode_emoji")
                                    )
                                else:
                                    parts.append(_norm(value))
                    except Exception:
                        pass

                    return " ".join(p for p in parts if p)

                # Strong matches: explicit Attack wording or common attack
                # identifiers used by Discord bots.  Include common Spanish
                # variants because the visible button does not have to be
                # English.
                strong_tokens = (
                    "attack", "atacar", "ataque", "fight", "horde_attack",
                    "attack_button", "btn_attack", "action_attack",
                )
                attack_emojis = (
                    "⚔", "⚔️", "🗡", "🗡️", "🔪", "🥊", "👊",
                    "crossed_swords", "crossed swords",
                )
                negative_tokens = (
                    "flee", "escape", "run", "back", "cancel", "leave",
                    "defend", "defense", "defence", "heal", "inventory",
                )

                scored = []
                for candidate in buttons:
                    haystack = _component_text(candidate)
                    score = 0

                    # Exact/near-exact label matches are strongest.
                    label = _norm(getattr(candidate, "label", None))
                    custom_id = _norm(getattr(candidate, "custom_id", None))

                    # A visible label containing "attack" is sufficient to
                    # select this button, regardless of surrounding emoji/text.
                    if "attack" in label:
                        score += 150
                    if label in ("atacar", "ataque"):
                        score += 100
                    if custom_id in ("attack", "atacar", "ataque", "fight"):
                        score += 90

                    for token in strong_tokens:
                        if token in haystack:
                            score += 40

                    for emoji in attack_emojis:
                        if emoji in haystack:
                            score += 35

                    for token in negative_tokens:
                        if token in haystack:
                            score -= 100

                    if score > 0:
                        scored.append((score, candidate))

                if scored:
                    scored.sort(key=lambda item: item[0], reverse=True)
                    best_score, best_button = scored[0]
                    btn = best_button

                    logger.info(
                        "[FIGHT_HORDE] Selected Attack button | score=%s label=%r custom_id=%r",
                        best_score,
                        getattr(btn, "label", None),
                        getattr(btn, "custom_id", None),
                    )
                elif len(buttons) == 1:
                    # If the target supplies only one actionable button, it is
                    # necessarily the only available action. This is safer than
                    # assuming a position when multiple actions exist.
                    btn = buttons[0]
                    logger.info(
                        "[FIGHT_HORDE] Only one active button; using it as Attack | label=%r custom_id=%r",
                        getattr(btn, "label", None),
                        getattr(btn, "custom_id", None),
                    )
                elif buttons:
                    # Multiple opaque buttons with no semantic Attack marker:
                    # preserve the existing fallback, but make the ambiguity
                    # explicit in logs so it can be diagnosed from one run.
                    logger.warning(
                        "[FIGHT_HORDE] Attack button could not be identified; "
                        "falling back to first active button. buttons=%s",
                        [
                            {
                                "label": getattr(b, "label", None),
                                "custom_id": getattr(b, "custom_id", None),
                                "emoji": str(getattr(b, "emoji", None)),
                            }
                            for b in buttons
                        ],
                    )
                    btn = buttons[0]
            elif cmd_name == "hunt" and buttons:
                # Hunt uses the same robust button selection strategy as
                # fight_horde, including label/custom_id/emoji matching.
                def _norm_hunt(value):
                    return re.sub(r"\s+", " ", str(value or "").strip().lower())

                def _hunt_text(component):
                    parts = [
                        _norm_hunt(getattr(component, "label", None)),
                        _norm_hunt(getattr(component, "custom_id", None)),
                        _norm_hunt(getattr(component, "emoji", None)),
                    ]
                    emoji = getattr(component, "emoji", None)
                    if emoji is not None:
                        parts.extend([
                            _norm_hunt(getattr(emoji, "name", None)),
                            _norm_hunt(getattr(emoji, "id", None)),
                            _norm_hunt(getattr(emoji, "unicode_emoji", None)),
                        ])
                    try:
                        raw = component.to_dict()
                        if isinstance(raw, dict):
                            parts.append(_norm_hunt(raw.get("label")))
                            parts.append(_norm_hunt(raw.get("custom_id")))
                            raw_emoji = raw.get("emoji")
                            if isinstance(raw_emoji, dict):
                                parts.extend(_norm_hunt(raw_emoji.get(k)) for k in ("name", "id", "unicode_emoji"))
                            else:
                                parts.append(_norm_hunt(raw_emoji))
                    except Exception:
                        pass
                    return " ".join(p for p in parts if p)

                hunt_positive = (
                    "hunt", "hunting", "cazar", "caza", "attack", "atacar", "ataque",
                    "hunt_button", "btn_hunt", "action_hunt",
                )
                hunt_negative = (
                    "flee", "escape", "run", "back", "cancel", "leave",
                    "defend", "defense", "defence", "heal", "inventory",
                )

                scored = []
                for candidate in buttons:
                    label = _norm_hunt(getattr(candidate, "label", None))
                    haystack = _hunt_text(candidate)
                    score = 0
                    if label in ("hunt", "hunting", "cazar", "caza"):
                        score += 150
                    for token in hunt_positive:
                        if token in haystack:
                            score += 40
                    for token in hunt_negative:
                        if token in haystack:
                            score -= 100
                    if score > 0:
                        scored.append((score, candidate))

                if scored:
                    scored.sort(key=lambda item: item[0], reverse=True)
                    best_score, btn = scored[0]
                    logger.info(
                        "[HUNT] Selected action button | score=%s label=%r custom_id=%r",
                        best_score, getattr(btn, "label", None), getattr(btn, "custom_id", None),
                    )
                else:
                    btn = buttons[0]
                    logger.warning(
                        "[HUNT] Could not identify Hunt button; using first active button | label=%r custom_id=%r",
                        getattr(btn, "label", None), getattr(btn, "custom_id", None),
                    )
            elif buttons:
                # Preserve the original first-active-button behavior for every
                # command other than fight_horde/hunt.
                btn = buttons[0]

            clicked_ok = False
            if btn:
                # Close the (small) race window between the trivia-priority
                # check earlier in this loop and the actual click: re-check
                # right before firing so nothing ever clicks while a Trivia
                # interaction is in-flight. Applies to every command that
                # goes through this shared clicker (plant/refine/fight_horde/
                # hunt), not just fight_horde.
                await _TRIVIA_IDLE_EVENT.wait()
                try:
                    await btn.click()
                    clicked_ok = True
                except discord.HTTPException as e:
                    detail = getattr(e, "text", None) or str(e)
                    code = getattr(e, "code", None)
                    # COMPONENT_VALIDATION_FAILED (50035) is a known,
                    # intermittent Discord-side quirk that is not specific to
                    # our logic — it also hits official bots — and usually
                    # clears up once the message/component is re-fetched.
                    # RELIABILITY FIX: bumped from a single retry that only
                    # applied to the first button in a batch (idx == 0) to
                    # a bounded 2-attempt retry (with backoff) that applies
                    # to every button in the batch, via the shared helper —
                    # this glitch isn't specific to whichever button happens
                    # to be first.
                    if code == 50035:
                        def _first_active_button(msg):
                            for row in getattr(msg, "components", None) or []:
                                for child in (getattr(row, "children", None) or [row]):
                                    if hasattr(child, "click") and not getattr(child, "disabled", False):
                                        return child
                            return None

                        clicked_ok, fresh = await obj._retry_click_after_validation_failure(
                            channel, resp.id, _first_active_button, cmd_name.upper(),
                        )
                        if clicked_ok and fresh is not None:
                            # Keep downstream processing (e.g. fight_horde's
                            # post-attack loop) working from the message that
                            # was actually clicked, not the stale pre-retry copy.
                            resp = fresh
                    if not clicked_ok:
                        msg = f"Failed clicking `{cmd_name}` button {idx + 1}/{len(collected)} (HTTP {e.status}): {detail}"
                        logger.error(msg)
                        obj.config["last_error"] = msg
                except Exception as e:
                    msg = f"Failed interacting with `{cmd_name}` button {idx + 1}/{len(collected)}: {e}"
                    logger.error(msg, exc_info=True)
                    obj.config["last_error"] = msg
            else:
                msg = f"No clickable active buttons found in `{cmd_name}` response {idx + 1}/{len(collected)}."
                logger.warning(msg)
                obj.config["last_error"] = msg

            # NEW: fight_horde keeps going after the first click — re-clicking
            # Attack on "Horde dodged!", healing via Gu-swap when HP is low,
            # riding out a mid-fight Trivia interruption, and finally checking
            # for Primeval Essence loot before letting the pipeline move on.
            if cmd_name == "fight_horde" and clicked_ok:
                try:
                    await obj._fight_horde_post_attack(channel, resp)
                except Exception as exc:
                    logger.error("[FIGHT_HORDE] Post-attack processing failed: %s", exc, exc_info=True)

            if idx < len(collected) - 1:
                await obj._jitter_sleep(gap)

        obj.config[last_run_key] = time.time()
        obj.config["stats_loops_completed"] += 1
        await obj.save_state()
        obj._retime()
        obj._log_progress_if_due()
        return True

    # -----------------------------------------------------------------------
    # Fight Horde: dodge re-click / low-HP heal / trivia-continuation / loot
    # -----------------------------------------------------------------------
    async def _wait_for_horde_continuation(obj, channel, prev_msg_id, timeout=10.0):
        """Wait for the next fight_horde state without losing a new message
        that is created while Trivia is being solved.

        Some target-bot flows create a brand-new message first and only attach
        the fight buttons in a later ``on_message_edit`` event. The old
        collector only accepted edits to ``prev_msg_id``, so that continuation
        could be silently missed and the main loop would eventually fire a new
        ``/fight_horde``.
        """
        queue: asyncio.Queue = asyncio.Queue()
        queued_ids = set()

        # Confirmation text from the heal sequence's own equip/unequip/use_gu
        # commands (fired moments earlier, possibly still in flight) so it
        # never gets mistaken for the horde's actual continuation.
        non_combat_markers = ("equipped", "unequipped", "you used", "you have used")

        def relevant(m):
            if getattr(getattr(m, "author", None), "id", None) != TARGET_BOT_ID:
                return False
            if getattr(getattr(m, "channel", None), "id", None) != channel.id:
                return False
            return True

        def looks_like_combat(m):
            # A message with active buttons is almost certainly a fight prompt.
            # Without buttons, require non-empty text that does not look like
            # one of our heal command confirmations. This lets an initially
            # empty message be ignored, while a later edit that adds buttons
            # is accepted.
            if getattr(m, "components", None):
                return True
            text = _gather_message_text(m).lower()
            if any(marker in text for marker in non_combat_markers):
                return False
            return bool(text)

        async def enqueue(m, source):
            message_id = getattr(m, "id", None)
            if message_id is None or message_id in queued_ids:
                return
            if not relevant(m) or not looks_like_combat(m):
                return
            queued_ids.add(message_id)
            logger.info(
                "[FIGHT_HORDE] Captured continuation message | source=%s | id=%s | prev=%s",
                source, message_id, prev_msg_id,
            )
            await queue.put(m)

        async def on_msg(m):
            await enqueue(m, "on_message")

        async def on_edit(before, after):
            if not relevant(after):
                return
            # Keep the original-message edit behavior, but ALSO accept a new
            # target-bot message that gains fight buttons after being created.
            # This is the key recovery path for Trivia -> fight_horde resumes.
            after_id = getattr(after, "id", None)
            if after_id == prev_msg_id or looks_like_combat(after):
                await enqueue(after, "on_message_edit")

        async def recover_recent():
            """Recover a continuation that arrived just before our listener
            was armed or whose initial gateway event was missed."""
            try:
                history_after = datetime.fromtimestamp(
                    time.time() - max(3.0, timeout),
                    tz=timezone.utc,
                )
                async for recent in channel.history(limit=20, after=history_after):
                    if getattr(recent, "id", None) == prev_msg_id:
                        continue
                    if relevant(recent) and looks_like_combat(recent):
                        await enqueue(recent, "history")
                        # The newest matching combat message is enough.
                        if not queue.empty():
                            break
            except Exception as exc:
                logger.debug("[FIGHT_HORDE] Continuation history recovery unavailable: %s", exc)

        obj.bot.add_listener(on_msg, name="on_message")
        obj.bot.add_listener(on_edit, name="on_message_edit")
        try:
            # Close the scheduling/history race before waiting for the normal
            # gateway path. This is intentionally bounded and only inspects the
            # last few seconds around the active fight.
            await asyncio.sleep(0)
            await recover_recent()

            deadline = time.time() + timeout
            # PERF FIX: recover_recent() makes a real channel.history() API
            # call. The old code ran it on every 0.25s queue-poll timeout,
            # i.e. up to ~4x/second for the whole wait window (up to 15s per
            # dodge round). That hammered the Discord API during every fight,
            # which is what made fight_horde (and anything queued behind it,
            # like the post-fight vault deposit) feel slow — and risked
            # tripping 429 rate limits that then slow down every OTHER
            # sequence too via safe_execute's backoff. The live on_message /
            # on_message_edit listeners already catch the normal case
            # instantly; history is only a safety net for missed gateway
            # events, so it only needs to run occasionally, not every tick.
            last_recover = 0.0
            recover_interval = 1.5
            while True:
                if _TRIVIA_ACTIVE_COUNT > 0:
                    # Keep pushing the deadline out for as long as a trivia is
                    # being solved, plus give the normal `timeout` worth of
                    # grace afterward for the target bot to react.
                    deadline = time.time() + timeout
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=min(remaining, 0.25))
                except asyncio.TimeoutError:
                    # A response can be delivered through history/edit even if
                    # the live event was missed. Re-check, but throttled.
                    now_ts = time.time()
                    if now_ts - last_recover >= recover_interval:
                        last_recover = now_ts
                        await recover_recent()
                    continue
                return msg
        finally:
            obj.bot.remove_listener(on_msg, name="on_message")
            obj.bot.remove_listener(on_edit, name="on_message_edit")

    async def _fight_horde_heal_sequence(obj, channel):
        """Low-HP recovery: swap off the appraisal Gu, equip + use the
        restoration Gu the configured number of times, then swap back."""
        gap = obj.config.get("fight_horde_heal_step_gap", 1.5)
        unequip_name = obj.config.get("gu1_name", "Blade Appraisal Gu")
        heal_name = obj.config.get("gu2_name", "Sword Body Restoration Gu")
        use_times = max(1, int(obj.config.get("fight_horde_heal_use_times", 2)))

        async def _run(cmd_name, **kwargs):
            cmd = obj.resolve_dynamic_command(cmd_name)
            if not cmd:
                msg = f"[FIGHT_HORDE][HEAL] Command `/{cmd_name}` not found in cache."
                logger.warning(msg)
                obj.config["last_error"] = msg
                return None
            return await obj.safe_execute(cmd, channel, **kwargs)

        logger.info("[FIGHT_HORDE][HEAL] Low HP detected, running Gu-swap heal sequence.")
        await _run("unequip", gu_name=unequip_name)
        await obj._jitter_sleep(gap)
        await _run("equip", gu_name=heal_name)
        obj.config["active_gu"] = "gu2"
        await obj._jitter_sleep(gap)
        for _ in range(use_times):
            await _run("use_gu", gu_name=heal_name)
            await obj._jitter_sleep(gap)
        await _run("unequip", gu_name=heal_name)
        await obj._jitter_sleep(gap)
        swap_back = await _run("equip", gu_name=unequip_name)
        if swap_back is not None:
            obj.config["active_gu"] = "gu1"
        else:
            # The final re-equip failed (command not found / safe_execute gave
            # up) -- do NOT claim gu1 is active when it likely isn't. Leaving
            # active_gu as "gu2" keeps later logic honest about real state
            # and surfaces the mismatch instead of hiding it.
            msg = "[FIGHT_HORDE][HEAL] Failed to re-equip Gu1 after healing; active_gu left as gu2 to reflect real state."
            logger.error(msg)
            obj.config["last_error"] = msg

    async def _fight_horde_post_attack(obj, channel, first_response, max_rounds=40):
        """Runs right after the initial Attack click on a fight_horde
        response. Keeps the fight going through repeated dodges, heals when
        HP is low, rides out a mid-fight Trivia interruption by waiting for
        and clicking whatever the target bot sends next, and finally
        deposits to the vault if the essence keyword never showed up."""
        essence_keyword = obj.config.get("fight_horde_essence_keyword", "Primeval Essence")
        essence_amount = max(1, int(obj.config.get("fight_horde_essence_amount", 50)))
        deposit_amount = obj.config.get("fight_horde_vault_deposit_amount", 2000)
        # Loot is considered found when the fight result contains the configured
        # amount + essence name, e.g. "+50 Primeval Essence" or "+100 Immortal Essence".
        essence_pattern = re.compile(
            rf"\+\s*{re.escape(str(essence_amount))}\s+{re.escape(str(essence_keyword))}",
            re.IGNORECASE,
        )
        essence_seen = False
        seen_ids = {getattr(first_response, "id", None)}
        current = first_response

        for _ in range(max_rounds):
            if not obj._still_active():
                break

            text = _gather_message_text(current)
            if essence_pattern.search(text):
                essence_seen = True

            hp = _extract_player_hp(text)
            if hp is not None:
                current_hp, max_hp = hp
                if max_hp > 0 and current_hp <= (max_hp * 0.50):
                    logger.info(
                        "[FIGHT_HORDE][HEAL] HP at %s/%s (%.1f%%) — 50%% max-HP threshold reached.",
                        current_hp, max_hp, (current_hp / max_hp) * 100,
                    )
                    await obj._fight_horde_heal_sequence(channel)

            dodged = "horde dodged" in text.lower()
            if dodged:
                btn = _select_attack_button(current)
                if btn:
                    # IMPORTANT: never click while a Trivia interaction is
                    # in-flight. discord.py-self's component .click() waits
                    # on a single global "interaction finish" gateway event;
                    # firing a second click here while Trivia's own click()
                    # is still waiting on that same mechanism races the two
                    # and can make the Trivia's click time out with
                    # "Did not receive a response from Discord", which is
                    # what was happening before this guard was added.
                    await _TRIVIA_IDLE_EVENT.wait()
                    dodge_clicked = False
                    try:
                        await btn.click()
                        dodge_clicked = True
                    except discord.HTTPException as e:
                        # RELIABILITY FIX: COMPONENT_VALIDATION_FAILED (50035)
                        # is the same known, intermittent Discord-side quirk
                        # the initial fight_horde click recovers from. This
                        # dodge re-click used to have no such recovery at
                        # all — any transient click failure here just gave
                        # up on the whole fight, which is what "fight_horde
                        # misses a click" looked like in practice. Now uses
                        # the same bounded 2-attempt retry as every other
                        # click site.
                        if getattr(e, "code", None) == 50035:
                            dodge_clicked, fresh = await obj._retry_click_after_validation_failure(
                                channel, current.id, _select_attack_button, "FIGHT_HORDE dodge re-click",
                            )
                            if fresh is not None:
                                current = fresh
                        if not dodge_clicked:
                            logger.error("[FIGHT_HORDE] Failed re-clicking Attack after dodge: %s", e)
                            break
                    except Exception as e:
                        logger.error("[FIGHT_HORDE] Failed re-clicking Attack after dodge: %s", e)
                        break
                else:
                    logger.warning("[FIGHT_HORDE] 'Horde dodged!' seen but no Attack button to re-click.")
                    break

            # Wait for further activity: either this same message getting
            # edited again (more dodge rounds), or a brand-new message from
            # the target bot (e.g. the fight continuation it sends once a
            # mid-fight Trivia has been answered). Give dodge follow-ups more
            # time since we just fired a click; give a shorter grace window
            # otherwise so a genuinely finished fight doesn't stall the loop.
            updated = await obj._wait_for_horde_continuation(
                channel, getattr(current, "id", None), timeout=(15.0 if dodged else 6.0)
            )
            if updated is None:
                break

            is_new_message = getattr(updated, "id", None) not in seen_ids
            seen_ids.add(getattr(updated, "id", None))

            # IMPORTANT: the message returned here may be the Trivia itself.
            # That is part of the current fight, not a new fight_horde prompt.
            # When the Trivia is answered, the target bot can resume the SAME
            # fight either by editing that message or by sending a fresh message.
            # Never pass the Trivia message through the normal Attack selector.
            trivia_expression = None
            try:
                trivia_expression = _trivia_extract_expression(_trivia_extract_text(updated))
            except Exception:
                trivia_expression = None

            if trivia_expression is not None or _TRIVIA_ACTIVE_COUNT > 0:
                logger.info(
                    "[FIGHT_HORDE] Trivia interruption belongs to active fight; waiting for its post-answer fight continuation."
                )

                # Wait for the Trivia worker to finish answering before trying
                # to touch any fight component. This preserves the existing
                # interaction priority and avoids racing Button.click() calls.
                await _TRIVIA_IDLE_EVENT.wait()

                async def _find_post_trivia_fight():
                    recovery_deadline = time.time() + 12.0
                    while time.time() < recovery_deadline:
                        candidates = []

                        # First refresh the exact message that carried the
                        # Trivia. The target bot may edit it back into the
                        # fight instead of creating another message.
                        updated_id = getattr(updated, "id", None)
                        if updated_id is not None:
                            try:
                                fetch_message = getattr(channel, "fetch_message", None)
                                if callable(fetch_message):
                                    refreshed = await fetch_message(updated_id)
                                    candidates.append(refreshed)
                            except Exception:
                                pass

                        # Then inspect a small recent history window for a new
                        # fight message sent immediately after the answer.
                        try:
                            async for recent in channel.history(limit=20):
                                if getattr(getattr(recent, "author", None), "id", None) != TARGET_BOT_ID:
                                    continue
                                if getattr(getattr(recent, "channel", None), "id", None) != channel.id:
                                    continue
                                candidates.append(recent)
                        except Exception as exc:
                            logger.debug(
                                "[FIGHT_HORDE] Post-Trivia history recovery unavailable: %s",
                                exc,
                            )

                        checked = set()
                        for candidate in candidates:
                            candidate_id = getattr(candidate, "id", None)
                            if candidate_id in checked:
                                continue
                            checked.add(candidate_id)
                            if candidate_id is None:
                                continue
                            if candidate_id != updated_id and candidate_id in seen_ids:
                                continue
                            if getattr(getattr(candidate, "author", None), "id", None) != TARGET_BOT_ID:
                                continue
                            attack_button = _select_attack_button(candidate)
                            if attack_button is None:
                                continue

                            seen_ids.add(candidate_id)
                            logger.info(
                                "[FIGHT_HORDE] Recovered SAME fight after Trivia | message_id=%s | clicking Attack.",
                                candidate_id,
                            )
                            await _TRIVIA_IDLE_EVENT.wait()
                            try:
                                await attack_button.click()
                                return candidate
                            except Exception as exc:
                                logger.error(
                                    "[FIGHT_HORDE] Failed clicking recovered Attack after Trivia | message_id=%s | error=%s",
                                    candidate_id,
                                    exc,
                                )
                                return candidate

                        await asyncio.sleep(0.20)
                    return None

                recovered_fight = await _find_post_trivia_fight()
                if recovered_fight is None:
                    logger.warning(
                        "[FIGHT_HORDE] Trivia was solved, but no continuation of the existing fight was found within the recovery window."
                    )
                    break

                current = recovered_fight
                continue

            current = updated

            if is_new_message:
                # A brand-new prompt (not an edit of what we already acted
                # on) -- most likely the target bot resuming the fight after
                # a non-Trivia interruption. Don't ignore it: click its Attack
                # button now, before returning control to the main pipeline.
                new_btn = _select_attack_button(current)
                if new_btn:
                    logger.info("[FIGHT_HORDE] New fight message after interruption -- clicking Attack.")
                    # Same race-avoidance guard as the dodge re-click above.
                    await _TRIVIA_IDLE_EVENT.wait()
                    try:
                        await new_btn.click()
                    except discord.HTTPException as e:
                        # Same bounded 2-attempt COMPONENT_VALIDATION_FAILED
                        # recovery as the dodge re-click and initial click.
                        recovered = False
                        if getattr(e, "code", None) == 50035:
                            recovered, fresh = await obj._retry_click_after_validation_failure(
                                channel, current.id, _select_attack_button, "FIGHT_HORDE post-interruption click",
                            )
                            if fresh is not None:
                                current = fresh
                        if not recovered:
                            logger.error("[FIGHT_HORDE] Failed clicking Attack on new fight message: %s", e)
                    except Exception as e:
                        logger.error("[FIGHT_HORDE] Failed clicking Attack on new fight message: %s", e)

        if not essence_seen:
            await obj.safe_execute(channel.send, f"!vault deposit {deposit_amount}")

    async def _handle_fight_horde(obj, channel, commands_dict, now) -> bool:
        if not obj.config.get("fight_horde_enabled", False):
            return False

        eff_last = obj.config.get("last_fight_horde_run", 0.0) or (now - obj.config["fight_horde_interval"])
        if now - eff_last < obj.config["fight_horde_interval"]:
            return False

        obj.active_sequence = "fight_horde"
        try:
            return await obj._execute_and_click_first_button(
                channel, commands_dict, "fight_horde", "stats_fight_horde_count", "last_fight_horde_run", now,
                batch_size=obj.config.get("fight_horde_batch_size", 1),
                batch_gap_key="fight_horde_batch_gap",
                prefix_key="cmd_prefix_fight_horde",
                feature_key="fight_horde",
            )
        finally:
            obj.active_sequence = "idle"

    async def _handle_hunt(obj, channel, commands_dict, now) -> bool:
        if not obj.config.get("hunt_enabled", False):
            return False

        eff_last = obj.config.get("last_hunt_run", 0.0) or (now - obj.config["hunt_interval"])
        if now - eff_last < obj.config["hunt_interval"]:
            return False

        obj.active_sequence = "hunt"
        try:
            return await obj._execute_and_click_first_button(
                channel, commands_dict, "hunt", "stats_hunt_count", "last_hunt_run", now,
                batch_size=obj.config.get("hunt_batch_size", 1),
                batch_gap_key="hunt_batch_gap",
                prefix_key="cmd_prefix_hunt",
                feature_key="hunt",
            )
        finally:
            obj.active_sequence = "idle"

    # -----------------------------------------------------------------------
    # Chain & Mod Slots
    # -----------------------------------------------------------------------
    async def _handle_modular_chain(obj, channel, now) -> bool:
        if not obj.config.get("modular_chain_enabled", False):
            return False

        eff_last = obj.config.get("last_modular_chain_run", 0.0) or (now - obj.config["modular_chain_interval"])
        if now - eff_last < obj.config["modular_chain_interval"]:
            return False

        obj.active_sequence = "modular_chain"
        try:
            chain_seq_str = obj.config.get("modular_chain_sequence", "")
            chain_slots = [int(x.strip()) for x in chain_seq_str.split(",") if x.strip().isdigit()]

            ran_any = False
            for slot_id in chain_slots:
                if slot_id not in MODULAR_SLOT_IDS:
                    continue
                # Purposely IGNORING f"slot{slot_id}_enabled" here so the chain can execute standalone chained commands.
                    
                cmd_name = obj.config.get(f"slot{slot_id}_command", "").strip()
                if not cmd_name:
                    continue

                if ran_any:
                    await obj._jitter_sleep(obj.config["modular_chain_step_gap"])
                if not obj._still_active():
                    return True

                kwargs = obj.build_slot_kwargs(slot_id)
                result = await obj.dispatch_modular_command(channel, slot_id, cmd_name, kwargs)

                if result is not None:
                    obj.config[f"stats_slot{slot_id}_count"] += 1
                    obj.config[f"slot{slot_id}_consecutive_failures"] = 0
                else:
                    prefix = obj.config.get(f"cmd_prefix_slot{slot_id}", "/")
                    msg = f"Modular chain (slot {slot_id}): command `{prefix}{cmd_name}` not found or failed."
                    logger.warning(msg)
                    obj.config["last_error"] = msg
                    obj.config[f"stats_slot{slot_id}_failures"] += 1

                    consec_key = f"slot{slot_id}_consecutive_failures"
                    max_key = f"slot{slot_id}_max_consecutive_failures"
                    obj.config[consec_key] = obj.config.get(consec_key, 0) + 1
                    if obj.config[consec_key] >= obj.config.get(max_key, 5):
                        # The chain deliberately ignores slot{n}_enabled (see
                        # comment above), so disabling that flag alone
                        # wouldn't stop it from running here — remove it from
                        # the sequence string too.
                        obj.config[f"slot{slot_id}_enabled"] = False
                        remaining = [s for s in chain_slots if s != slot_id]
                        obj.config["modular_chain_sequence"] = ",".join(str(s) for s in remaining)
                        fail_msg = f"Slot {slot_id} (`{prefix}{cmd_name}`) failed {obj.config[consec_key]}x in a row in the modular chain. Removed from the chain — re-add it to `modular_chain_sequence` once fixed."
                        await obj.notify_channel(f"⚠️ Slot {slot_id} Removed From Chain", fail_msg)
                        await obj.send_alert(f"⚠️ Slot {slot_id} Removed From Chain", fail_msg, severe=True)
                        obj.config[consec_key] = 0
                ran_any = True

            obj.config["last_modular_chain_run"] = now
            if ran_any:
                obj.config["stats_loops_completed"] += 1
                await obj.save_state()
                obj._retime()
                obj._log_progress_if_due()
                return True
            return False
        finally:
            obj.active_sequence = "idle"

    async def _handle_slot(obj, channel, slot_id, now) -> bool:
        enabled_key = f"slot{slot_id}_enabled"
        if not obj.config.get(enabled_key, False):
            return False

        command_name = obj.config.get(f"slot{slot_id}_command", "").strip()
        if not command_name:
            return False

        interval_key = f"slot{slot_id}_interval"
        last_run_key = f"last_slot{slot_id}_run"
        eff_last = obj.config[last_run_key] or (now - obj.config[interval_key])
        if now - eff_last < obj.config[interval_key]:
            return False

        obj.active_sequence = f"slot{slot_id}"
        try:
            kwargs = obj.build_slot_kwargs(slot_id)
            result = await obj.dispatch_modular_command(channel, slot_id, command_name, kwargs)

            if result is not None:
                obj.config[f"stats_slot{slot_id}_count"] += 1
                obj.config[f"slot{slot_id}_consecutive_failures"] = 0
            else:
                prefix = obj.config.get(f"cmd_prefix_slot{slot_id}", "/")
                msg = f"Modular slot {slot_id}: command `{prefix}{command_name}` not found or failed."
                logger.warning(msg)
                obj.config["last_error"] = msg
                obj.config[f"stats_slot{slot_id}_failures"] += 1

                consec_key = f"slot{slot_id}_consecutive_failures"
                max_key = f"slot{slot_id}_max_consecutive_failures"
                obj.config[consec_key] = obj.config.get(consec_key, 0) + 1
                if obj.config[consec_key] >= obj.config.get(max_key, 5):
                    obj.config[enabled_key] = False
                    fail_msg = f"Slot {slot_id} (`{prefix}{command_name}`) failed {obj.config[consec_key]}x in a row. Disabled — `~~toggle slot{slot_id}` to re-enable."
                    await obj.notify_channel(f"⚠️ Slot {slot_id} Auto-Disabled", fail_msg)
                    await obj.send_alert(f"⚠️ Slot {slot_id} Auto-Disabled", fail_msg, severe=True)
                    obj.config[consec_key] = 0

            obj.config[last_run_key] = now
            obj.config["stats_loops_completed"] += 1
            await obj.save_state()
            obj._retime()
            obj._log_progress_if_due()
            return True
        finally:
            obj.active_sequence = "idle"

    # -----------------------------------------------------------------------
    # Main loop
    # -----------------------------------------------------------------------
    @tasks.loop(seconds=DEFAULT_CONFIG["loop_interval"])
    async def farm_loop(obj):
        try:
            if obj._fail_count_pause_active():
                obj.active_sequence = "fail_count_emergency_pause"
                obj.farm_loop.change_interval(seconds=1.0)
                return

            if _TRIVIA_ACTIVE_COUNT > 0:
                obj.active_sequence = "trivia_priority"
                obj.farm_loop.change_interval(seconds=0.25)
                return

            if obj.active_sequence == "trivia_priority":
                obj.active_sequence = "idle"

            if obj.config.get("is_paused", False):
                obj.farm_loop.change_interval(seconds=5.0)
                return

            channel = await obj.resolve_channel(CHANNEL_ID)
            if not channel:
                # Rate-limited warning: log once per ~60s instead of spamming
                # every tick, but make sure it's visible somewhere instead of
                # the old fully-silent `return`.
                last_warn = getattr(obj, "_last_channel_warn", 0)
                if time.time() - last_warn > 60:
                    logger.warning(f"farm_loop: channel {CHANNEL_ID} unresolved this tick — see last_error for details.")
                    obj._last_channel_warn = time.time()
                obj.farm_loop.change_interval(seconds=5.0)
                return

            commands_dict = await obj.get_commands(channel)
            now = time.time()

            if obj.config["last_break_time"] == 0:
                obj.config["last_break_time"] = now
            if obj.config["stats_start_time"] == 0:
                obj.config["stats_start_time"] = now
                await obj.save_state()

            if await obj._handle_sleep(): return
            if await obj._handle_break(now): return
            if await obj._handle_planting(channel, commands_dict, now): return
            if await obj._handle_refine(channel, commands_dict, now): return
            if await obj._handle_fight_horde(channel, commands_dict, now): return
            if await obj._handle_hunt(channel, commands_dict, now): return
            if await obj._handle_modular_chain(channel, now): return

            chain_seq_str = obj.config.get("modular_chain_sequence", "")
            chain_slots = [int(x.strip()) for x in chain_seq_str.split(",") if x.strip().isdigit()] if obj.config.get("modular_chain_enabled", False) else []

            for _slot in MODULAR_SLOT_IDS:
                if _slot in chain_slots:
                    continue
                if await obj._handle_slot(channel, _slot, now):
                    return

            obj.config["stats_loops_completed"] += 1
            obj._retime()
            obj._log_progress_if_due()

        except Exception as error:
            logger.error(f"Loop runtime mismatch: {error}")
            obj.config["last_error"] = f"Crash Tracked: {error}"

            now = time.time()
            obj.config["crash_times"].append(now)
            while obj.config["crash_times"] and now - obj.config["crash_times"][0] > CRASH_WINDOW_SECONDS:
                obj.config["crash_times"].pop(0)

            await obj.save_state()

            if len(obj.config["crash_times"]) > MAX_CRASHES_PER_WINDOW:
                obj.farm_loop.stop()
                msg = f"Script failure rate exceeded safety thresholds ({len(obj.config['crash_times'])} crashes in {CRASH_WINDOW_SECONDS // 60}m). Shutting down."
                await obj.notify_channel("🛑 Structural Safety Lockdown", msg)
                await obj.send_alert("🛑 Structural Safety Lockdown", msg, severe=True)
                return

            obj.farm_loop.change_interval(seconds=5.0)

    @tasks.loop(seconds=DEFAULT_CONFIG["command_refresh_interval"])
    async def command_refresh_loop(obj):
        try:
            channel = await obj.resolve_channel(CHANNEL_ID)
            if channel:
                await obj.get_commands(channel, force_refresh=True)
        except Exception as e:
            logger.error(f"Command refresh error handled gracefully: {e}")

    @tasks.loop(seconds=60)
    async def farm_loop_watchdog(obj):
        """Safety net: if farm_loop's underlying task ever dies silently
        (e.g. an uncaught BaseException such as a leaked
        asyncio.CancelledError escapes the loop body -- which farm_loop's
        own `except Exception` clause does NOT catch, since CancelledError
        is a BaseException, not an Exception, as of Python 3.8), every
        sequence would just stop repeating forever with no visible error.
        This periodically checks whether farm_loop is still running and,
        if it unexpectedly isn't (and the user didn't deliberately ~~stop
        it), restarts it and posts a notice.
        """
        try:
            if obj._user_stopped:
                return

            # RELIABILITY FIX: farm_loop.is_running() only reflects whether
            # the tasks.loop's own asyncio task is alive -- it says nothing
            # about whether the underlying Discord gateway connection is
            # actually up. tasks.loop keeps ticking on its own schedule
            # regardless of websocket state, so a dead/closed gateway used
            # to leave farm_loop looking perfectly "RUNNING" in ~~status
            # while every channel operation silently failed, invisible until
            # the crash-count safety lockdown eventually tripped. Check
            # gateway health explicitly so this is caught immediately.
            if obj.bot.is_closed():
                if not obj._gateway_closed_alerted:
                    obj._gateway_closed_alerted = True
                    msg = (
                        "The Discord gateway connection is closed, but farm_loop's task is still "
                        "alive and will keep reporting RUNNING. This process cannot safely "
                        "reconnect a closed client on its own — please restart the bot (e.g. "
                        "redeploy on Railway or restart the process)."
                    )
                    logger.error("Watchdog: gateway connection is closed. %s", msg)
                    await obj.send_alert("🛑 Gateway Connection Closed", msg, severe=True)
                return
            else:
                obj._gateway_closed_alerted = False

            if obj.farm_loop.is_running():
                return

            logger.error("Watchdog: farm_loop was found stopped without an explicit ~~stop. Restarting.")
            obj.farm_loop.change_interval(seconds=obj.config["loop_interval"])
            obj.farm_loop.start()
            msg = (
                "The farming loop stopped unexpectedly (not via `~~stop`) and has been automatically restarted. "
                "Check `~~status` for `Latest Exceptions` if this keeps happening."
            )
            await obj.notify_channel("🔁 Watchdog Auto-Restart", msg)
            await obj.send_alert("🔁 Watchdog Auto-Restart", msg, severe=True)
        except Exception as e:
            logger.error(f"farm_loop_watchdog error handled gracefully: {e}")

    @tasks.loop(minutes=30)
    async def daily_report_loop(obj):
        """
        Checks every 30 minutes whether daily_report_interval_hours has
        elapsed since the last report and, if so, sends one. If DAILY_ID
        isn't set, _send_daily_report() itself is a no-op for the channel —
        but we still let the interval and baseline advance here so the
        report doesn't build into one giant catch-up message the moment
        DAILY_ID finally gets configured later.
        """
        try:
            interval_seconds = obj.config.get("daily_report_interval_hours", 24) * 3600
            last_sent = obj.config.get("daily_report_last_sent", 0.0)
            if last_sent == 0.0:
                obj.config["daily_report_last_sent"] = time.time()
                await obj.save_state()
                return
            if time.time() - last_sent >= interval_seconds:
                await obj._send_daily_report(reason="scheduled")
        except Exception as e:
            logger.error(f"daily_report_loop error handled gracefully: {e}")

    @tasks.loop(seconds=DEFAULT_CONFIG["hq_gather_interval"])
    async def hq_gather_loop(obj):
        try:
            if not obj.config.get("hq_gather_enabled", False):
                return
            if obj.config.get("is_paused", False):
                return
            if obj.active_sequence != "idle":
                # Wait till core sequences are totally finished before barking gathers to avoid button overrides or overlaps
                return

            target_id = obj.config.get("hq_gather_channel_id") or CHANNEL_ID
            channel = obj.bot.get_channel(target_id)
            if not channel:
                msg = f"hq-gather target channel {target_id} not found/accessible."
                if obj.config.get("last_error") != msg:
                    logger.warning(msg)
                    obj.config["last_error"] = msg
                return

            result = await obj.safe_execute(channel.send, obj.config["hq_gather_message"])
            if result is not None:
                obj.config["stats_hq_gather_count"] += 1
                obj.config["last_hq_gather_time"] = time.time()
                await obj.save_state()
        except Exception as e:
            logger.error(f"hq_gather_loop error handled gracefully: {e}")

    async def _apply_hq_gather_state(obj, enabled: bool):
        obj.config["hq_gather_enabled"] = enabled
        if enabled:
            obj.hq_gather_loop.change_interval(seconds=obj.config["hq_gather_interval"])
            if not obj.hq_gather_loop.is_running():
                obj.hq_gather_loop.start()
        else:
            if obj.hq_gather_loop.is_running():
                obj.hq_gather_loop.stop()

    @tasks.loop(seconds=30)
    async def voice_watch_loop(obj):
        if not obj.config.get("voice_enabled", False):
            return
        channel_id = obj.config.get("voice_channel_id", 0)
        if not channel_id:
            return

        try:
            channel = obj.bot.get_channel(channel_id)
            if not channel:
                msg = f"Voice target channel {channel_id} not found/accessible."
                if obj.config.get("last_error") != msg:
                    logger.warning(msg)
                    obj.config["last_error"] = msg
                return

            guild = channel.guild
            bot_member = guild.me
            if bot_member.voice and bot_member.voice.channel and bot_member.voice.channel.id == channel_id:
                return

            await guild.change_voice_state(channel=channel, self_mute=True, self_deaf=True)
            logger.info(f"Voice watchdog (re)connected to channel {channel_id} via WebSocket.")
        except Exception as e:
            logger.error(f"voice_watch_loop error handled gracefully: {e}")

    async def _apply_voice_state(obj, enabled: bool):
        obj.config["voice_enabled"] = enabled
        if enabled:
            if not obj.voice_watch_loop.is_running():
                obj.voice_watch_loop.start()
        else:
            if obj.voice_watch_loop.is_running():
                obj.voice_watch_loop.stop()

    # -----------------------------------------------------------------------
    # Commands
    # -----------------------------------------------------------------------
    @commands.command(name="set")
    async def set_config(obj, ctx, key: str, *, value: str):
        # Special virtual key: ~~set cmd_prefix_slotall ! / ~~set cmd_prefix_slotall /
        # Applies the given prefix to every modular slot in one shot instead
        # of setting them one at a time.
        if key.lower() == "cmd_prefix_slotall":
            if value not in ("/", "!"):
                await obj.reply(ctx, "❌ Invalid Prefix", "`cmd_prefix_slotall` must be either `/` or `!`.")
                return
            for _slot in MODULAR_SLOT_IDS:
                obj.config[f"cmd_prefix_slot{_slot}"] = value
            await obj.save_state()
            await obj.reply(
                ctx, "⚙️ Parameter Logged",
                f"Set dispatch prefix to **{value}** for all {len(MODULAR_SLOT_IDS)} modular slots."
            )
            return

        if key not in SETTABLE:
            await obj.reply(ctx, "❌ Rejection", f"`{key}` is not a valid parameter. Type `~~settings` to view registry.")
            return

        caster = SETTABLE[key]
        try:
            if caster == bool:
                lowered = value.lower()
                if lowered in ("true", "1", "yes", "on", "enable"):
                    parsed = True
                elif lowered in ("false", "0", "no", "off", "disable"):
                    parsed = False
                else:
                    raise ValueError(f"'{value}' is not a recognized boolean value")
            else:
                parsed = caster(value)
        except ValueError:
            await obj.reply(ctx, "❌ Type Cast Error", f"Cannot convert input to type `{caster.__name__}`.")
            return

        if key in MIN_VALUES and parsed < MIN_VALUES[key]:
            await obj.reply(ctx, "❌ Boundary Error", f"Value is lower than minimum permitted boundary ({MIN_VALUES[key]}).")
            return

        if key.startswith("cmd_prefix_") and parsed not in ("/", "!"):
            await obj.reply(ctx, "❌ Invalid Prefix", f"`{key}` must be either `/` or `!`.")
            return

        old_val = obj.config[key]
        obj.config[key] = parsed
        await obj.save_state()

        if key in ("sleep_enabled", "sleep_start_hour", "sleep_duration_hours") and obj.farm_loop.is_running():
            obj._retime()

        await obj.reply(ctx, "⚙️ Parameter Logged", f"Successfully mutated parameter `{key}`:\n**{old_val}** ➔ **{parsed}**")

    @commands.command()
    async def settings(obj, ctx):
        groups = {
            "⏰ Core Loop & Latency Profile": ["loop_interval", "loop_jitter", "log_summary_interval_loops"],
            "🌾 Agricultural Cultivation Matrix": ["plant_interval", "plant_step_gap", "plant_repeats", "plant_material", "plant_quantity", "cmd_prefix_plant", "cmd_prefix_harvest_plots"],
            "🧬 Refinement Sequence": ["refine_interval", "refine_recipe_id", "refine_max_consecutive_failures", "cmd_prefix_refine"],
            "⚔️ Combat & Hunt Protocols": ["fight_horde_interval", "fight_horde_batch_size", "fight_horde_batch_gap", "fight_horde_max_consecutive_failures", "cmd_prefix_fight_horde", "hunt_interval", "hunt_batch_size", "hunt_batch_gap", "hunt_max_consecutive_failures", "cmd_prefix_hunt"],
            "🧪 Gu Loadout & Heal Sequence": ["gu1_name", "gu2_name", "fight_horde_low_hp_threshold", "fight_horde_heal_use_times", "fight_horde_heal_step_gap", "fight_horde_essence_keyword", "fight_horde_vault_deposit_amount"],
            "⛓️ Modular Chain Configuration": ["modular_chain_interval", "modular_chain_step_gap", "modular_chain_sequence"],
            "📢 HQ Gather Announcements": ["hq_gather_message", "hq_gather_interval", "hq_gather_channel_id"],
            "📅 Daily Report & Alerts": ["daily_report_interval_hours", "alert_on_all_errors"],
            **{
                f"🧩 Modular Slot {_slot}": [
                    f"slot{_slot}_command", f"cmd_prefix_slot{_slot}",
                    f"slot{_slot}_param1_name", f"slot{_slot}_param1_value",
                    f"slot{_slot}_param2_name", f"slot{_slot}_param2_value",
                    f"slot{_slot}_param3_name", f"slot{_slot}_param3_value",
                    f"slot{_slot}_interval", f"slot{_slot}_max_consecutive_failures",
                ]
                for _slot in MODULAR_SLOT_IDS
            },
            "☕ Human Behavior Simulation Bounds": ["break_interval", "break_duration", "sleep_enabled", "sleep_start_hour", "sleep_duration_hours"],
        }

        header = (
            "## ⚙️ SYSTEM CONFIGURATION VARIABLE REGISTRY\n"
            "Update via: `~~set <parameter> <value>`\n"
            "Toggle-only flags (plant/refine/fight_horde/hunt/modular_chain/slots) are managed via `~~toggle <target>`, not `~~set`."
        )
        blocks = [header]
        for name, keys in groups.items():
            block = f"### {name}\n"
            for k in keys:
                curr = obj.config[k]
                df = DEFAULT_CONFIG[k]
                mn = MIN_VALUES.get(k, "N/A")
                desc = DESCRIPTIONS.get(k, "No description available.")
                block += f"* `{k}`\n  ↳ **Current:** `{curr}` | **Default:** `{df}` | **Min Bound:** `{mn}`\n  ↳ *Info:* {desc}\n"
            blocks.append(block)

        for chunk in chunk_blocks(blocks):
            await ctx.send(chunk)

    @commands.command(name="toggle")
    async def toggle(obj, ctx, target: str = None):
        """Toggle plant / refine / fight_horde / hunt / sequences / modular_chain on or off. Usage: ~~toggle <target>"""
        if not target or target.lower() not in TOGGLE_TARGETS:
            valid = ", ".join(f"`{t}`" for t in TOGGLE_TARGETS)
            await obj.reply(ctx, "❌ Rejection", f"Specify a valid target: {valid}.\nUsage: `~~toggle <target>`")
            return

        key, last_run_key, interval_key, emoji, label = TOGGLE_TARGETS[target.lower()]
        new_state = not obj.config[key]
        obj.config[key] = new_state

        note = None
        if new_state and last_run_key and interval_key:
            obj.config[last_run_key] = time.time() - obj.config[interval_key]
            note = "Queued for immediate execution."
            if key == "refine_enabled":
                obj.config["refine_consecutive_failures"] = 0

        await obj.save_state()
        await ctx.message.add_reaction(emoji if new_state else "❌")
        await obj.reply(
            ctx,
            f"{emoji if new_state else '❌'} {label} {'enabled' if new_state else 'disabled'}.",
            note,
        )

    @commands.command(name="gu")
    async def gu_toggle(obj, ctx, target: str = None):
        """Alternates the equipped Gu between gu1 (Blade Appraisal Gu) and
        gu2 (Sword Body Restoration Gu). Usage:
          ~~gu            -> swap to whichever one isn't currently active
          ~~gu gu1        -> switch to gu1 specifically
          ~~gu gu2        -> switch to gu2 specifically
        Names for gu1/gu2 are configurable via `~~set gu1_name <name>` and
        `~~set gu2_name <name>`.
        """
        t = (target or "").strip().lower()
        if t not in ("", "gu1", "gu2"):
            await obj.reply(ctx, "❌ Rejection", "Usage: `~~gu`, `~~gu gu1`, or `~~gu gu2`.")
            return

        current = obj.config.get("active_gu", "gu1")
        if t == "":
            desired = "gu2" if current == "gu1" else "gu1"
        else:
            desired = t

        if desired == current:
            await obj.reply(ctx, "ℹ️ No Change", f"`{desired}` (**{obj.config.get(f'{desired}_name')}**) is already equipped.")
            return

        gu1_name = obj.config.get("gu1_name", "Blade Appraisal Gu")
        gu2_name = obj.config.get("gu2_name", "Sword Body Restoration Gu")
        current_name = gu1_name if current == "gu1" else gu2_name
        desired_name = gu1_name if desired == "gu1" else gu2_name

        channel = await obj.resolve_channel(CHANNEL_ID)
        if not channel:
            await obj.reply(ctx, "❌ Error", "Farm channel not resolved.")
            return

        cmd = obj.resolve_dynamic_command("unequip")
        if not cmd:
            await obj.reply(ctx, "❌ Error", "`/unequip` not found in command cache.")
            return
        await obj.safe_execute(cmd, channel, gu_name=current_name)
        await obj._jitter_sleep(1.5)

        cmd = obj.resolve_dynamic_command("equip")
        if not cmd:
            await obj.reply(ctx, "❌ Error", "`/equip` not found in command cache.")
            return
        await obj.safe_execute(cmd, channel, gu_name=desired_name)

        obj.config["active_gu"] = desired
        await obj.save_state()
        await ctx.message.add_reaction("🔁")
        await obj.reply(ctx, "🔁 Gu Swapped", f"**{current_name}** ({current}) ➔ **{desired_name}** ({desired})")

    @commands.command(name="toggle_prefix")
    async def toggle_prefix(obj, ctx, target: str = None):
        """Flip a command's dispatch prefix between / and !. Usage: ~~toggle_prefix <target>|slotall"""
        t = (target or "").lower()

        if t == "slotall":
            current = obj.config.get("cmd_prefix_slot1", "/")
            new_val = "!" if current == "/" else "/"
            for _slot in MODULAR_SLOT_IDS:
                obj.config[f"cmd_prefix_slot{_slot}"] = new_val
            await obj.save_state()
            await ctx.message.add_reaction("🔁")
            await obj.reply(
                ctx, "🔁 Prefix Toggled",
                f"All {len(MODULAR_SLOT_IDS)} modular slots switched to **{new_val}**."
            )
            return

        if t not in PREFIX_TARGETS:
            valid = ", ".join(f"`{k}`" for k in list(PREFIX_TARGETS) + ["slotall"])
            await obj.reply(ctx, "❌ Rejection", f"Specify a valid target: {valid}.\nUsage: `~~toggle_prefix <target>`")
            return

        key = PREFIX_TARGETS[t]
        old_val = obj.config.get(key, "/")
        new_val = "!" if old_val == "/" else "/"
        obj.config[key] = new_val
        await obj.save_state()
        await ctx.message.add_reaction("🔁")
        await obj.reply(ctx, "🔁 Prefix Toggled", f"`{key}`: **{old_val}** ➔ **{new_val}**")

    @commands.command(name="preview")
    async def preview(obj, ctx, target: str = None):
        """Show what a sequence would send without actually dispatching it."""
        t = (target or "").lower()

        if t in MODULAR_SLOT_NAMES:
            slot_id = int(t[len("slot"):])
            command_name = obj.config.get(f"slot{slot_id}_command", "").strip()
            if not command_name:
                await obj.reply(ctx, f"🔍 Preview: Modular Slot {slot_id}", "Not configured yet. Set a command with `~~set slot{}_command <name>`.".format(slot_id))
                return
            cmd = obj.resolve_dynamic_command(command_name)
            cmd_status = "cached ✅" if cmd else "NOT CACHED ⚠️ (check spelling/subcommands, or run `~~start`)"
            kwargs = obj.build_slot_kwargs(slot_id)

            lines = [f"**🔍 Preview: Modular Slot {slot_id} → `/{command_name}`**", f"Command resolved: {cmd_status}"]
            if kwargs:
                for k, v in kwargs.items():
                    lines.append(f"`{k}` = `{v}`")
            else:
                lines.append("(no parameters configured)")
            await ctx.send("\n".join(lines))
            return

        if not target or t not in PREVIEWABLE_STEPS:
            valid = ", ".join(f"`{s}`" for s in list(PREVIEWABLE_STEPS) + list(MODULAR_SLOT_NAMES))
            await obj.reply(ctx, "❌ Rejection", f"Specify a valid target: {valid}.\nUsage: `~~preview <target>`")
            return

        step = t
        cmd = obj.cached_commands.get(step)
        cmd_status = "cached ✅" if cmd else "NOT CACHED ⚠️ (run `~~start` or wait for the command refresh)"
        kwargs = obj.STEP_SPECS[step]()

        lines = [f"**🔍 Preview: `/{step}`**", f"Command resolved: {cmd_status}"]
        if kwargs:
            for k, v in kwargs.items():
                lines.append(f"`{k}` = `{v}`")
        else:
            lines.append("(no parameters)")
        await ctx.send("\n".join(lines))

    @commands.command(name="profile")
    async def profile(obj, ctx, action: str = None, name: str = None):
        """Save/load/list/delete a named configuration snapshot."""
        action = (action or "").lower()
        if action not in ("save", "load", "list", "delete") or (action != "list" and not name):
            await obj.reply(ctx, "❌ Rejection", "Usage: `~~profile save <name>` | `~~profile load <name>` | `~~profile list` | `~~profile delete <name>`")
            return

        profiles = await asyncio.to_thread(obj._load_profiles_sync)

        if action == "list":
            if not profiles:
                await obj.reply(ctx, "📁 Profiles", "No saved profiles yet.")
            else:
                await obj.reply(ctx, "📁 Saved Profiles", "\n".join(f"`{n}`" for n in profiles))
            return

        if action == "save":
            snapshot = {k: obj.config[k] for k in SETTABLE}
            snapshot.update({t[0]: obj.config[t[0]] for t in TOGGLE_TARGETS.values()})
            snapshot["hq_gather_enabled"] = obj.config["hq_gather_enabled"]
            profiles[name] = snapshot
            await asyncio.to_thread(obj._save_profiles_sync, profiles)
            await obj.reply(ctx, "💾 Profile Saved", f"Current configuration saved as `{name}`.")
            return

        if action == "delete":
            if name not in profiles:
                await obj.reply(ctx, "❌ Not Found", f"No profile named `{name}`.")
                return
            del profiles[name]
            await asyncio.to_thread(obj._save_profiles_sync, profiles)
            await obj.reply(ctx, "🗑️ Profile Deleted", f"Removed `{name}`.")
            return

        if name not in profiles:
            await obj.reply(ctx, "❌ Not Found", f"No profile named `{name}`.")
            return
        toggle_keys = {t[0] for t in TOGGLE_TARGETS.values()}
        skipped = []
        hq_gather_state = None
        for k, v in profiles[name].items():
            if k == "hq_gather_enabled" and isinstance(v, bool):
                hq_gather_state = v
            elif k in SETTABLE:
                if is_valid_settable(k, v):
                    obj.config[k] = v
                else:
                    skipped.append(k)
            elif k in toggle_keys and isinstance(v, bool):
                obj.config[k] = v
        if hq_gather_state is not None:
            await obj._apply_hq_gather_state(hq_gather_state)
        await obj.save_state()
        note = f"Configuration `{name}` applied."
        if skipped:
            note += f"\n⚠️ Skipped invalid values for: {', '.join(skipped)}."
        await obj.reply(ctx, "📂 Profile Loaded", note)

    @commands.command(name="hq-gather")
    async def hq_gather_cmd(obj, ctx, channel: discord.TextChannel = None):
        """Toggle a repeating plain-text message on/off."""
        if channel is not None:
            obj.config["hq_gather_channel_id"] = channel.id

        new_state = not obj.hq_gather_loop.is_running()
        await obj._apply_hq_gather_state(new_state)
        await obj.save_state()

        target_id = obj.config.get("hq_gather_channel_id") or CHANNEL_ID
        target_channel = obj.bot.get_channel(target_id)
        target_desc = target_channel.mention if target_channel else f"channel `{target_id}` (not found yet)"

        if new_state:
            await ctx.message.add_reaction("📢")
            await obj.reply(
                ctx,
                "📢 HQ Gather Enabled",
                f"Sending `{obj.config['hq_gather_message']}` every {obj.config['hq_gather_interval']}s to {target_desc}.\n"
                f"Auto-paused while sequences are running to prevent interruption issues.",
            )
        else:
            await ctx.message.add_reaction("❌")
            await obj.reply(ctx, "❌ HQ Gather Disabled", "Repeating message stopped.")

    @commands.command(name="jvc")
    async def jvc(obj, ctx, channel_id: int = None):
        """Join and stay connected to a voice channel by ID until ~~lvc is used."""
        if channel_id is None:
            await obj.reply(ctx, "❌ Rejection", "Usage: `~~jvc <voice_channel_id>`")
            return

        channel = obj.bot.get_channel(channel_id)
        if not channel:
            await obj.reply(ctx, "❌ Not Found", f"Channel `{channel_id}` not found or not accessible from this account's cache.")
            return
        if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
            await obj.reply(ctx, "❌ Invalid Channel", f"`{channel_id}` is a `{type(channel).__name__}`, not a voice or stage channel.")
            return

        obj.config["voice_channel_id"] = channel_id
        await obj._apply_voice_state(True)
        await obj.save_state()

        try:
            await channel.guild.change_voice_state(channel=channel, self_mute=True, self_deaf=True)
            await ctx.message.add_reaction("🔊")
            await obj.reply(
                ctx,
                "🔊 Voice Joined",
                f"Connected to `{channel.name}` (`{channel_id}`) via WebSocket protocol. Staying connected until `~~lvc`; "
                f"a watchdog will auto-reconnect on drops.",
            )
        except Exception as e:
            await obj.reply(ctx, "❌ Connection Failed", str(e))

    @commands.command(name="lvc")
    async def lvc(obj, ctx):
        """Leave the current voice channel and stop the auto-reconnect watchdog."""
        await obj._apply_voice_state(False)
        channel_id = obj.config.get("voice_channel_id", 0)
        obj.config["voice_channel_id"] = 0
        await obj.save_state()

        disconnected = False
        target_channel = obj.bot.get_channel(channel_id) if channel_id else None
        guild = target_channel.guild if target_channel else ctx.guild
        if guild:
            try:
                await guild.change_voice_state(channel=None)
                disconnected = True
            except Exception as e:
                logger.error(f"Error disconnecting voice state: {e}")

        await ctx.message.add_reaction("👋")
        await obj.reply(
            ctx,
            "👋 Voice Left",
            "Disconnected and watchdog stopped." if disconnected else "Watchdog stopped (no active connection found).",
        )

    @commands.command()
    async def reset_stats(obj, ctx):
        obj.config["stats_start_time"] = time.time()
        obj.config["stats_plant_count"] = 0
        obj.config["stats_harvest_count"] = 0
        obj.config["stats_refine_count"] = 0
        obj.config["stats_refine_failures"] = 0
        obj.config["stats_fight_horde_count"] = 0
        obj.config["stats_hunt_count"] = 0
        obj.config["stats_hq_gather_count"] = 0
        for _slot in MODULAR_SLOT_IDS:
            obj.config[f"stats_slot{_slot}_count"] = 0
            obj.config[f"stats_slot{_slot}_failures"] = 0
        obj.config["stats_loops_completed"] = 0
        obj.config["stats_total_break_time"] = 0
        obj.config["first_plant_done"] = False
        await obj.save_state()
        await ctx.message.add_reaction("🧹")
        await obj.reply(ctx, "🧹 Session Performance Metrics Cleared")

    @commands.command()
    async def start(obj, ctx):
        if obj.farm_loop.is_running():
            if obj.config["is_paused"]:
                obj.config["is_paused"] = False
                await obj.save_state()
                await ctx.message.add_reaction("▶️")
                await obj.reply(ctx, "▶️ Resumed", "Automation unpaused; loop operations restored.")
            else:
                await obj.reply(ctx, "⚠️ Status Alert", "Automation cores already online.")
            return

        now = time.time()
        obj.config["last_plant_run"] = now - obj.config["plant_interval"]
        obj.config["last_refine_run"] = now - obj.config["refine_interval"]
        obj.config["last_fight_horde_run"] = now - obj.config["fight_horde_interval"]
        obj.config["last_hunt_run"] = now - obj.config["hunt_interval"]
        obj.config["last_modular_chain_run"] = now - obj.config.get("modular_chain_interval", 300)
        obj.config["first_plant_done"] = False
        obj.config["is_paused"] = False

        if obj.config["last_break_time"] == 0:
            obj.config["last_break_time"] = now
        if obj.config["stats_start_time"] == 0:
            obj.config["stats_start_time"] = now
        await obj.save_state()

        obj.farm_loop.change_interval(seconds=obj.config["loop_interval"])
        obj.farm_loop.start()
        obj._user_stopped = False
        if not obj.command_refresh_loop.is_running():
            obj.command_refresh_loop.change_interval(seconds=obj.config["command_refresh_interval"])
            obj.command_refresh_loop.start()
        if not obj.farm_loop_watchdog.is_running():
            obj.farm_loop_watchdog.start()
        if not obj.daily_report_loop.is_running():
            obj.daily_report_loop.start()

        missing = []
        channel = await obj.resolve_channel(CHANNEL_ID)
        if channel:
            commands_dict = await obj.get_commands(channel, force_refresh=True)
            if obj.config["plant_enabled"] and obj.config.get("cmd_prefix_plant", "/") == "/" and (not commands_dict.get("plant") or not commands_dict.get("harvest_plots")):
                missing.append("plant (plant/harvest_plots)")
            if obj.config["refine_enabled"] and obj.config.get("cmd_prefix_refine", "/") == "/" and not commands_dict.get("refine"):
                missing.append("refine")
            if obj.config["fight_horde_enabled"] and obj.config.get("cmd_prefix_fight_horde", "/") == "/" and not commands_dict.get("fight_horde"):
                missing.append("fight_horde")
            if obj.config["hunt_enabled"] and obj.config.get("cmd_prefix_hunt", "/") == "/" and not commands_dict.get("hunt"):
                missing.append("hunt")
            
            # Use dynamic resolution to check slots so we properly parse nested subcommands
            for _slot in MODULAR_SLOT_IDS:
                if not obj.config.get(f"slot{_slot}_enabled"):
                    continue
                _name = obj.config.get(f"slot{_slot}_command", "").strip()
                if not _name:
                    missing.append(f"slot{_slot} (no command name configured)")
                elif obj.config.get(f"cmd_prefix_slot{_slot}", "/") == "/" and not obj.resolve_dynamic_command(_name):
                    missing.append(f"slot{_slot} (/{_name})")

        msg = "Farming routines armed. Sequences primed to execute immediately."
        if not channel:
            msg = (
                f"⚠️ Loop started, but channel `{CHANNEL_ID}` could not be resolved "
                f"({obj.config.get('last_error', 'unknown reason')}). Nothing will be dispatched "
                f"until this is fixed — check CHANNEL_ID and that this account can see that channel."
            )
        elif missing:
            msg += f"\n⚠️ Missing commands for: {', '.join(missing)}. Those sequences will silently no-op until resolved."

        await ctx.message.add_reaction("✅" if (channel and not missing) else "⚠️")
        await obj.reply(ctx, "✅ System Initialized" if channel else "⚠️ Channel Unresolved", msg)

    @commands.command()
    async def stop(obj, ctx):
        if not obj.farm_loop.is_running():
            return
        obj.farm_loop.stop()
        if obj.config["is_paused"] and obj.config.get("pause_started_at"):
            obj.config["session_paused_seconds"] = obj.config.get("session_paused_seconds", 0.0) + (time.time() - obj.config["pause_started_at"])
            obj.config["pause_started_at"] = 0.0
        obj.config["is_paused"] = False
        obj._user_stopped = True
        await obj.save_state()
        await obj._send_daily_report(reason="~~stop")
        await ctx.message.add_reaction("🛑")
        await obj.reply(ctx, "🛑 Automation Suspended", "Loop system shutdown fully. Use `~~start` to reinitialize.")

    @commands.command(name="ping")
    async def ping(obj, ctx):
        start = time.time()
        msg = await ctx.send("🏓 Pinging...")
        latency_ms = (time.time() - start) * 1000
        ws_latency_ms = obj.bot.latency * 1000
        await msg.edit(
            content=(
                f"🏓 **Pong!**\n"
                f"Message round-trip: `{latency_ms:.0f}ms`\n"
                f"Gateway latency: `{ws_latency_ms:.0f}ms`"
            )
        )

    @commands.command(name="panel")
    async def panel(obj, ctx):
        """Posts the URL of the CIPHER web dashboard, if it's running."""
        port = os.getenv("PORT")
        domain = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("RAILWAY_STATIC_URL")

        if domain:
            link = domain if domain.startswith("http") else f"https://{domain}"
            source_note = "🌐 Hosted dashboard (Railway)"
        elif port:
            link = f"http://localhost:{port}"
            source_note = "🖥️ Local dashboard (only reachable from this machine, not a public link)"
        else:
            await obj.reply(
                ctx,
                "⚠️ No Web Dashboard Running",
                "This session is using the local console UI, not the web dashboard. "
                "The web dashboard only starts when a `PORT` env var is set (e.g. on Railway).",
            )
            return

        auth_note = (
            "🔒 Protected — you'll be asked for the DASHBOARD_USER/DASHBOARD_PASS you set."
            if globals().get("DASHBOARD_PASS")
            else "⚠️ No password set on this deploy — anyone with this link can open and control the panel."
        )

        await ctx.message.add_reaction("🔗")
        await obj.reply(ctx, "🔗 CIPHER Control Panel", f"{link}\n{source_note}\n{auth_note}")

    @commands.command()
    async def pause(obj, ctx):
        if not obj.farm_loop.is_running():
            await obj.reply(ctx, "❌ Action Denied", "System loop is not currently active. Run `~~start` first.")
            return
        if obj.config["is_paused"]:
            await obj.reply(ctx, "⚠️ Notice", "Automation system is already paused.")
            return
        obj.config["is_paused"] = True
        obj.config["pause_started_at"] = time.time()
        await obj.save_state()
        await ctx.message.add_reaction("⏸️")
        await obj.reply(ctx, "⏸️ Automation Paused", "Operations frozen in place. Use `~~resume` to unfreeze.")

    @commands.command()
    async def resume(obj, ctx):
        if not obj.farm_loop.is_running():
            await obj.reply(ctx, "❌ Action Denied", "System loop thread is dead. Use `~~start` to boot up.")
            return
        if not obj.config["is_paused"]:
            await obj.reply(ctx, "⚠️ Notice", "System is already running and processing actively.")
            return
        obj.config["is_paused"] = False
        if obj.config.get("pause_started_at"):
            obj.config["session_paused_seconds"] = obj.config.get("session_paused_seconds", 0.0) + (time.time() - obj.config["pause_started_at"])
            obj.config["pause_started_at"] = 0.0
        await obj.save_state()
        obj.farm_loop.change_interval(seconds=obj.config["loop_interval"])
        await ctx.message.add_reaction("▶️")
        await obj.reply(ctx, "▶️ Operations Resumed", "Farming threads active again.")

    @commands.command()
    async def status(obj, ctx):
        loop_status = "PAUSED" if obj.config.get("is_paused", False) else ("RUNNING" if obj.farm_loop.is_running() else "STOPPED")
        plant_status = "ENABLED" if obj.config["plant_enabled"] else "DISABLED"
        refine_status = "ENABLED" if obj.config["refine_enabled"] else "DISABLED"
        fight_horde_status = "ENABLED" if obj.config["fight_horde_enabled"] else "DISABLED"
        hunt_status = "ENABLED" if obj.config["hunt_enabled"] else "DISABLED"
        chain_status = "ENABLED" if obj.config.get("modular_chain_enabled") else "DISABLED"
        sleep_status = "ACTIVE" if obj.config["sleep_enabled"] else "DISABLED"
        hq_gather_status = "RUNNING" if obj.hq_gather_loop.is_running() else "STOPPED"

        voice_channel_id = obj.config.get("voice_channel_id", 0)
        voice_channel_obj = obj.bot.get_channel(voice_channel_id) if voice_channel_id else None
        if voice_channel_obj:
            guild_me = voice_channel_obj.guild.me if voice_channel_obj.guild else None
            if guild_me and guild_me.voice and guild_me.voice.channel and guild_me.voice.channel.id == voice_channel_id:
                voice_status = f"CONNECTED ({voice_channel_obj.name})"
            elif obj.voice_watch_loop.is_running():
                voice_status = "RECONNECTING (watchdog active)"
            else:
                voice_status = "DISCONNECTED"
        else:
            voice_status = "DISCONNECTED"

        now = time.time()

        target_channel_obj = obj.bot.get_channel(CHANNEL_ID) if CHANNEL_ID else None
        if target_channel_obj:
            _guild_name = target_channel_obj.guild.name if getattr(target_channel_obj, "guild", None) else "DM"
            target_channel_display = f"#{target_channel_obj.name} ({CHANNEL_ID}) in {_guild_name} ✅"
        elif not CHANNEL_ID:
            target_channel_display = "NOT SET (CHANNEL_ID=0) ⚠️"
        else:
            target_channel_display = f"UNRESOLVED (id={CHANNEL_ID}) ⚠️ — {obj.config.get('last_error', 'not yet cached; will retry via fetch_channel')}"

        plant_countdown = format_countdown(obj.config["last_plant_run"] + obj.config["plant_interval"], now) if obj.config["last_plant_run"] else "DUE / IMMEDIATE"
        refine_countdown = format_countdown(obj.config["last_refine_run"] + obj.config["refine_interval"], now) if obj.config["last_refine_run"] else "DUE / IMMEDIATE"
        hunt_countdown = format_countdown(obj.config["last_hunt_run"] + obj.config["hunt_interval"], now) if obj.config["last_hunt_run"] else "DUE / IMMEDIATE"
        break_countdown = format_countdown(obj.config["last_break_time"] + obj.config["break_interval"], now) if obj.config["last_break_time"] else "PENDING"
        uptime_str = str(timedelta(seconds=int(now - obj.config["stats_start_time"]))) if obj.config["stats_start_time"] else "0:00:00"
        current_seq = obj.config.get('modular_chain_sequence', '1,2,3')

        dashboard = (
            f"### 📊 FARMING AUTOMATION DASHBOARD (GMT+1)\n"
            f"```ini\n"
            f"[System Status]\n"
            f"Loop Status      = {loop_status}\n"
            f"Target Channel   = {target_channel_display}\n"
            f"Watchdog         = {'ACTIVE' if obj.farm_loop_watchdog.is_running() else 'INACTIVE'}\n"
            f"Latency Profiler = Gaussian Normal Curve (±{obj.config['loop_jitter']}s Jitter)\n"
            f"Plant Sequence   = {plant_status}\n"
            f"Refine Sequence  = {refine_status} (no jitter, fail streak={obj.config['refine_consecutive_failures']})\n"
            f"Fight Horde      = {fight_horde_status}\n"
            f"Hunt Sequence    = {hunt_status}\n"
            f"Modular Chain    = {chain_status} (interval={obj.config['modular_chain_interval']}s, sequence=[{current_seq}])\n"
            f"HQ Gather        = {hq_gather_status} (blocked during running sequences, currently={obj.active_sequence})\n"
            f"Voice Channel    = {voice_status}\n"
            f"Nocturnal Sleep  = {sleep_status}\n"
            f"Next Plant Run   = {plant_countdown}\n"
            f"Next Refine Run  = {refine_countdown}\n"
            f"Next Hunt Run    = {hunt_countdown}\n"
            f"Next Human Break = {break_countdown}\n\n"
            f"[Farming Configurations]\n"
            f"Base Cycle Time  = {obj.config['loop_interval']}s\n"
            f"Plant Targeting  = {obj.config['plant_material']} (x{obj.config['plant_quantity']})\n"
            f"Refine Targeting = {obj.config['refine_recipe_id']} (every {obj.config['refine_interval']}s)\n\n"
            f"[Modular Slots] (per-slot dispatch mode)\n"
            + "".join(
                f"Slot {_slot}            = "
                f"{'ENABLED' if obj.config.get(f'slot{_slot}_enabled') else 'DISABLED'}"
                f" → {obj.config.get(f'cmd_prefix_slot{_slot}', '/')}{obj.config.get(f'slot{_slot}_command') or '(unset)'}"
                f" (ok={obj.config.get(f'stats_slot{_slot}_count', 0):,}, fail={obj.config.get(f'stats_slot{_slot}_failures', 0):,})\n"
                for _slot in MODULAR_SLOT_IDS
            )
            + "\n"
            f"[Session Metrics Log]\n"
            f"Active Uptime    = {uptime_str}\n"
            f"Loops Completed  = {obj.config['stats_loops_completed']:,}\n"
            f"Dispatched {obj.config.get('cmd_prefix_harvest_plots', '/')}harvest={obj.config['stats_harvest_count']:,}\n"
            f"Dispatched {obj.config.get('cmd_prefix_plant', '/')}plant= {obj.config['stats_plant_count']:,}\n"
            f"Dispatched {obj.config.get('cmd_prefix_refine', '/')}refine={obj.config['stats_refine_count']:,} (failures={obj.config['stats_refine_failures']:,})\n"
            f"Dispatched {obj.config.get('cmd_prefix_fight_horde', '/')}fight= {obj.config['stats_fight_horde_count']:,}\n"
            f"Dispatched {obj.config.get('cmd_prefix_hunt', '/')}hunt = {obj.config['stats_hunt_count']:,}\n"
            f"HQ Gather Sent   = {obj.config['stats_hq_gather_count']:,}\n"
            f"Break Downtime   = {str(timedelta(seconds=obj.config['stats_total_break_time']))}\n\n"
            f"[Incident Reports]\n"
            f"Recent Crashes   = {len(obj.config['crash_times'])} in last {CRASH_WINDOW_SECONDS // 60}m\n"
            f"Latest Exceptions= {obj.config['last_error'] or 'NONE'}\n\n"
            f"[Reporting & Alerts]\n"
            f"Daily Report     = {'channel ' + str(DAILY_ID) if DAILY_ID else 'NOT SET (skipped)'} (every {obj.config.get('daily_report_interval_hours', 24)}h, or on ~~stop)\n"
            f"Alert Channel    = {'channel ' + str(ALERT_ID) if ALERT_ID else 'NOT SET (skipped)'} (mode={'all errors' if obj.config.get('alert_on_all_errors') else 'severe only'})\n"
            f"Session Errors   = {obj.config.get('session_errors_count', 0):,}\n"
            f"Session Paused   = {str(timedelta(seconds=int(obj.config.get('session_paused_seconds', 0.0))))}\n"
            f"```"
        )
        await ctx.send(dashboard)

    @commands.command(name="vault")
    async def vault_amount(obj, ctx, amount: str = None):
        """Shows or changes the amount deposited when a fight_horde result
        doesn't drop the essence keyword. Usage:
          ~~vault          -> show current amount
          ~~vault 3000     -> set the amount to 3000
        """
        if not amount:
            await obj.reply(
                ctx, "🏦 Vault Deposit Amount",
                f"Currently **{obj.config.get('fight_horde_vault_deposit_amount', 2000):,}**.\n"
                f"Change it with `~~vault <amount>`."
            )
            return

        try:
            parsed = int(amount)
        except ValueError:
            await obj.reply(ctx, "❌ Type Cast Error", "`amount` must be a whole number, e.g. `~~vault 3000`.")
            return

        if parsed < 0:
            await obj.reply(ctx, "❌ Boundary Error", "`amount` cannot be negative.")
            return

        old_val = obj.config.get("fight_horde_vault_deposit_amount", 2000)
        obj.config["fight_horde_vault_deposit_amount"] = parsed
        await obj.save_state()
        await ctx.message.add_reaction("🏦")
        await obj.reply(ctx, "🏦 Vault Deposit Amount Updated", f"**{old_val:,}** ➔ **{parsed:,}**")

    @commands.command(name="activegu")
    async def active_gu(obj, ctx, use_times: str = None):
        """Shows which Gu is equipped, or changes how many times Gu2 is used.

        Usage:
          ~~activegu       -> show current Gu/status
          ~~activegu 3     -> use Gu2 three times per low-HP trigger
        """
        if use_times is not None:
            try:
                parsed = int(use_times)
            except ValueError:
                await obj.reply(ctx, "❌ Type Cast Error", "`use_times` must be a whole number, e.g. `~~activegu 3`.")
                return
            if parsed < 1:
                await obj.reply(ctx, "❌ Boundary Error", "Gu2 use count must be at least 1.")
                return

            old_val = int(obj.config.get("fight_horde_heal_use_times", 2))
            obj.config["fight_horde_heal_use_times"] = parsed
            await obj.save_state()
            await ctx.message.add_reaction("🧪")
            await obj.reply(
                ctx,
                "🧪 Gu2 Usage Updated",
                f"Gu2 uses per low-HP trigger: **{old_val}** ➔ **{parsed}**.\n"
                f"The heal sequence will call `/use_gu` on Gu2 **{parsed}x**."
            )
            return

        gu1_name = obj.config.get("gu1_name", "Blade Appraisal Gu")
        gu2_name = obj.config.get("gu2_name", "Sword Body Restoration Gu")
        active = obj.config.get("active_gu", "gu1")
        use_count = int(obj.config.get("fight_horde_heal_use_times", 2))

        gu1_state = "🟢 ACTIVE" if active == "gu1" else "⚪ inactive"
        gu2_state = "🟢 ACTIVE" if active == "gu2" else "⚪ inactive"
        active_name = gu1_name if active == "gu1" else gu2_name

        dashboard = (
            f"### 🧪 GU LOADOUT STATUS\n"
            f"```ini\n"
            f"[Currently Equipped]\n"
            f"Active Gu        = {active} → {active_name}\n\n"
            f"[Gu Slots]\n"
            f"gu1              = {gu1_name}\n"
            f"                   {gu1_state}\n"
            f"gu2              = {gu2_name}\n"
            f"                   {gu2_state}\n\n"
            f"[Heal Sequence Config]\n"
            f"Low HP Trigger   = 50% of Max HP\n"
            f"Gu2 Use Count    = {use_count}\n"
            f"Step Gap         = {obj.config.get('fight_horde_heal_step_gap', 1.5)}s\n"
            f"Essence Amount   = +{obj.config.get('fight_horde_essence_amount', 50)}\n"
            f"Essence Keyword  = {obj.config.get('fight_horde_essence_keyword', 'Primeval Essence')}\n"
            f"Essence Mode     = {obj.config.get('fight_horde_essence_mode', 'Primeval Essence')}\n"
            f"Vault Deposit    = {obj.config.get('fight_horde_vault_deposit_amount', 2000)}\n\n"
            f"[Commands]\n"
            f"~~activegu        → show status\n"
            f"~~activegu 3      → use Gu2 3x per heal trigger\n"
            f"~~essence 100     → detect +100 of the current essence\n"
            f"~~toggle_essence  → switch Primeval/Immortal Essence\n"
            f"```"
        )
        await ctx.send(dashboard)

    @commands.command(name="essence")
    async def essence_amount(obj, ctx, amount: str = None):
        """Shows or changes the expected essence loot amount.

        Usage: ~~essence -> show current amount; ~~essence 100 -> detect +100 <essence>.
        """
        if not amount:
            current = int(obj.config.get("fight_horde_essence_amount", 50))
            keyword = obj.config.get("fight_horde_essence_keyword", "Primeval Essence")
            await obj.reply(
                ctx,
                "💠 Essence Detector",
                f"Current detector: **+{current} {keyword}**.\n"
                f"Change it with `~~essence <amount>`, e.g. `~~essence 100`."
            )
            return

        try:
            parsed = int(amount)
        except ValueError:
            await obj.reply(ctx, "❌ Type Cast Error", "`amount` must be a whole number, e.g. `~~essence 100`.")
            return
        if parsed < 1:
            await obj.reply(ctx, "❌ Boundary Error", "Essence amount must be at least 1.")
            return

        old_val = int(obj.config.get("fight_horde_essence_amount", 50))
        obj.config["fight_horde_essence_amount"] = parsed
        await obj.save_state()
        await ctx.message.add_reaction("💠")
        keyword = obj.config.get("fight_horde_essence_keyword", "Primeval Essence")
        await obj.reply(ctx, "💠 Essence Detector Updated", f"**+{old_val} {keyword}** ➔ **+{parsed} {keyword}**")

    @commands.command(name="toggle_essence")
    async def toggle_essence(obj, ctx):
        """Switches the loot detector between Primeval Essence and Immortal Essence."""
        current = obj.config.get("fight_horde_essence_keyword", "Primeval Essence")
        if current.lower() == "primeval essence":
            new_keyword = "Immortal Essence"
        else:
            new_keyword = "Primeval Essence"

        obj.config["fight_horde_essence_keyword"] = new_keyword
        obj.config["fight_horde_essence_mode"] = new_keyword
        await obj.save_state()
        await ctx.message.add_reaction("🔄")
        amount = int(obj.config.get("fight_horde_essence_amount", 50))
        await obj.reply(
            ctx,
            "🔄 Essence Detector Toggled",
            f"Detector is now **+{amount} {new_keyword}**.\n"
            f"The amount stays at **{amount}** until changed with `~~essence <amount>`."
        )

    # -----------------------------------------------------------------------
    # ~~logs: paginated log viewer
    # -----------------------------------------------------------------------
    LOGS_PAGE_CHAR_LIMIT = 1800
    LOGS_MAX_LINES = 1000
    LOGS_NAV_TIMEOUT = 180
    LOGS_ARROW_LEFT = "⬅️"
    LOGS_ARROW_RIGHT = "➡️"

    def _build_log_pages(obj):
        try:
            with open(LOG_FILE_PATH, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except FileNotFoundError:
            return ["(no log file found yet)"]

        lines = content.strip("\n").split("\n") if content.strip("\n") else []
        lines = lines[-obj.LOGS_MAX_LINES:]

        pages = []
        current_lines, current_len = [], 0
        for line in lines:
            line_len = len(line) + 1
            if current_lines and current_len + line_len > obj.LOGS_PAGE_CHAR_LIMIT:
                pages.append("\n".join(current_lines))
                current_lines, current_len = [line], line_len
            else:
                current_lines.append(line)
                current_len += line_len
        if current_lines:
            pages.append("\n".join(current_lines))

        return pages if pages else ["(log file is empty)"]

    def _format_log_page(obj, pages, index):
        return f"### 📜 LOGS (page {index + 1}/{len(pages)})\n```\n{pages[index]}\n```"

    @commands.command(name="logs")
    async def logs(obj, ctx):
        pages = obj._build_log_pages()
        index = len(pages) - 1  # open on the most recent page

        msg = await ctx.send(obj._format_log_page(pages, index))

        if len(pages) <= 1:
            # Nothing to paginate — no arrows added at all. This is the
            # closest self-bot equivalent to a "greyed out / disabled"
            # button: Discord's API only allows real (disableable) message
            # components on messages sent by an application/bot, not on
            # messages sent by a normal user account, so a self-bot can't
            # attach an actual disabled button here. Omitting the reaction
            # entirely is the closest available stand-in.
            return

        reaction_state = {"left": False, "right": False}

        async def sync_reactions():
            # Keep BOTH arrows visible on every page, including page N/N.
            # Navigation wraps around, so neither arrow becomes a dead end.
            if not reaction_state["left"]:
                await obj.safe_execute(msg.add_reaction, obj.LOGS_ARROW_LEFT)
                reaction_state["left"] = True
            if not reaction_state["right"]:
                await obj.safe_execute(msg.add_reaction, obj.LOGS_ARROW_RIGHT)
                reaction_state["right"] = True

        await sync_reactions()

        def check(payload):
            return (
                payload.message_id == msg.id
                and payload.user_id == obj.bot.user.id
                and str(payload.emoji) in (obj.LOGS_ARROW_LEFT, obj.LOGS_ARROW_RIGHT)
            )

        while True:
            try:
                # NOTE: the bot adds the arrow reactions to its own message
                # using the same account that's reading it (self-bot), so
                # when the account owner clicks an arrow in their client it
                # TOGGLES that reaction OFF rather than adding a new one
                # (a single account can't react twice with the same emoji).
                # That's why we listen for a removal here, not an addition.
                payload = await obj.bot.wait_for("raw_reaction_remove", check=check, timeout=obj.LOGS_NAV_TIMEOUT)
            except asyncio.TimeoutError:
                try:
                    await msg.clear_reactions()
                except Exception:
                    pass
                return

            emoji = str(payload.emoji)
            if emoji == obj.LOGS_ARROW_LEFT:
                index = (index - 1) % len(pages)  # left: previous page, wraps to the last
                reaction_state["left"] = False  # the user's removal just took it off — clear our tracked state so sync_reactions re-adds it
            elif emoji == obj.LOGS_ARROW_RIGHT:
                index = (index + 1) % len(pages)  # right: next page, wraps to the first
                reaction_state["right"] = False  # same as above, for the right arrow

            await obj.safe_execute(msg.edit, content=obj._format_log_page(pages, index))
            await sync_reactions()

    # -----------------------------------------------------------------------
    # ~~commands: paginated command reference
    # -----------------------------------------------------------------------
    COMMANDS_PER_PAGE = 10
    COMMANDS_ARROW_LEFT = "⬅️"
    COMMANDS_ARROW_RIGHT = "➡️"
    COMMANDS_REGISTRY = [
        ("set", "⚙️", "Set a config value: `~~set <key> <value>`"),
        ("settings", "📋", "View all settings, grouped by category"),
        ("toggle", "🔀", "Toggle plant/refine/fight_horde/hunt/modular_chain on-off"),
        ("toggle_prefix", "🔁", "Flip a command's dispatch prefix between `/` and `!`"),
        ("gu", "🧪", "Alternate equipped Gu between gu1 (Blade Appraisal Gu) and gu2 (Sword Body Restoration Gu)"),
        ("activegu", "🧪", "Show Gu status; `~~activegu <N>` changes Gu2 uses per heal trigger"),
        ("essence", "💠", "Show/change detected essence amount, e.g. `~~essence 100`"),
        ("toggle_essence", "🔄", "Toggle essence detector between Primeval Essence and Immortal Essence"),
        ("vault", "🏦", "Show or change the vault deposit amount used when no configured essence drops"),
        ("preview", "👁️", "Preview what a sequence would send, without dispatching it"),
        ("profile", "👤", "Save/load/list/delete a named config snapshot"),
        ("hq-gather", "📢", "Toggle a repeating announcement message on-off"),
        ("jvc", "🔊", "Join and stay connected to a voice channel"),
        ("lvc", "🔇", "Leave the current voice channel"),
        ("reset_stats", "🔄", "Reset all farming stat counters to zero"),
        ("start", "▶️", "Start the farming automation loop"),
        ("stop", "⏹️", "Stop the farming automation loop"),
        ("pause", "⏸️", "Pause the loop without fully stopping it"),
        ("resume", "▶️", "Resume a paused loop"),
        ("ping", "🏓", "Check message round-trip and gateway latency"),
        ("status", "📊", "Show the full farming dashboard"),
        ("logs", "📜", "Show the log file, paginated"),
        ("commands", "📖", "Show this command list"),
    ]

    def _build_commands_pages(obj):
        entries = obj.COMMANDS_REGISTRY
        pages = []
        for i in range(0, len(entries), obj.COMMANDS_PER_PAGE):
            chunk = entries[i:i + obj.COMMANDS_PER_PAGE]
            lines = [f"{emoji} ~~{name:<14} = {desc}" for name, emoji, desc in chunk]
            pages.append("\n".join(lines))
        return pages if pages else ["(no commands registered)"]

    def _format_commands_page(obj, pages, index):
        return (
            f"### 📖 COMMAND REFERENCE (page {index + 1}/{len(pages)})\n"
            f"```ini\n"
            f"{pages[index]}\n"
            f"```"
        )

    @commands.command(name="commands")
    async def commands_list(obj, ctx):
        pages = obj._build_commands_pages()
        index = 0

        msg = await ctx.send(obj._format_commands_page(pages, index))

        if len(pages) <= 1:
            # Nothing to paginate — no arrows added at all (same self-bot
            # limitation as ~~logs: real disableable buttons can't be sent
            # by a user-account message, only by an application/bot one).
            return

        reaction_state = {"left": False, "right": False}

        async def sync_reactions():
            # Keep BOTH arrows visible on every page, including page N/N.
            # Navigation wraps around, so neither arrow becomes a dead end.
            if not reaction_state["left"]:
                await obj.safe_execute(msg.add_reaction, obj.COMMANDS_ARROW_LEFT)
                reaction_state["left"] = True
            if not reaction_state["right"]:
                await obj.safe_execute(msg.add_reaction, obj.COMMANDS_ARROW_RIGHT)
                reaction_state["right"] = True

        await sync_reactions()

        def check(payload):
            return (
                payload.message_id == msg.id
                and payload.user_id == obj.bot.user.id
                and str(payload.emoji) in (obj.COMMANDS_ARROW_LEFT, obj.COMMANDS_ARROW_RIGHT)
            )

        while True:
            try:
                # Same self-bot quirk as ~~logs: the account reacting IS the
                # account clicking, so a click TOGGLES the reaction OFF —
                # we listen for the removal, then re-add it right after.
                payload = await obj.bot.wait_for("raw_reaction_remove", check=check, timeout=obj.LOGS_NAV_TIMEOUT)
            except asyncio.TimeoutError:
                try:
                    await msg.clear_reactions()
                except Exception:
                    pass
                return

            emoji = str(payload.emoji)
            if emoji == obj.COMMANDS_ARROW_LEFT:
                index = (index - 1) % len(pages)  # left wraps to the last page
                reaction_state["left"] = False  # the user's removal just took it off — clear our tracked state so sync_reactions re-adds it
            elif emoji == obj.COMMANDS_ARROW_RIGHT:
                index = (index + 1) % len(pages)  # right goes to the next page (wraps to the first)
                reaction_state["right"] = False  # same as above, for the right arrow

            await obj.safe_execute(msg.edit, content=obj._format_commands_page(pages, index))
            await sync_reactions()

    @commands.Cog.listener()
    async def on_message(obj, message):
        # Feed trivia into the single persistent worker. Do not create a new
        # task per message: the button/modal state is sequential by nature.
        try:
            if (
                getattr(getattr(message, "channel", None), "id", None) == CHANNEL_ID
                and getattr(getattr(message, "author", None), "id", None) == TARGET_BOT_ID
            ):
                trivia_text = _trivia_extract_text(message)
                trivia_expression = _trivia_extract_expression(trivia_text)

                if trivia_expression is not None:
                    try:
                        await _trivia_enqueue_message(message, trivia_expression)
                    except Exception as exc:
                        _trivia_record_error(exc)
                        logger.exception("[TRIVIA] Failed to queue message %s: %s", getattr(message, "id", None), exc)
        except Exception as exc:
            logger.exception("[TRIVIA] FarmingEngine listener error: %s", exc)

        if message.channel.id != CHANNEL_ID or message.author.id != TARGET_BOT_ID:
            return

        # Emergency stop: the target bot's explicit "5/6 within 60 minutes"
        # Fail Count warning means stop EVERYTHING for exactly 62 minutes.
        # We require both the warning sentence and the precise 5/6 + 60-minute
        # marker so ordinary embeds containing unrelated fractions do not pause us.
        text_to_scan = message.content.lower()
        for embed in message.embeds:
            if embed.description:
                text_to_scan += " " + embed.description.lower()
            if embed.title:
                text_to_scan += " " + embed.title.lower()
            if embed.footer and embed.footer.text:
                text_to_scan += " " + embed.footer.text.lower()
            if embed.author and embed.author.name:
                text_to_scan += " " + embed.author.name.lower()
            for field in embed.fields:
                text_to_scan += f" {field.name.lower()} {field.value.lower()}"

        fail_count_warning = (
            "the heavens grow wary of your relentless assault" in text_to_scan
            and re.search(r"\b5\s*/\s*6\s+within\s+60\s+minutes\b", text_to_scan) is not None
        )
        if fail_count_warning:
            await obj._activate_fail_count_pause()
            return

        depleted = any(dep in text_to_scan for dep in ["not enough", "insufficient", "don't have enough", "missing materials", "lack of", "don't possess"])
        if not depleted:
            return

        if obj.config["plant_material"].lower() in text_to_scan:
            if obj.config["plant_enabled"]:
                obj.config["plant_enabled"] = False
                await obj.save_state()
                msg = f"Out of `{obj.config['plant_material']}`. Disabling planting module loops."
                await obj.notify_channel("⚠️ Component Deactivated", msg)
                await obj.send_alert("⚠️ Plant Auto-Disabled", msg, severe=True)
        elif obj.config["refine_recipe_id"].lower() in text_to_scan:
            if obj.config["refine_enabled"]:
                obj.config["refine_enabled"] = False
                await obj.save_state()
                msg = f"Missing materials for recipe `{obj.config['refine_recipe_id']}`. Disabling refine loop."
                await obj.notify_channel("⚠️ Component Deactivated", msg)
                await obj.send_alert("⚠️ Refine Auto-Disabled", msg, severe=True)

    #CHAT GPT INICIA A AGREGAR DESDE ACA

# ---------------------------------------------------------------------------
# Math Trivia Automation
# ---------------------------------------------------------------------------

import ast
import operator as _operator

_TRIVIA_BINARY_OPERATORS = {
    ast.Add: _operator.add,
    ast.Sub: _operator.sub,
    ast.Mult: _operator.mul,
    ast.Div: _operator.truediv,
    ast.FloorDiv: _operator.floordiv,
    ast.Mod: _operator.mod,
    ast.Pow: _operator.pow,
}

_TRIVIA_UNARY_OPERATORS = {
    ast.UAdd: _operator.pos,
    ast.USub: _operator.neg,
}

# Trivia processing uses multiple workers so one slow/failed trivia does not
# block later trivias. The actual button/modal exchange is serialized because
# one account should not try to open several modals simultaneously.
_TRIVIA_ACTIVE_MESSAGE_IDS = set()
_TRIVIA_PENDING_MODAL_FUTURES = []
_TRIVIA_QUEUE = asyncio.Queue()
_TRIVIA_WORKER_TASKS = []
_TRIVIA_INTERACTION_LOCK = asyncio.Lock()
_TRIVIA_ACTIVE_COUNT = 0
# PERF FIX: every "wait until no trivia is active" site used to be a
# `while _TRIVIA_ACTIVE_COUNT > 0: await asyncio.sleep(0.05)` busy-poll loop,
# repeated in 9 different places. Each one wakes up 20x/second checking a
# plain int regardless of whether anything actually changed, which adds up
# across a long fight_horde loop with several such guards per round, and
# means every waiter reacts up to 50ms late even when trivia finishes
# instantly. A single asyncio.Event shared by all of them wakes every waiter
# the moment the last active trivia clears -- no polling, no added latency.
# Starts "set" (idle) since nothing is active yet at import time.
_TRIVIA_IDLE_EVENT = asyncio.Event()
_TRIVIA_IDLE_EVENT.set()
_TRIVIA_WORKER_COUNT = 3
_TRIVIA_EXPIRES_AFTER = 15.0
_TRIVIA_RETRY_DELAY = 0.10
_TRIVIA_CLICK_STAGE_TIMEOUT = 4.0
_TRIVIA_STATS = {
    "detected": 0,
    "math_detected": 0,
    "solved": 0,
    "failed": 0,
    "ignored": 0,
    "errors": 0,
    "answer_clicks": 0,
    "interactions": 0,
    "modals": 0,
    "answers_entered": 0,
    "submits": 0,
    "started_at": time.time(),
    "last_error": None,
    "last_expression": None,
    "last_answer": None,
    "last_message_id": None,
    "fail_reasons": {},
}


def _trivia_record_error(error):
    _TRIVIA_STATS["errors"] += 1
    _TRIVIA_STATS["last_error"] = str(error)


def _trivia_extract_text(message):
    parts = []

    content = getattr(message, "content", None)
    if content:
        parts.append(str(content))

    for embed in getattr(message, "embeds", []) or []:
        for attr in ("title", "description"):
            value = getattr(embed, attr, None)
            if value:
                parts.append(str(value))

        footer = getattr(embed, "footer", None)
        footer_text = getattr(footer, "text", None) if footer else None
        if footer_text:
            parts.append(str(footer_text))

        author = getattr(embed, "author", None)
        author_name = getattr(author, "name", None) if author else None
        if author_name:
            parts.append(str(author_name))

        for field in getattr(embed, "fields", []) or []:
            name = getattr(field, "name", None)
            value = getattr(field, "value", None)
            if name:
                parts.append(str(name))
            if value:
                parts.append(str(value))

    return "\n".join(parts).strip()


def _trivia_normalize_expression(expression):
    return (
        expression.strip()
        .replace("×", "*")
        .replace("✕", "*")
        .replace("÷", "/")
        .replace("−", "-")
        .replace("–", "-")
        .replace("—", "-")
        .replace("^", "**")
        .replace("x", "*")
        .replace("X", "*")
    )


def _trivia_safe_calculate(expression):
    expression = _trivia_normalize_expression(expression)

    if not re.fullmatch(r"[0-9+\-*/().%\s]+", expression):
        raise ValueError("Expression contains unsupported characters.")

    tree = ast.parse(expression, mode="eval")

    def visit(node):
        if isinstance(node, ast.Expression):
            return visit(node.body)

        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
                return node.value
            raise ValueError("Invalid constant.")

        if isinstance(node, ast.UnaryOp):
            operation = _TRIVIA_UNARY_OPERATORS.get(type(node.op))
            if operation is None:
                raise ValueError("Unary operator is not allowed.")
            return operation(visit(node.operand))

        if isinstance(node, ast.BinOp):
            operation = _TRIVIA_BINARY_OPERATORS.get(type(node.op))
            if operation is None:
                raise ValueError("Binary operator is not allowed.")

            left = visit(node.left)
            right = visit(node.right)

            if abs(left) > 10**12 or abs(right) > 10**12:
                raise ValueError("Operand is too large.")

            if isinstance(node.op, ast.Pow) and abs(right) > 20:
                raise ValueError("Exponent is too large.")

            try:
                result = operation(left, right)
            except ZeroDivisionError as exc:
                raise ValueError("Division by zero.") from exc

            if isinstance(result, (int, float)) and abs(result) > 10**18:
                raise ValueError("Result is too large.")

            return result

        raise ValueError(f"Unsupported AST node: {type(node).__name__}")

    result = visit(tree)

    if isinstance(result, float):
        if result.is_integer():
            return int(result)
        return round(result, 10)

    return result


def _trivia_extract_expression(text):
    """Extract only the explicit '= ?' math format used by the trivia."""
    if not text:
        return None

    # This intentionally requires '= ?' so ordinary messages such as 1/1
    # are never mistaken for trivia.
    match = re.search(
        r"(?P<expr>[0-9][0-9\s+\-*/().%×÷xX−–—^]*?)\s*=\s*\?",
        text,
        flags=re.IGNORECASE,
    )

    if not match:
        return None

    expression = match.group("expr").strip()

    if not re.search(r"[+\-*/%×÷xX−–—^]", expression):
        return None

    return expression


def _trivia_all_components(message):
    result = []

    for row in getattr(message, "components", []) or []:
        children = getattr(row, "children", None)
        if children is not None:
            try:
                result.extend(list(children))
            except TypeError:
                pass
        else:
            result.append(row)

    return result


def _trivia_find_answer_button(message):
    components = _trivia_all_components(message)

    logger.info(
        "[TRIVIA] Component scan: %s component(s).",
        len(components),
    )

    for index, component in enumerate(components, start=1):
        label = getattr(component, "label", None)
        custom_id = getattr(component, "custom_id", None)
        disabled = getattr(component, "disabled", False)

        logger.info(
            "[TRIVIA] Component %s: type=%s label=%r custom_id=%r disabled=%r",
            index,
            type(component).__name__,
            label,
            custom_id,
            disabled,
        )

        if disabled:
            continue

        # The trivia button is labeled "✍️ Answer", not just "Answer".
        # Match the word "answer" anywhere in the visible label so emojis
        # or other decorative characters do not prevent detection.
        label_text = str(label or "").strip().lower()
        custom_id_text = str(custom_id or "").strip().lower()

        if "answer" in label_text:
            return component

        if "answer" in custom_id_text:
            return component

    return None


def _trivia_modal_components(modal):
    components = []

    for row in getattr(modal, "components", []) or []:
        children = getattr(row, "children", None)
        if children is not None:
            try:
                components.extend(list(children))
            except TypeError:
                pass
        else:
            components.append(row)

    return components


def _trivia_find_input(modal):
    components = _trivia_modal_components(modal)

    for index, component in enumerate(components, start=1):
        label = str(getattr(component, "label", "") or "").strip().lower()
        placeholder = str(getattr(component, "placeholder", "") or "").strip().lower()
        custom_id = str(getattr(component, "custom_id", "") or "").strip().lower()
        component_type = str(getattr(getattr(component, "type", None), "name", "") or "").lower()

        logger.info(
            "[TRIVIA] Modal component %s: type=%s label=%r placeholder=%r custom_id=%r",
            index,
            type(component).__name__,
            label,
            placeholder,
            custom_id,
        )

        haystack = " ".join((label, placeholder, custom_id, component_type))

        if (
            "enter the answer" in haystack
            or placeholder == "enter the answer"
            or custom_id in ("enter_the_answer", "answer")
        ):
            return component

    # If the label/custom_id is different, use the first TextInput.
    for component in components:
        if type(component).__name__.lower() == "textinput":
            return component

    return None


def _trivia_find_modal_future(channel_id):
    for item in list(_TRIVIA_PENDING_MODAL_FUTURES):
        if item[0] == channel_id and not item[1].done():
            return item[1]
    return None


async def trivia_interaction_listener(interaction):
    """Capture the interaction created by the Answer button click."""
    try:
        _TRIVIA_STATS["interactions"] += 1

        channel = getattr(interaction, "channel", None)
        channel_id = getattr(channel, "id", None)
        modal = getattr(interaction, "modal", None)

        logger.info(
            "[TRIVIA] Interaction received | id=%s | type=%s | nonce=%s | channel=%s | modal=%s",
            getattr(interaction, "id", None),
            getattr(interaction, "type", None),
            getattr(interaction, "nonce", None),
            channel_id,
            type(modal).__name__ if modal is not None else None,
        )

        # On some versions/flows the modal is attached to the interaction
        # before the standalone 'modal' event is observed by user listeners.
        # Use it directly when available.
        if modal is not None:
            future = _trivia_find_modal_future(channel_id)
            if future is None:
                # The channel can be unavailable in an edge case, but there
                # is only one active trivia modal at a time in this bot.
                for item in list(_TRIVIA_PENDING_MODAL_FUTURES):
                    if not item[1].done():
                        future = item[1]
                        break

            if future is not None and not future.done():
                future.set_result(modal)
                logger.info("[TRIVIA] Modal obtained directly from interaction.")

    except Exception as exc:
        logger.exception("[TRIVIA] Interaction listener error: %s", exc)


async def trivia_modal_listener(modal):
    """Capture the actual modal object Discord dispatches after Answer."""
    try:
        interaction = getattr(modal, "interaction", None)
        channel = getattr(interaction, "channel", None)
        channel_id = getattr(channel, "id", None)

        # Do NOT reject the modal just because the channel is temporarily
        # unavailable on the interaction object. The pending future already
        # belongs to the trivia that triggered it.
        logger.info(
            "[TRIVIA] MODAL EVENT RECEIVED | id=%s | custom_id=%s | title=%s | channel=%s",
            getattr(modal, "id", None),
            getattr(modal, "custom_id", None),
            getattr(modal, "title", None),
            channel_id,
        )

        future = _trivia_find_modal_future(channel_id) if channel_id is not None else None

        if future is None:
            # Fallback: only one trivia modal is expected to be pending at a time.
            for item in list(_TRIVIA_PENDING_MODAL_FUTURES):
                if not item[1].done():
                    future = item[1]
                    break

        if future is not None and not future.done():
            future.set_result(modal)
            logger.info("[TRIVIA] Modal future resolved successfully.")
        else:
            logger.warning("[TRIVIA] Modal received but no pending trivia future exists.")

    except Exception as exc:
        logger.exception("[TRIVIA] Modal listener error: %s", exc)


async def _trivia_wait_for_modal(channel_id, timeout=5.0):
    """Wait for discord.py-self's real 'modal' dispatch event."""
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    item = (channel_id, future)
    _TRIVIA_PENDING_MODAL_FUTURES.append(item)

    try:
        return await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        return None
    finally:
        try:
            _TRIVIA_PENDING_MODAL_FUTURES.remove(item)
        except ValueError:
            pass


async def _trivia_fill_input(input_component, answer):
    answer = str(answer)

    # discord.py-self TextInput has a dedicated answer() method.
    answer_method = getattr(input_component, "answer", None)
    if callable(answer_method):
        answer_method(answer)
        return True

    # Fallback for compatible component implementations.
    value_property = getattr(type(input_component), "value", None)
    if value_property is not None:
        try:
            input_component.value = answer
            return True
        except Exception as exc:
            logger.warning(
                "[TRIVIA] Could not set TextInput.value: %s",
                exc,
            )

    return False


async def _trivia_submit_modal(modal):
    """Submit the actual discord.py-self Modal object."""
    submit = getattr(modal, "submit", None)

    if not callable(submit):
        raise RuntimeError(
            f"Modal type {type(modal).__name__} does not expose submit()."
        )

    result = submit()
    if hasattr(result, "__await__"):
        await result

    return True


async def _trivia_enqueue_message(message, expression=None):
    global _TRIVIA_ACTIVE_COUNT

    cog = bot.get_cog("FarmingEngine")
    if cog is not None and cog._fail_count_pause_active():
        logger.info("[TRIVIA] Ignored during 5/6 emergency pause | id=%s", getattr(message, "id", None))
        return False

    message_id = getattr(message, "id", None)
    if message_id is None:
        return False

    if message_id in _TRIVIA_ACTIVE_MESSAGE_IDS:
        logger.debug("[TRIVIA] Duplicate message ignored: id=%s", message_id)
        return False

    if expression is None:
        expression = _trivia_extract_expression(_trivia_extract_text(message))
    if expression is None:
        return False

    _TRIVIA_ACTIVE_MESSAGE_IDS.add(message_id)
    _TRIVIA_ACTIVE_COUNT += 1
    _TRIVIA_IDLE_EVENT.clear()
    _TRIVIA_STATS["detected"] += 1
    _TRIVIA_STATS["math_detected"] += 1

    try:
        _TRIVIA_QUEUE.put_nowait(message)
    except Exception:
        _TRIVIA_ACTIVE_MESSAGE_IDS.discard(message_id)
        _TRIVIA_ACTIVE_COUNT = max(0, _TRIVIA_ACTIVE_COUNT - 1)
        if _TRIVIA_ACTIVE_COUNT == 0:
            _TRIVIA_IDLE_EVENT.set()
        raise

    logger.info(
        "[TRIVIA] Queued math trivia | id=%s | expression=%s | queue=%s | active=%s",
        message_id,
        expression,
        _TRIVIA_QUEUE.qsize(),
        _TRIVIA_ACTIVE_COUNT,
    )
    return True


async def _trivia_worker(worker_index):
    global _TRIVIA_ACTIVE_COUNT

    logger.info("[TRIVIA] Worker %s started.", worker_index)
    while True:
        message = await _TRIVIA_QUEUE.get()
        message_id = getattr(message, "id", None)
        try:
            cog = bot.get_cog("FarmingEngine")
            if cog is not None and cog._fail_count_pause_active():
                logger.info("[TRIVIA] Dropping queued trivia during 5/6 emergency pause | id=%s", message_id)
                continue

            logger.info(
                "[TRIVIA] Worker %s processing | id=%s | remaining=%s | active=%s",
                worker_index,
                message_id,
                _TRIVIA_QUEUE.qsize(),
                _TRIVIA_ACTIVE_COUNT,
            )
            await _trivia_with_retries(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _trivia_record_error(exc)
            logger.exception(
                "[TRIVIA] Worker %s recovered from unexpected error for message %s: %s",
                worker_index,
                message_id,
                exc,
            )
        finally:
            _TRIVIA_ACTIVE_MESSAGE_IDS.discard(message_id)
            _TRIVIA_ACTIVE_COUNT = max(0, _TRIVIA_ACTIVE_COUNT - 1)
            if _TRIVIA_ACTIVE_COUNT == 0:
                _TRIVIA_IDLE_EVENT.set()
            _TRIVIA_QUEUE.task_done()
            logger.info(
                "[TRIVIA] Worker %s finished | id=%s | active=%s | queue=%s",
                worker_index,
                message_id,
                _TRIVIA_ACTIVE_COUNT,
                _TRIVIA_QUEUE.qsize(),
            )


async def _trivia_mark_failed(message, message_id, reason=None):
    """Correct the optimistic ✅ reaction (added on attempt 1 before the
    button/modal/submit exchange is known to succeed) once every retry has
    been exhausted, so the message doesn't keep a false 'solved' signal."""
    _TRIVIA_STATS["failed"] += 1
    bucket = (reason or "unknown").split(":")[0].strip() or "unknown"
    _TRIVIA_STATS["fail_reasons"][bucket] = _TRIVIA_STATS["fail_reasons"].get(bucket, 0) + 1
    try:
        me = getattr(bot, "user", None)
        if me is not None:
            await message.remove_reaction("✅", me)
    except Exception as exc:
        logger.debug("[TRIVIA] Could not remove ✅ reaction on failure | id=%s: %s", message_id, exc)
    try:
        await message.add_reaction("❌")
    except Exception as exc:
        logger.debug("[TRIVIA] Could not add ❌ reaction on failure | id=%s: %s", message_id, exc)
    try:
        cog = _get_cog()
        if cog is not None:
            detail = f" ({reason})" if reason else ""
            await cog.send_alert(
                "⚠️ Trivia Failed",
                f"Exhausted all retries within the 15s window for trivia message `{message_id}`{detail}.",
                severe=False,
            )
    except Exception as exc:
        logger.debug("[TRIVIA] Could not send failure alert | id=%s: %s", message_id, exc)


async def _trivia_with_retries(message):
    """Retry a trivia only while its original 15-second lifetime remains."""
    message_id = getattr(message, "id", None)
    created_at = getattr(message, "created_at", None)
    if created_at is not None:
        try:
            deadline = created_at.timestamp() + _TRIVIA_EXPIRES_AFTER
        except Exception:
            deadline = time.time() + _TRIVIA_EXPIRES_AFTER
    else:
        deadline = time.time() + _TRIVIA_EXPIRES_AFTER

    attempt = 0
    last_attempt_reason = None
    fail_ctx = {}
    struggle_alert_sent = False
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            logger.info("[TRIVIA] Expired before attempt | id=%s", message_id)
            return False

        attempt += 1
        fail_ctx.clear()
        logger.info("[TRIVIA] Attempt %s | id=%s | %.2fs remaining", attempt, message_id, remaining)

        try:
            # Serialize only the interactive Discord component exchange. This
            # prevents multiple workers from opening conflicting modals at once.
            async with _TRIVIA_INTERACTION_LOCK:
                # Bound ONE attempt, not the whole 15-second trivia lifetime.
                # If the interaction stalls, _trivia_process_message returns and
                # the outer loop starts a fresh attempt from scratch.
                attempt_timeout = min(1.0, max(0.1, remaining))
                result = await asyncio.wait_for(
                    _trivia_process_message(message, deadline=deadline, attempt=attempt, fail_ctx=fail_ctx),
                    timeout=attempt_timeout,
                )
            if result:
                return True
            last_attempt_reason = fail_ctx.get("reason") or "finished without solving"
            logger.warning(
                "[TRIVIA] Attempt %s finished without solving | id=%s | reason=%s",
                attempt,
                message_id,
                last_attempt_reason,
            )
        except asyncio.TimeoutError:
            last_attempt_reason = fail_ctx.get("reason") or "timed out"
            logger.warning("[TRIVIA] Attempt %s timed out | id=%s", attempt, message_id)
        except Exception as exc:
            last_attempt_reason = fail_ctx.get("reason") or str(exc)
            _trivia_record_error(exc)
            logger.warning("[TRIVIA] Attempt %s failed | id=%s: %s", attempt, message_id, exc)

        remaining = deadline - time.time()
        if remaining <= 0:
            logger.info("[TRIVIA] Expired after %s attempt(s) | id=%s", attempt, message_id)
            if attempt >= 1:
                await _trivia_mark_failed(message, message_id, reason=last_attempt_reason)
            return False

        # First sign of trouble: give a human the maximum possible lead time
        # to jump in manually, in case the automation genuinely can't close
        # this one out. Fired once per trivia, as a background task so it
        # never delays the next attempt. A few seconds' notice is still
        # better odds than none, and it costs nothing on the (much more
        # common) path where the very next attempt succeeds anyway.
        if not struggle_alert_sent:
            struggle_alert_sent = True
            expr = fail_ctx.get("expression") or _TRIVIA_STATS.get("last_expression")
            ans = fail_ctx.get("answer")

            async def _send_struggle_alert():
                try:
                    cog = _get_cog()
                    if cog is not None:
                        latency_ms = round(getattr(bot, "latency", 0.0) * 1000)
                        await cog.send_alert(
                            "🟡 Trivia Struggling",
                            (
                                f"Attempt {attempt} failed ({last_attempt_reason}) with "
                                f"{remaining:.1f}s left | id=`{message_id}` | "
                                f"expr=`{expr}` answer=`{ans}` | ws_latency={latency_ms}ms"
                            ),
                            severe=False,
                        )
                except Exception as exc:
                    logger.debug("[TRIVIA] Could not send struggle alert | id=%s: %s", message_id, exc)

            asyncio.create_task(_send_struggle_alert())

        await asyncio.sleep(min(_TRIVIA_RETRY_DELAY, max(0.01, remaining)))


async def _trivia_process_message(message, deadline=None, attempt=1, fail_ctx=None):
    message_id = getattr(message, "id", None)

    def _remaining():
        return (deadline - time.time()) if deadline is not None else float("inf")

    def _ensure_time():
        remaining = _remaining()
        if remaining <= 0:
            raise TimeoutError("Trivia expired before completion")
        return remaining

    # Queue ownership is established by _trivia_enqueue_message(). Keeping
    # this function focused on processing avoids rejecting its own queued item.
    try:
        _TRIVIA_STATS["last_message_id"] = message_id

        text = _trivia_extract_text(message)

        logger.info(
            "[TRIVIA] Candidate message received | id=%s | channel=%s",
            message_id,
            getattr(message.channel, "id", "unknown"),
        )
        logger.info(
            "[TRIVIA] Message text: %s",
            text.replace("\n", " ")[:1000] if text else "<empty>",
        )

        expression = _trivia_extract_expression(text)
        if expression is None:
            _TRIVIA_STATS["ignored"] += 1
            return

        _TRIVIA_STATS["last_expression"] = expression

        logger.info(
            "[TRIVIA] Mathematical expression detected: %s",
            expression,
        )
        _ensure_time()

        try:
            answer = _trivia_safe_calculate(expression)
        except Exception as exc:
            _trivia_record_error(exc)
            if fail_ctx is not None:
                fail_ctx["reason"] = "calc_error"
            logger.error("[TRIVIA] Failed to solve %r: %s", expression, exc)
            return

        _TRIVIA_STATS["last_answer"] = answer
        if fail_ctx is not None:
            fail_ctx["expression"] = expression
            fail_ctx["answer"] = answer
        logger.info("[TRIVIA] Calculated answer: %s", answer)
        _ensure_time()

        # React only once. Retries should not add duplicate reactions.
        # Fired as a background task instead of awaited: this reaction is
        # purely cosmetic and its REST round-trip has no reason to delay
        # reaching the button click, which is the actual time-critical step.
        if attempt == 1:
            async def _react_ack():
                try:
                    await message.add_reaction("✅")
                    logger.info("[TRIVIA] Added ✅ reaction to trivia message.")
                except Exception as exc:
                    _trivia_record_error(exc)
                    logger.error("[TRIVIA] Could not add ✅ reaction: %s", exc)

            asyncio.create_task(_react_ack())

        # Refresh the message before retries (never on attempt 1, where the
        # message just arrived fresh off the gateway and a REST round-trip
        # here would only eat into the 15s budget for no benefit). Retries
        # DO need a fresh copy so they never reuse a stale Button object
        # after Discord has updated the component.
        current_message = message
        if attempt > 1:
            try:
                fetch_message = getattr(message.channel, "fetch_message", None)
                if callable(fetch_message):
                    current_message = await fetch_message(message.id)
                    logger.info("[TRIVIA] Refreshed trivia message before interaction | id=%s", message.id)
            except Exception as exc:
                logger.warning("[TRIVIA] Could not refresh trivia message; using cached message: %s", exc)

        button = _trivia_find_answer_button(current_message)
        if button is None:
            _trivia_record_error("Answer button not found")
            if fail_ctx is not None:
                fail_ctx["reason"] = "button_not_found"
            logger.error("[TRIVIA] Answer button was not found.")
            return

        logger.info("[TRIVIA] Answer button found.")

        # discord.py-self v2.1.0 returns the Interaction created by
        # Button.click(). The modal opened by Discord is attached to that
        # Interaction asynchronously, so keep the returned Interaction and
        # use its modal first. The existing modal-event listener remains as a
        # compatibility fallback if the modal is dispatched just after click().
        # Keep the fallback modal listener alive for the whole remaining life of
        # this trivia. A short fixed timeout here could finish with None while
        # Button.click() was still legitimately waiting for its gateway ACK.
        modal_wait_task = asyncio.create_task(
            _trivia_wait_for_modal(
                current_message.channel.id,
                timeout=max(0.1, _remaining()),
            )
        )

        try:
            click = getattr(button, "click", None)
            if not callable(click):
                raise RuntimeError(
                    f"Answer button type {type(button).__name__} has no click()."
                )

            # Button.click() can wait internally for interaction_finish.
            # Give this click a short per-attempt budget; if it stalls, cancel
            # it and let the outer retry loop redo the COMPLETE process.
            click_task = asyncio.create_task(click())
            modal = getattr(click_task, "modal", None)  # normally None; defensive

            click_timeout = min(1.0, max(0.1, _remaining()))
            done, pending = await asyncio.wait(
                {click_task, modal_wait_task},
                timeout=click_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not done:
                if not click_task.done():
                    click_task.cancel()
                if not modal_wait_task.done():
                    modal_wait_task.cancel()
                await asyncio.gather(click_task, modal_wait_task, return_exceptions=True)
                if fail_ctx is not None:
                    fail_ctx["reason"] = "click_stalled"
                logger.warning(
                    "[TRIVIA] Button interaction stalled; abandoning attempt %s and restarting full process | id=%s",
                    attempt,
                    message_id,
                )
                return

            if modal_wait_task in done:
                # The modal itself is the authoritative result we need. The
                # click task may still be waiting for interaction_finish; cancel
                # that local wait so it cannot later turn into a noisy
                # InvalidData exception. The interaction request has already
                # been sent by Button.click().
                modal = modal_wait_task.result()
                if not click_task.done():
                    click_task.cancel()
                    await asyncio.gather(click_task, return_exceptions=True)
                _TRIVIA_STATS["answer_clicks"] += 1
                logger.info(
                    "[TRIVIA] Answer button interaction produced modal before click() returned. "
                    "modal=%s",
                    getattr(modal, "id", None),
                )
            else:
                # Button.click() can raise InvalidData when its internal
                # interaction_finish event is missed even though Discord has
                # already accepted the component interaction. The modal event
                # is a stronger signal for this trivia flow, so give the armed
                # modal waiter a short grace window before treating the click as
                # genuinely failed. This avoids needlessly re-clicking a button
                # when the first interaction actually succeeded.
                try:
                    interaction = click_task.result()
                except discord.InvalidData as exc:
                    # Do NOT wait on the same failed click. This attempt is
                    # finished; the outer loop will refresh the message and redo
                    # expression extraction, calculation, button lookup and click.
                    if not modal_wait_task.done():
                        modal_wait_task.cancel()
                        await asyncio.gather(modal_wait_task, return_exceptions=True)
                    if fail_ctx is not None:
                        fail_ctx["reason"] = "button_click_failed"
                    logger.warning(
                        "[TRIVIA] Button.click() failed; abandoning attempt %s and restarting full process | id=%s | error=%s",
                        attempt,
                        message_id,
                        exc,
                    )
                    return
                except Exception:
                    raise
                else:
                    _TRIVIA_STATS["answer_clicks"] += 1

                    logger.info(
                        "[TRIVIA] Answer button clicked | interaction=%s | type=%s | successful=%s",
                        getattr(interaction, "id", None),
                        getattr(interaction, "type", None),
                        getattr(interaction, "successful", None),
                    )

                    # v2.1.0 may attach the modal to the returned interaction a
                    # little after click() returns. Prefer it when available.
                    modal = getattr(interaction, "modal", None)

                    if modal is None:
                        # Poll both the interaction's own `.modal` attribute
                        # (populated a little after click() returns on some
                        # versions) and modal_wait_task (resolved the instant
                        # the dispatched 'modal' event/interaction listener
                        # fires -- often faster than the attribute). A 0.02s
                        # tick instead of 0.1s means we react within ~20ms of
                        # either becoming available instead of up to 100ms.
                        elapsed = 0.0
                        while elapsed < 1.0:
                            if _remaining() <= 0:
                                break
                            if modal_wait_task.done():
                                try:
                                    modal = modal_wait_task.result()
                                except Exception:
                                    modal = None
                                break
                            await asyncio.sleep(0.02)
                            elapsed += 0.02
                            modal = getattr(interaction, "modal", None)
                            if modal is not None:
                                break

                    if modal is None:
                        # click() completed, so any modal event that arrived should
                        # be picked up by the already-armed fallback waiter.
                        try:
                            modal = await asyncio.wait_for(
                                modal_wait_task,
                                timeout=max(0.1, _remaining()),
                            )
                        except asyncio.TimeoutError:
                            modal = None

        except Exception:
            if not modal_wait_task.done():
                modal_wait_task.cancel()
            raise

        if modal is None:
            _trivia_record_error("Modal event was not received")
            if fail_ctx is not None:
                fail_ctx["reason"] = "modal_not_received"
            logger.error(
                "[TRIVIA] Answer was clicked, but no modal event was received."
            )
            return

        _TRIVIA_STATS["modals"] += 1
        logger.info(
            "[TRIVIA] Real modal received | id=%s | custom_id=%s | title=%s",
            getattr(modal, "id", None),
            getattr(modal, "custom_id", None),
            getattr(modal, "title", None),
        )

        answer_input = _trivia_find_input(modal)
        if answer_input is None:
            _trivia_record_error("Enter the answer input not found")
            if fail_ctx is not None:
                fail_ctx["reason"] = "input_not_found"
            logger.error("[TRIVIA] Could not find 'Enter the answer'.")
            return

        if not await _trivia_fill_input(answer_input, answer):
            _trivia_record_error("Could not fill Enter the answer")
            if fail_ctx is not None:
                fail_ctx["reason"] = "fill_failed"
            logger.error(
                "[TRIVIA] Could not enter answer into 'Enter the answer'."
            )
            return

        _TRIVIA_STATS["answers_entered"] += 1
        logger.info("[TRIVIA] Answer entered: %s", answer)

        try:
            await _trivia_submit_modal(modal)
        except Exception as exc:
            _trivia_record_error(exc)
            if fail_ctx is not None:
                fail_ctx["reason"] = "submit_failed"
            logger.error(
                "[TRIVIA] Failed to submit modal: %s",
                exc,
                exc_info=True,
            )
            return

        _TRIVIA_STATS["submits"] += 1
        _TRIVIA_STATS["solved"] += 1

        logger.info(
            "[TRIVIA] Math trivia completed successfully: %s -> %s",
            expression,
            answer,
        )
        return True

    except Exception as exc:
        _trivia_record_error(exc)
        if fail_ctx is not None:
            fail_ctx["reason"] = f"unexpected: {exc}"
        logger.exception("[TRIVIA] Unexpected trivia error: %s", exc)
    finally:
        # Queue/active-state ownership belongs to the worker.
        # Do not mutate the active set here because retries stay inside the
        # same queued item and the worker is the single source of truth.
        pass


@commands.command(name="trivia")
async def trivia_status(ctx):
    """Display math trivia statistics."""
    uptime = str(
        timedelta(
            seconds=int(time.time() - _TRIVIA_STATS["started_at"])
        )
    )

    last_error = _TRIVIA_STATS["last_error"] or "NONE"
    last_expression = _TRIVIA_STATS["last_expression"] or "NONE"
    last_answer = _TRIVIA_STATS["last_answer"]
    last_answer = "NONE" if last_answer is None else str(last_answer)

    solved = _TRIVIA_STATS["solved"]
    failed = _TRIVIA_STATS["failed"]
    resolved_total = solved + failed
    success_rate = f"{(solved / resolved_total * 100):.1f}%" if resolved_total else "N/A"
    fail_reasons = _TRIVIA_STATS.get("fail_reasons") or {}
    fail_breakdown = (
        ", ".join(f"{k}={v}" for k, v in sorted(fail_reasons.items(), key=lambda kv: -kv[1]))
        if fail_reasons else "NONE"
    )

    status = (
        "**🧮 Trivia Status**\n"
        "```\n"
        f"System            = ACTIVE\n"
        f"Channel ID        = {CHANNEL_ID}\n"
        f"Target Bot ID     = {TARGET_BOT_ID}\n"
        f"Uptime            = {uptime}\n\n"
        f"Detected          = {_TRIVIA_STATS['detected']:,}\n"
        f"Math Trivias      = {_TRIVIA_STATS['math_detected']:,}\n"
        f"Solved            = {solved:,}\n"
        f"Failed            = {failed:,}\n"
        f"Success Rate      = {success_rate}\n"
        f"Fail Breakdown    = {fail_breakdown}\n"
        f"Ignored           = {_TRIVIA_STATS['ignored']:,}\n"
        f"Answer Clicks     = {_TRIVIA_STATS['answer_clicks']:,}\n"
        f"Interactions      = {_TRIVIA_STATS['interactions']:,}\n"
        f"Modals            = {_TRIVIA_STATS['modals']:,}\n"
        f"Answers Entered   = {_TRIVIA_STATS['answers_entered']:,}\n"
        f"Submits           = {_TRIVIA_STATS['submits']:,}\n"
        f"Errors            = {_TRIVIA_STATS['errors']:,}\n\n"
        f"Last Expression   = {last_expression}\n"
        f"Last Answer       = {last_answer}\n"
        f"Last Error        = {last_error}\n"
        "```"
    )

    try:
        await ctx.send(status)
    except Exception as exc:
        logger.error("[TRIVIA] Failed to send trivia status: %s", exc)


# ---------------------------------------------------------------------------
# CIPHER — Console UI (live dashboard + command input)
# ---------------------------------------------------------------------------
import threading
from collections import deque
from rich.align import Align
from rich.box import HEAVY, ROUNDED
from rich.console import Console as _RichConsole
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

_ACCENT = "#5865F2"
_console = _RichConsole()

_CONSOLE_LOG = deque(maxlen=300)      # command feedback log
_INPUT_BUFFER = {"text": ""}
_STOP_EVENT = threading.Event()
_VALID_COMMANDS = ("start", "stop", "pause", "resume", "status", "help", "quit")

# Marks when the console/bot process was launched, used for "Bot Uptime"
# (distinct from the farming loop's own stats_start_time uptime).
_BOT_START_TS = time.time()

_BANNER_CIPHER = (
    " ███   █████ ████  █   █ █████ ████  \n"
    "█   █    █   █   █ █   █ █     █   █ \n"
    "█        █   ████  █████ ████  ████  \n"
    "█   █    █   █      █    █     █  █  \n"
    " ███   █████ █      █    █████ █   █ "
)


def _log_feedback(text, style=None):
    ts = time.strftime("%H:%M:%S")
    _CONSOLE_LOG.append((ts, text, style))


class _FakeMessage:
    async def add_reaction(self, emoji):
        _log_feedback(f"reaction: {emoji}", style=_ACCENT)


class _FakeCtx:
    """Stands in for a discord.py ctx so the Cog's commands can be invoked
    directly from the console, without going through Discord."""

    def __init__(self, bot_obj):
        self.bot = bot_obj
        self.message = _FakeMessage()
        self.author = bot_obj.user

    async def send(self, content=""):
        for line in str(content).split("\n"):
            if line.strip():
                _log_feedback(line)
        return _FakeMessage()


async def _run_cog_command(cog, name):
    command = bot.get_command(name)
    if command is None:
        _log_feedback(f"[!] Command '{name}' does not exist on the bot.", style="bold red")
        return
    ctx = _FakeCtx(bot)
    try:
        await command.callback(cog, ctx)
    except Exception as exc:  # noqa: BLE001
        _log_feedback(f"[!] Error running '{name}': {exc}", style="bold red")


def _get_cog():
    return bot.get_cog("FarmingEngine")


def _make_header():
    banner = Text(_BANNER_CIPHER, style=f"bold {_ACCENT}", justify="center")
    subtitle = Text("Discord Automation Suite — Control Console", style="dim italic", justify="center")
    group = Text()
    group.append_text(banner)
    group.append("\n")
    group.append_text(subtitle)
    return Panel(Align.center(group), border_style=_ACCENT, box=HEAVY, padding=(1, 2))


def _make_system_table(cog):
    table = Table(title="🖥  System", border_style=_ACCENT, box=ROUNDED, expand=True, title_style=f"bold {_ACCENT}")
    table.add_column("Field", style="bold")
    table.add_column("Value")

    now = time.time()
    bot_uptime = str(timedelta(seconds=int(now - _BOT_START_TS)))
    table.add_row("Bot Uptime", bot_uptime)

    if cog is None:
        table.add_row("Status", "[bold red]NOT CONNECTED (waiting for login...)[/]")
        return table

    if cog.config.get("is_paused"):
        loop_status, style = "PAUSED", "yellow"
    elif cog.farm_loop.is_running():
        loop_status, style = "RUNNING", "green"
    else:
        loop_status, style = "STOPPED", "red"

    farm_uptime = (
        str(timedelta(seconds=int(now - cog.config["stats_start_time"])))
        if cog.config.get("stats_start_time")
        else "0:00:00"
    )

    channel_obj = bot.get_channel(CHANNEL_ID) if CHANNEL_ID else None
    if channel_obj:
        channel_display = f"#{channel_obj.name} ({CHANNEL_ID})"
    elif CHANNEL_ID:
        channel_display = f"[yellow]unresolved (id={CHANNEL_ID})[/]"
    else:
        channel_display = "[yellow]NOT SET[/]"

    next_plant = format_countdown(cog.config["last_plant_run"] + cog.config["plant_interval"], now) if cog.config["last_plant_run"] else "DUE / IMMEDIATE"
    next_refine = format_countdown(cog.config["last_refine_run"] + cog.config["refine_interval"], now) if cog.config["last_refine_run"] else "DUE / IMMEDIATE"
    next_hunt = format_countdown(cog.config["last_hunt_run"] + cog.config["hunt_interval"], now) if cog.config["last_hunt_run"] else "DUE / IMMEDIATE"
    next_break = format_countdown(cog.config["last_break_time"] + cog.config["break_interval"], now) if cog.config["last_break_time"] else "PENDING"

    table.add_row("Loop", f"[bold {style}]{loop_status}[/]")
    table.add_row("Target Channel", channel_display)
    table.add_row("Farm Uptime", farm_uptime)
    table.add_row("Watchdog", "[green]ACTIVE[/]" if cog.farm_loop_watchdog.is_running() else "[dim]INACTIVE[/]")
    table.add_row("Next Plant", next_plant)
    table.add_row("Next Refine", next_refine)
    table.add_row("Next Hunt", next_hunt)
    table.add_row("Next Break", next_break)
    table.add_row("Last Error", str(cog.config.get("last_error") or "NONE"))
    return table


def _make_stats_table(cog):
    table = Table(title="📊 Stats", border_style=_ACCENT, box=ROUNDED, expand=True, title_style=f"bold {_ACCENT}")
    table.add_column("Metric", style="bold")
    table.add_column("Total", justify="right")

    rows = [
        ("Harvest", cog.config.get("stats_harvest_count", 0) if cog else 0),
        ("Plant", cog.config.get("stats_plant_count", 0) if cog else 0),
        ("Refine", cog.config.get("stats_refine_count", 0) if cog else 0),
        ("Fight Horde", cog.config.get("stats_fight_horde_count", 0) if cog else 0),
        ("Hunt", cog.config.get("stats_hunt_count", 0) if cog else 0),
    ]
    for name, value in rows:
        table.add_row(name, f"{value:,}")
    return table


def _make_slots_table(cog):
    table = Table(title="🧩 Modular Slots (1-15)", border_style=_ACCENT, box=ROUNDED, expand=True, title_style=f"bold {_ACCENT}")
    table.add_column("Slot", justify="center")
    table.add_column("Status")
    table.add_column("Command")
    table.add_column("Prefix", justify="center")
    table.add_column("OK", justify="right")
    table.add_column("Fail", justify="right")

    if cog is None:
        table.add_row("-", "[dim]no data[/dim]", "-", "-", "-", "-")
        return table

    for slot in MODULAR_SLOT_IDS:
        enabled = cog.config.get(f"slot{slot}_enabled")
        state = "[bold green]ON[/]" if enabled else "[dim]OFF[/]"
        cmd = cog.config.get(f"slot{slot}_command") or "[dim](unassigned)[/dim]"
        prefix = cog.config.get(f"cmd_prefix_slot{slot}", "/")
        ok = cog.config.get(f"stats_slot{slot}_count", 0)
        fail = cog.config.get(f"stats_slot{slot}_failures", 0)
        fail_style = "red" if fail else "dim"
        table.add_row(str(slot), state, cmd, prefix, f"{ok:,}", f"[{fail_style}]{fail:,}[/{fail_style}]")
    return table


def _make_console_panel():
    lines = []
    for ts, text, style in list(_CONSOLE_LOG)[-12:]:
        prefix = f"[dim]{ts}[/dim] "
        lines.append(f"{prefix}[{style}]{text}[/{style}]" if style else f"{prefix}{text}")
    body = "\n".join(lines) if lines else "[dim](no activity yet — type 'help')[/dim]"
    return Panel(body, title="💬 Console / Feedback", border_style=_ACCENT, box=ROUNDED)


def _make_input_panel():
    prompt = Text()
    prompt.append("CIPHER", style=f"bold {_ACCENT}")
    prompt.append(" > ", style="bold")
    prompt.append(_INPUT_BUFFER["text"])
    prompt.append("█", style=f"bold {_ACCENT}")
    return Panel(prompt, border_style=_ACCENT, box=ROUNDED)


def _build_layout():
    cog = _get_cog()
    layout = Layout()
    layout.split_column(
        Layout(_make_header(), name="header", size=8),
        Layout(name="body", ratio=1),
        Layout(_make_console_panel(), name="console", size=15),
        Layout(_make_input_panel(), name="input", size=3),
    )
    layout["body"].split_row(
        Layout(name="left", ratio=1),
        Layout(_make_slots_table(cog), name="slots", ratio=2),
    )
    layout["left"].split_column(
        Layout(_make_system_table(cog), name="system"),
        Layout(_make_stats_table(cog), name="stats"),
    )
    return layout


_HELP_TEXT = "Commands: start, stop, pause, resume, status, help, quit"


def _stdin_reader(loop, queue):
    """Reads keyboard input in raw mode, without blocking the dashboard refresh."""
    try:
        import select
        import termios
        import tty

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            attrs = termios.tcgetattr(fd)
            attrs[3] = attrs[3] & ~termios.ECHO  # we render the input ourselves
            termios.tcsetattr(fd, termios.TCSANOW, attrs)

            while not _STOP_EVENT.is_set():
                ready, _, _ = select.select([sys.stdin], [], [], 0.1)
                if not ready:
                    continue
                ch = sys.stdin.read(1)
                if ch in ("\r", "\n"):
                    line = _INPUT_BUFFER["text"].strip()
                    _INPUT_BUFFER["text"] = ""
                    if line:
                        loop.call_soon_threadsafe(queue.put_nowait, line)
                elif ch in ("\x7f", "\b"):
                    _INPUT_BUFFER["text"] = _INPUT_BUFFER["text"][:-1]
                elif ch == "\x03":  # Ctrl+C
                    loop.call_soon_threadsafe(queue.put_nowait, "quit")
                elif ch.isprintable():
                    _INPUT_BUFFER["text"] += ch
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    except ImportError:
        # Windows fallback: no live buffer editing, line by line via input().
        while not _STOP_EVENT.is_set():
            try:
                line = input()
            except EOFError:
                break
            line = line.strip()
            if line:
                loop.call_soon_threadsafe(queue.put_nowait, line)


async def _dashboard_loop(live):
    while not _STOP_EVENT.is_set():
        live.update(_build_layout(), refresh=True)
        await asyncio.sleep(5)


async def _command_processor(queue):
    while not _STOP_EVENT.is_set():
        try:
            cmd = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue

        cmd_lower = cmd.strip().lower()
        if cmd_lower == "quit":
            _log_feedback("Shutting down CIPHER...", style="bold red")
            _STOP_EVENT.set()
            break

        if cmd_lower == "help":
            _log_feedback(_HELP_TEXT, style=_ACCENT)
            continue

        cog = _get_cog()
        if cog is None:
            _log_feedback("[!] Bot is not ready yet (waiting for on_ready).", style="bold red")
            continue

        if cmd_lower in ("start", "stop", "pause", "resume"):
            _log_feedback(f"> {cmd_lower}")
            await _run_cog_command(cog, cmd_lower)
        elif cmd_lower == "status":
            _log_feedback("> status — check the tables above, they refresh every 5s")
        elif cmd_lower not in _VALID_COMMANDS:
            _log_feedback(f"[!] Unknown command: '{cmd}'. Type 'help'.", style="yellow")


async def run_console_ui():
    """Starts the Discord bot + the console dashboard together."""
    loop = asyncio.get_event_loop()
    command_queue = asyncio.Queue()

    reader_thread = threading.Thread(target=_stdin_reader, args=(loop, command_queue), daemon=True)
    reader_thread.start()

    _log_feedback("Logging in to Discord...", style=_ACCENT)
    bot_task = asyncio.create_task(bot.start(TOKEN))

    with Live(_build_layout(), console=_console, screen=True, auto_refresh=False) as live:
        dash_task = asyncio.create_task(_dashboard_loop(live))
        try:
            await _command_processor(command_queue)
        finally:
            _STOP_EVENT.set()
            dash_task.cancel()

    if not bot.is_closed():
        await bot.close()
    bot_task.cancel()
    try:
        await bot_task
    except (asyncio.CancelledError, Exception):
        pass


def _gather_dashboard_state():
    """Snapshot of everything the console/web dashboard needs, in one place
    so both UIs stay in sync."""
    cog = _get_cog()
    now = time.time()
    state = {
        "connected": cog is not None,
        "bot_uptime": str(timedelta(seconds=int(now - _BOT_START_TS))),
    }
    if cog is None:
        return state

    if cog.config.get("is_paused"):
        loop_status = "PAUSED"
    elif cog.farm_loop.is_running():
        loop_status = "RUNNING"
    else:
        loop_status = "STOPPED"

    channel_obj = bot.get_channel(CHANNEL_ID) if CHANNEL_ID else None
    if channel_obj:
        channel_display = f"#{channel_obj.name} ({CHANNEL_ID})"
    elif CHANNEL_ID:
        channel_display = f"unresolved (id={CHANNEL_ID})"
    else:
        channel_display = "NOT SET"

    farm_uptime = (
        str(timedelta(seconds=int(now - cog.config["stats_start_time"])))
        if cog.config.get("stats_start_time")
        else "0:00:00"
    )

    state.update({
        "loop_status": loop_status,
        "channel": channel_display,
        "farm_uptime": farm_uptime,
        "last_error": str(cog.config.get("last_error") or "NONE"),
        "watchdog": "ACTIVE" if cog.farm_loop_watchdog.is_running() else "INACTIVE",
        "next_plant": format_countdown(cog.config["last_plant_run"] + cog.config["plant_interval"], now) if cog.config["last_plant_run"] else "DUE / IMMEDIATE",
        "next_refine": format_countdown(cog.config["last_refine_run"] + cog.config["refine_interval"], now) if cog.config["last_refine_run"] else "DUE / IMMEDIATE",
        "next_hunt": format_countdown(cog.config["last_hunt_run"] + cog.config["hunt_interval"], now) if cog.config["last_hunt_run"] else "DUE / IMMEDIATE",
        "next_break": format_countdown(cog.config["last_break_time"] + cog.config["break_interval"], now) if cog.config["last_break_time"] else "PENDING",
        "stats": {
            "harvest": cog.config.get("stats_harvest_count", 0),
            "plant": cog.config.get("stats_plant_count", 0),
            "refine": cog.config.get("stats_refine_count", 0),
            "fight_horde": cog.config.get("stats_fight_horde_count", 0),
            "hunt": cog.config.get("stats_hunt_count", 0),
        },
        "slots": [
            {
                "slot": slot,
                "enabled": bool(cog.config.get(f"slot{slot}_enabled")),
                "command": cog.config.get(f"slot{slot}_command") or "",
                "prefix": cog.config.get(f"cmd_prefix_slot{slot}", "/"),
                "ok": cog.config.get(f"stats_slot{slot}_count", 0),
                "fail": cog.config.get(f"stats_slot{slot}_failures", 0),
            }
            for slot in MODULAR_SLOT_IDS
        ],
        "log": [{"ts": ts, "text": text} for ts, text, _style in list(_CONSOLE_LOG)[-30:]],
    })
    return state


# ---------------------------------------------------------------------------
# CIPHER — Web dashboard (for hosts like Railway with no interactive stdin)
# ---------------------------------------------------------------------------
import base64
from aiohttp import web

DASHBOARD_USER = os.getenv("DASHBOARD_USER", "admin")
DASHBOARD_PASS = os.getenv("DASHBOARD_PASS", "")


@web.middleware
async def _auth_middleware(request, handler):
    if not DASHBOARD_PASS:
        return await handler(request)
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Basic "):
        try:
            decoded = base64.b64decode(auth[6:]).decode()
            user, _, pwd = decoded.partition(":")
            if user == DASHBOARD_USER and pwd == DASHBOARD_PASS:
                return await handler(request)
        except Exception:
            pass
    return web.Response(
        status=401,
        headers={"WWW-Authenticate": 'Basic realm="CIPHER"'},
        text="Unauthorized",
    )


_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>CIPHER Console</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { --accent: #5865F2; --bg: #0f1117; --panel: #161a24; --text: #e6e6e6; }
  * { box-sizing: border-box; }
  body { margin:0; font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); padding: 24px; }
  h1 { text-align:center; color: var(--accent); letter-spacing: 6px; margin-bottom: 4px; }
  .subtitle { text-align:center; color:#888; margin-bottom: 24px; font-size: 13px; }
  .grid { display:grid; grid-template-columns: 1fr 2fr; gap: 16px; max-width: 1100px; margin: 0 auto; }
  .panel { background: var(--panel); border: 1px solid var(--accent); border-radius: 10px; padding: 16px; }
  .panel h2 { margin-top:0; color: var(--accent); font-size: 15px; text-transform: uppercase; letter-spacing: 1px; }
  table { width:100%; border-collapse: collapse; font-size: 13px; }
  td, th { padding: 6px 8px; border-bottom: 1px solid #262b38; text-align:left; }
  .ok { color:#3ddc84; } .bad { color:#ff5c5c; } .warn { color:#f5c542; } .dim { color:#666; }
  .buttons { display:flex; gap:8px; margin-top:16px; flex-wrap: wrap; }
  button { background: var(--accent); border:none; color:white; padding:8px 16px; border-radius:6px; cursor:pointer; font-weight:600; }
  button:hover { opacity:0.85; }
  .log { background:#0b0d13; border-radius:8px; padding:10px; font-family: monospace; font-size:12px; max-height:260px; overflow-y:auto; }
  .log div { margin-bottom:4px; }
  .full { grid-column: 1 / -1; }
</style>
</head>
<body>
  <h1>CIPHER</h1>
  <div class="subtitle">Discord Automation Suite — Control Panel</div>
  <div class="grid">
    <div class="panel">
      <h2>System</h2>
      <table id="system-table"></table>
      <div class="buttons">
        <button onclick="sendCmd('start')">Start</button>
        <button onclick="sendCmd('stop')">Stop</button>
        <button onclick="sendCmd('pause')">Pause</button>
        <button onclick="sendCmd('resume')">Resume</button>
      </div>
    </div>
    <div class="panel">
      <h2>Stats</h2>
      <table id="stats-table"></table>
    </div>
    <div class="panel full">
      <h2>Modular Slots (1-15)</h2>
      <table>
        <thead><tr><th>Slot</th><th>Status</th><th>Command</th><th>Prefix</th><th>OK</th><th>Fail</th></tr></thead>
        <tbody id="slots-body"></tbody>
      </table>
    </div>
    <div class="panel full">
      <h2>Console / Feedback</h2>
      <div class="log" id="log"></div>
    </div>
  </div>

<script>
async function sendCmd(cmd) {
  await fetch('/api/command', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({command: cmd})
  });
  refresh();
}

function row(label, value) {
  return `<tr><td>${label}</td><td>${value}</td></tr>`;
}

async function refresh() {
  const res = await fetch('/api/state');
  if (!res.ok) return;
  const s = await res.json();

  if (!s.connected) {
    document.getElementById('system-table').innerHTML = row('Status', '<span class="bad">NOT CONNECTED</span>');
    return;
  }

  const loopClass = s.loop_status === 'RUNNING' ? 'ok' : (s.loop_status === 'PAUSED' ? 'warn' : 'bad');
  document.getElementById('system-table').innerHTML =
    row('Loop', `<span class="${loopClass}">${s.loop_status}</span>`) +
    row('Bot Uptime', s.bot_uptime) +
    row('Farm Uptime', s.farm_uptime) +
    row('Target Channel', s.channel) +
    row('Watchdog', s.watchdog) +
    row('Next Plant', s.next_plant) +
    row('Next Refine', s.next_refine) +
    row('Next Hunt', s.next_hunt) +
    row('Next Break', s.next_break) +
    row('Last Error', s.last_error);

  document.getElementById('stats-table').innerHTML =
    row('Harvest', s.stats.harvest.toLocaleString()) +
    row('Plant', s.stats.plant.toLocaleString()) +
    row('Refine', s.stats.refine.toLocaleString()) +
    row('Fight Horde', s.stats.fight_horde.toLocaleString()) +
    row('Hunt', s.stats.hunt.toLocaleString());

  document.getElementById('slots-body').innerHTML = s.slots.map(sl => `
    <tr>
      <td>${sl.slot}</td>
      <td>${sl.enabled ? '<span class="ok">ON</span>' : '<span class="dim">OFF</span>'}</td>
      <td>${sl.command || '<span class="dim">(unassigned)</span>'}</td>
      <td>${sl.prefix}</td>
      <td>${sl.ok.toLocaleString()}</td>
      <td>${sl.fail ? '<span class="bad">' + sl.fail.toLocaleString() + '</span>' : '<span class="dim">0</span>'}</td>
    </tr>`).join('');

  document.getElementById('log').innerHTML = s.log.slice().reverse().map(
    l => `<div><span class="dim">${l.ts}</span> ${l.text}</div>`
  ).join('');
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


async def _web_index(request):
    return web.Response(text=_DASHBOARD_HTML, content_type="text/html")


async def _web_state(request):
    return web.json_response(_gather_dashboard_state())


async def _web_command(request):
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    cmd = str(data.get("command", "")).strip().lower()
    if cmd not in ("start", "stop", "pause", "resume"):
        return web.json_response({"ok": False, "error": f"unknown command '{cmd}'"}, status=400)

    cog = _get_cog()
    if cog is None:
        return web.json_response({"ok": False, "error": "bot not ready yet"}, status=503)

    _log_feedback(f"> {cmd} (web)")
    await _run_cog_command(cog, cmd)
    return web.json_response({"ok": True})


async def run_web_dashboard():
    """Starts the Discord bot + an HTTP dashboard (for Railway and similar
    hosts, where there's no real interactive terminal / stdin)."""
    app = web.Application(middlewares=[_auth_middleware])
    app.router.add_get("/", _web_index)
    app.router.add_get("/api/state", _web_state)
    app.router.add_post("/api/command", _web_command)

    port = int(os.getenv("PORT", "8080"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    logger.info(f"[WEB] CIPHER dashboard listening on 0.0.0.0:{port}")
    if not DASHBOARD_PASS:
        logger.warning(
            "[WEB] DASHBOARD_PASS is not set — the panel is PUBLIC to anyone with the URL. "
            "Set DASHBOARD_USER / DASHBOARD_PASS env vars to protect it."
        )

    _log_feedback("Logging in to Discord...", style=_ACCENT)
    bot_task = asyncio.create_task(bot.start(TOKEN))

    try:
        await bot_task
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()


# ---------------------------------------------------------------------------
# Existing bot startup
# ---------------------------------------------------------------------------

bot = commands.Bot(command_prefix="~~", self_bot=True, chunk_guilds_at_startup=False)

# Trivia listeners are registered separately so FarmingEngine's existing
# on_message listener remains untouched.
bot.add_listener(trivia_interaction_listener, "interaction")
bot.add_listener(trivia_modal_listener, "modal")
if not bot.get_command("trivia"):
    bot.add_command(trivia_status)

async def _ensure_trivia_workers():
    """Keep the configured Trivia worker pool alive without disturbing healthy workers."""
    global _TRIVIA_WORKER_TASKS

    healthy = [task for task in _TRIVIA_WORKER_TASKS if not task.done() and not task.cancelled()]
    if len(healthy) == _TRIVIA_WORKER_COUNT:
        _TRIVIA_WORKER_TASKS = healthy
        return

    # Keep healthy workers and replace only missing slots.
    _TRIVIA_WORKER_TASKS = healthy[:_TRIVIA_WORKER_COUNT]
    next_index = len(_TRIVIA_WORKER_TASKS) + 1
    while len(_TRIVIA_WORKER_TASKS) < _TRIVIA_WORKER_COUNT:
        worker_index = next_index
        task = asyncio.create_task(_trivia_worker(worker_index), name=f"trivia-worker-{worker_index}")

        def _worker_done(completed, index=worker_index):
            if completed.cancelled():
                return
            exc = completed.exception()
            if exc is not None:
                logger.error("[TRIVIA] Worker %s terminated unexpectedly: %s", index, exc, exc_info=exc)
                try:
                    asyncio.create_task(_ensure_trivia_workers())
                except RuntimeError:
                    # Event loop is already shutting down.
                    pass

        task.add_done_callback(_worker_done)
        _TRIVIA_WORKER_TASKS.append(task)
        next_index += 1


@bot.event
async def on_ready():
    if not bot.get_cog("FarmingEngine"):
        await bot.add_cog(FarmingEngine(bot))

    await _ensure_trivia_workers()

    logger.info(f"bot session safely initialized: {bot.user}")
    logger.info("[TRIVIA] Math trivia system active | CHANNEL_ID=%s | TARGET_BOT_ID=%s", CHANNEL_ID, TARGET_BOT_ID)

if __name__ == "__main__":
    if not TOKEN or not CHANNEL_ID or not TARGET_BOT_ID:
        logger.error("Environment file missing critical deployment requirements.")
    else:
        # Railway (and most host providers) inject a PORT env var for web
        # services and give you a log stream, not a real interactive
        # terminal — so stdin-based input doesn't work there. If PORT is
        # set, serve the HTTP dashboard instead of the local rich console.
        use_web = bool(os.getenv("PORT")) or bool(os.getenv("RAILWAY_ENVIRONMENT"))
        try:
            if use_web:
                asyncio.run(run_web_dashboard())
            else:
                asyncio.run(run_console_ui())
        except KeyboardInterrupt:
            pass
        finally:
            _STOP_EVENT.set()
            print("\nCIPHER stopped.")
