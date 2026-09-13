import os
import asyncio
import json
import logging
import random
import time
import discord
from logging.handlers import RotatingFileHandler
from discord.ext import commands, tasks
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Setup and Configuration Constants
# ---------------------------------------------------------------------------
load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
TARGET_BOT_ID = int(os.getenv("TARGET_BOT_ID", "0"))

STATE_FILE = "farm_state.json"
PROFILE_FILE = "farm_profiles.json"
TZ_GMT1 = timezone(timedelta(hours=1))

MESSAGE_CHUNK_LIMIT = 1900

logger = logging.getLogger("farm_selfbot")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
_console = logging.StreamHandler()
_console.setFormatter(_fmt)
_file = RotatingFileHandler("farm_selfbot.log", maxBytes=1_000_000, backupCount=3)
_file.setFormatter(_fmt)
logger.addHandler(_console)
logger.addHandler(_file)

MAX_BACKOFF = 60
MAX_RETRIES = 8
MAX_CRASHES_PER_WINDOW = 5
CRASH_WINDOW_SECONDS = 300

MODULAR_SLOT_IDS = (1, 2, 3, 4, 5)

DEFAULT_CONFIG = {
    "is_paused": False,
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
    "fight_horde_batch_size": 1,
    "fight_horde_batch_gap": 0.5,

    "hunt_enabled": False,
    "hunt_interval": 60,
    "last_hunt_run": 0.0,
    "stats_hunt_count": 0,
    "hunt_batch_size": 1,
    "hunt_batch_gap": 0.5,

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
    "hunt_interval": int,
    "hunt_batch_size": int,
    "hunt_batch_gap": float,
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
}

for _slot in MODULAR_SLOT_IDS:
    SETTABLE[f"slot{_slot}_command"] = str
    SETTABLE[f"slot{_slot}_param1_name"] = str
    SETTABLE[f"slot{_slot}_param1_value"] = str
    SETTABLE[f"slot{_slot}_param2_name"] = str
    SETTABLE[f"slot{_slot}_param2_value"] = str
    SETTABLE[f"slot{_slot}_param3_name"] = str
    SETTABLE[f"slot{_slot}_param3_value"] = str
    SETTABLE[f"slot{_slot}_interval"] = int

# All intervals updated to permit 1s minimum per user request
MIN_VALUES = {
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

    def __init__(self, bot):
        self.bot = bot
        self.cached_commands = {}
        self.cached_app_command_list = []
        self.active_sequence = "idle"
        # Tracks whether ~~stop was called deliberately, so the watchdog
        # below can tell "user wants it off" apart from "it silently died".
        self._user_stopped = False

        self.config = dict(DEFAULT_CONFIG)
        self.config["crash_times"] = list(DEFAULT_CONFIG["crash_times"])
        self.load_state_sync()

        self.STEP_SPECS = {
            "plant": lambda: {"material": self.config["plant_material"], "quantity": self.config["plant_quantity"]},
            "harvest_plots": lambda: {},
            "refine": lambda: {"recipe_id": self.config["refine_recipe_id"]},
            "fight_horde": lambda: {},
            "hunt": lambda: {},
        }

    async def cog_check(self, ctx):
        return ctx.author.id == self.bot.user.id

    async def cog_unload(self):
        logger.info("Emergency save protocol triggered via structural shutdown event.")
        await self.save_state()

    # -----------------------------------------------------------------------
    # Persistence
    # -----------------------------------------------------------------------
    def load_state_sync(self):
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
                self.config[key] = data[key]

            meta_keys = [k for k in DEFAULT_CONFIG if k not in SETTABLE and k != "crash_times"]
            for key in meta_keys:
                if key in data:
                    self.config[key] = data[key]

            if "crash_times" in data and isinstance(data["crash_times"], list):
                now = time.time()
                self.config["crash_times"] = [t for t in data["crash_times"] if now - t <= CRASH_WINDOW_SECONDS]
        except Exception as e:
            logger.error(f"Configuration engine encountered unreadable data structures: {e}")

    def _sync_write(self, payload):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.error(f"Threaded storage commit failure: {e}")

    async def save_state(self):
        await asyncio.to_thread(self._sync_write, dict(self.config))

    def _load_profiles_sync(self):
        if not os.path.exists(PROFILE_FILE):
            return {}
        try:
            with open(PROFILE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Profile store unreadable: {e}")
            return {}

    def _save_profiles_sync(self, profiles):
        try:
            with open(PROFILE_FILE, "w") as f:
                json.dump(profiles, f, indent=2)
        except Exception as e:
            logger.error(f"Profile store write failure: {e}")

    async def reply(self, ctx, title, description=None):
        content = f"**{title}**\n"
        if description:
            content += f"{description}\n"
        await ctx.send(content.strip())

    async def notify_channel(self, title, description=None):
        channel = self.bot.get_channel(CHANNEL_ID)
        if channel:
            try:
                await channel.send(f"**{title}**\n{description if description else ''}".strip())
            except Exception:
                pass

    async def get_commands(self, channel, force_refresh=False):
        if force_refresh or not self.cached_commands:
            try:
                app_commands = await channel.application_commands()
                app_commands = [c for c in app_commands if c.application_id == TARGET_BOT_ID]
                self.cached_app_command_list = app_commands
                self.cached_commands = {
                    "plant": discord.utils.get(app_commands, name="plant"),
                    "harvest_plots": discord.utils.get(app_commands, name="harvest_plots"),
                    "refine": discord.utils.get(app_commands, name="refine"),
                    "fight_horde": discord.utils.get(app_commands, name="fight_horde"),
                    "hunt": discord.utils.get(app_commands, name="hunt"),
                }
            except Exception as e:
                logger.error(f"Failed command cache matrix extraction: {e}")
        return self.cached_commands

    def resolve_dynamic_command(self, name):
        """Resolves slash commands, smoothly traversing `.children` for subcommands like 'sect essence_deposit'."""
        if not name:
            return None
        parts = name.strip().split()
        cmd = discord.utils.get(self.cached_app_command_list, name=parts[0])
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

    def build_slot_kwargs(self, slot_id):
        kwargs = {}
        for p in (1, 2, 3):
            pname = self.config.get(f"slot{slot_id}_param{p}_name", "").strip()
            if not pname:
                continue
            pvalue_raw = self.config.get(f"slot{slot_id}_param{p}_value", "")
            coerced = coerce_slot_value(pvalue_raw)
            if coerced is None:
                continue
            kwargs[pname] = coerced
        return kwargs

    async def safe_execute(self, func, *args, **kwargs):
        delay = 1
        for _ in range(MAX_RETRIES):
            try:
                await asyncio.sleep(random.uniform(0.1, 0.35))
                return await func(*args, **kwargs)
            except discord.HTTPException as e:
                if e.status == 429:
                    backoff_time = delay + random.uniform(0, delay * 0.25)
                    await asyncio.sleep(backoff_time)
                    delay = min(delay * 2, MAX_BACKOFF)
                else:
                    self.config["last_error"] = f"HTTP Error Status: {e.status}"
                    return None
            except Exception as e:
                self.config["last_error"] = str(e)
                return None
        return None

    async def run_step(self, channel, commands_dict, step_name):
        cmd = commands_dict.get(step_name)
        if not cmd:
            msg = f"Command `{step_name}` not found in cache (target bot may have changed its commands)."
            logger.warning(msg)
            self.config["last_error"] = msg
            return None

        kwargs = self.STEP_SPECS[step_name]()
        result = await self.safe_execute(cmd, channel, **kwargs)
        if result is not None:
            stat_target = self.STAT_KEYS.get(step_name)
            if stat_target:
                self.config[stat_target] += 1
        return result

    def _still_active(self):
        return self.farm_loop.is_running() and not self.config.get("is_paused", False)

    async def _jitter_sleep(self, base_gap):
        await asyncio.sleep(calculate_bell_curve_delay(base_gap, self.config["loop_jitter"]))

    def _retime(self):
        next_tick = calculate_bell_curve_delay(self.config["loop_interval"], self.config["loop_jitter"])
        self.farm_loop.change_interval(seconds=max(1.0, next_tick))

    def _log_progress_if_due(self):
        interval = self.config.get("log_summary_interval_loops", 0)
        n = self.config["stats_loops_completed"]
        if interval <= 0 or n == 0 or n % interval != 0:
            return
        uptime = (
            str(timedelta(seconds=int(time.time() - self.config["stats_start_time"])))
            if self.config["stats_start_time"]
            else "n/a"
        )
        logger.info(
            f"[Progress] loops={n:,} uptime={uptime} "
            f"refine_fail_streak={self.config['refine_consecutive_failures']} "
            f"last_error={self.config['last_error'] or 'none'}"
        )

    # -----------------------------------------------------------------------
    # Tick handlers
    # -----------------------------------------------------------------------
    async def _handle_sleep(self) -> bool:
        if not self.config.get("sleep_enabled", False):
            return False

        current_hour = datetime.now(TZ_GMT1).hour
        start_hour = self.config["sleep_start_hour"]
        wake_hour = (start_hour + self.config["sleep_duration_hours"]) % 24
        is_sleeping = (
            start_hour <= current_hour < wake_hour
            if start_hour < wake_hour
            else (current_hour >= start_hour or current_hour < wake_hour)
        )
        if not is_sleeping:
            return False

        self.farm_loop.change_interval(seconds=900.0)
        return True

    async def _handle_break(self, now) -> bool:
        if now - self.config["last_break_time"] < self.config["break_interval"]:
            return False

        actual_break = calculate_bell_curve_delay(self.config["break_duration"], self.config["loop_jitter"] * 12)
        await self.notify_channel("☕ Fatigue Protocol Engaged", f"Simulating human break sequence. Idling for {actual_break:.0f}s.")
        await asyncio.sleep(max(10, actual_break))

        self.config["stats_total_break_time"] += int(actual_break)
        self.config["last_break_time"] = time.time()
        await self.save_state()
        self._retime()
        return True

    async def _handle_planting(self, channel, commands_dict, now) -> bool:
        if not self.config["plant_enabled"]:
            return False

        eff_last_plant = self.config["last_plant_run"] or (now - self.config["plant_interval"])
        time_until_plant = self.config["plant_interval"] - (now - eff_last_plant)
        if time_until_plant > 5:
            return False
        if time_until_plant > 0:
            await asyncio.sleep(time_until_plant)

        self.active_sequence = "plant"
        try:
            if self.config.get("first_plant_done", False):
                await self.run_step(channel, commands_dict, "harvest_plots")
                await self._jitter_sleep(self.config["plant_step_gap"])

            for _ in range(self.config["plant_repeats"]):
                if not self._still_active():
                    return True
                await self.run_step(channel, commands_dict, "plant")
                await self._jitter_sleep(self.config["plant_step_gap"])

            self.config["first_plant_done"] = True
            self.config["last_plant_run"] = time.time()
            self.config["stats_loops_completed"] += 1
            await self.save_state()
            self._retime()
            self._log_progress_if_due()
            return True
        finally:
            self.active_sequence = "idle"

    async def _handle_refine(self, channel, commands_dict, now) -> bool:
        if not self.config["refine_enabled"]:
            return False

        eff_last_refine = self.config["last_refine_run"] or (now - self.config["refine_interval"])
        if now - eff_last_refine < self.config["refine_interval"]:
            return False

        self.active_sequence = "refine"
        try:
            if not self._still_active():
                return True

            result = await self.run_step(channel, commands_dict, "refine")
            self.config["last_refine_run"] = now

            if result is None:
                self.config["stats_refine_failures"] += 1
                self.config["refine_consecutive_failures"] += 1
                if self.config["refine_consecutive_failures"] >= self.config["refine_max_consecutive_failures"]:
                    self.config["refine_enabled"] = False
                    await self.notify_channel(
                        "⚠️ Refine Auto-Disabled",
                        f"`/refine` failed {self.config['refine_consecutive_failures']}x in a row. Check materials, then `~~toggle refine` to re-enable."
                    )
                    self.config["refine_consecutive_failures"] = 0
            else:
                self.config["refine_consecutive_failures"] = 0

            self.config["stats_loops_completed"] += 1
            await self.save_state()
            self._retime()
            self._log_progress_if_due()
            return True
        finally:
            self.active_sequence = "idle"

    # -----------------------------------------------------------------------
    # Interactive Component Logic (Fight Horde / Hunt)
    # -----------------------------------------------------------------------
    async def _collect_component_responses(self, check_msg, count, timeout) -> list:
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

        self.bot.add_listener(on_msg, name="on_message")
        self.bot.add_listener(on_edit, name="on_message_edit")
        try:
            collected = []
            seen_ids = set()
            deadline = time.time() + timeout
            while len(collected) < count:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                if msg.id in seen_ids:
                    continue
                seen_ids.add(msg.id)
                collected.append(msg)
            return collected
        finally:
            self.bot.remove_listener(on_msg, name="on_message")
            self.bot.remove_listener(on_edit, name="on_message_edit")

    async def _execute_and_click_first_button(self, channel, commands_dict, cmd_name, stat_key, last_run_key, now, batch_size=1, batch_gap_key=None) -> bool:
        cmd = commands_dict.get(cmd_name)
        if not cmd:
            cmd = self.resolve_dynamic_command(cmd_name)

        if not cmd:
            msg = f"Command `/{cmd_name}` not found in cache."
            logger.warning(msg)
            self.config["last_error"] = msg
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
            self.config[last_run_key] = time.time()
            self.config["stats_loops_completed"] += 1
            await self.save_state()
            self._retime()
            self._log_progress_if_due()
            return True

        batch_size = max(1, int(batch_size))
        gap = self.config.get(batch_gap_key, 0.5) if batch_gap_key else 0.5

        def check_msg(m):
            if m.author.id != TARGET_BOT_ID or m.channel.id != channel.id or not m.components:
                return False
            inter = getattr(m, 'interaction', None)
            if inter and getattr(inter, 'user', None):
                if inter.user.id != self.bot.user.id:
                    return False
            return True

        # BUGFIX: the collector is now armed BEFORE we fire anything, and
        # stays armed for the whole batch, so there's no window where a
        # fast response could arrive before we're listening for it.
        collect_timeout = min(90.0, 15.0 * batch_size)
        collector_task = asyncio.ensure_future(
            self._collect_component_responses(check_msg, batch_size, collect_timeout)
        )

        fired = 0
        for i in range(batch_size):
            if not self._still_active():
                break
            result = await self.safe_execute(cmd, channel)
            if result is not None:
                self.config[stat_key] += 1
                fired += 1
            if i < batch_size - 1:
                await self._jitter_sleep(gap)

        if fired == 0:
            collector_task.cancel()
            try:
                await collector_task
            except (asyncio.CancelledError, Exception):
                pass
            self.config[last_run_key] = time.time()
            self.config["stats_loops_completed"] += 1
            await self.save_state()
            self._retime()
            self._log_progress_if_due()
            return True

        collected = await collector_task

        if len(collected) < fired:
            self.config["last_error"] = (
                f"`/{cmd_name}` batch: only {len(collected)}/{fired} response(s) with buttons arrived in time "
                f"(the rest may have hit a cooldown or other rejection on the target bot)."
            )

        for idx, resp in enumerate(collected):
            if not self._still_active():
                break

            btn = None
            for row in resp.components:
                children = getattr(row, "children", None)
                candidates = children if children is not None else [row]
                for child in candidates:
                    if hasattr(child, 'click') and not getattr(child, 'disabled', False):
                        btn = child
                        break
                if btn:
                    break

            if btn:
                try:
                    await btn.click()
                except discord.HTTPException as e:
                    detail = getattr(e, "text", None) or str(e)
                    self.config["last_error"] = f"Failed clicking `{cmd_name}` button {idx + 1}/{len(collected)} (HTTP {e.status}): {detail}"
                except Exception as e:
                    self.config["last_error"] = f"Failed interacting with `{cmd_name}` button {idx + 1}/{len(collected)}: {e}"
            else:
                self.config["last_error"] = f"No clickable active buttons found in `{cmd_name}` response {idx + 1}/{len(collected)}."

            if idx < len(collected) - 1:
                await self._jitter_sleep(gap)

        self.config[last_run_key] = time.time()
        self.config["stats_loops_completed"] += 1
        await self.save_state()
        self._retime()
        self._log_progress_if_due()
        return True


    async def _handle_fight_horde(self, channel, commands_dict, now) -> bool:
        if not self.config.get("fight_horde_enabled", False):
            return False

        eff_last = self.config.get("last_fight_horde_run", 0.0) or (now - self.config["fight_horde_interval"])
        if now - eff_last < self.config["fight_horde_interval"]:
            return False

        self.active_sequence = "fight_horde"
        try:
            return await self._execute_and_click_first_button(
                channel, commands_dict, "fight_horde", "stats_fight_horde_count", "last_fight_horde_run", now,
                batch_size=self.config.get("fight_horde_batch_size", 1),
                batch_gap_key="fight_horde_batch_gap",
            )
        finally:
            self.active_sequence = "idle"

    async def _handle_hunt(self, channel, commands_dict, now) -> bool:
        if not self.config.get("hunt_enabled", False):
            return False

        eff_last = self.config.get("last_hunt_run", 0.0) or (now - self.config["hunt_interval"])
        if now - eff_last < self.config["hunt_interval"]:
            return False

        self.active_sequence = "hunt"
        try:
            return await self._execute_and_click_first_button(
                channel, commands_dict, "hunt", "stats_hunt_count", "last_hunt_run", now,
                batch_size=self.config.get("hunt_batch_size", 1),
                batch_gap_key="hunt_batch_gap",
            )
        finally:
            self.active_sequence = "idle"

    # -----------------------------------------------------------------------
    # Chain & Mod Slots
    # -----------------------------------------------------------------------
    async def _handle_modular_chain(self, channel, now) -> bool:
        if not self.config.get("modular_chain_enabled", False):
            return False

        eff_last = self.config.get("last_modular_chain_run", 0.0) or (now - self.config["modular_chain_interval"])
        if now - eff_last < self.config["modular_chain_interval"]:
            return False

        self.active_sequence = "modular_chain"
        try:
            chain_seq_str = self.config.get("modular_chain_sequence", "")
            chain_slots = [int(x.strip()) for x in chain_seq_str.split(",") if x.strip().isdigit()]

            ran_any = False
            for slot_id in chain_slots:
                if slot_id not in MODULAR_SLOT_IDS:
                    continue
                # Purposely IGNORING f"slot{slot_id}_enabled" here so the chain can execute standalone chained commands.
                    
                cmd_name = self.config.get(f"slot{slot_id}_command", "").strip()
                if not cmd_name:
                    continue

                if ran_any:
                    await self._jitter_sleep(self.config["modular_chain_step_gap"])
                if not self._still_active():
                    return True

                cmd = self.resolve_dynamic_command(cmd_name)
                kwargs = self.build_slot_kwargs(slot_id)
                
                if not cmd:
                    msg = f"Modular chain (slot {slot_id}): command `/{cmd_name}` not found in cache."
                    logger.warning(msg)
                    self.config["last_error"] = msg
                    self.config[f"stats_slot{slot_id}_failures"] += 1
                else:
                    result = await self.safe_execute(cmd, channel, **kwargs)
                    if result is not None:
                        self.config[f"stats_slot{slot_id}_count"] += 1
                    else:
                        self.config[f"stats_slot{slot_id}_failures"] += 1
                ran_any = True

            self.config["last_modular_chain_run"] = now
            if ran_any:
                self.config["stats_loops_completed"] += 1
                await self.save_state()
                self._retime()
                self._log_progress_if_due()
                return True
            return False
        finally:
            self.active_sequence = "idle"

    async def _handle_slot(self, channel, slot_id, now) -> bool:
        enabled_key = f"slot{slot_id}_enabled"
        if not self.config.get(enabled_key, False):
            return False

        command_name = self.config.get(f"slot{slot_id}_command", "").strip()
        if not command_name:
            return False

        interval_key = f"slot{slot_id}_interval"
        last_run_key = f"last_slot{slot_id}_run"
        eff_last = self.config[last_run_key] or (now - self.config[interval_key])
        if now - eff_last < self.config[interval_key]:
            return False

        self.active_sequence = f"slot{slot_id}"
        try:
            cmd = self.resolve_dynamic_command(command_name)
            kwargs = self.build_slot_kwargs(slot_id)

            if not cmd:
                msg = f"Modular slot {slot_id}: command `/{command_name}` not found in cache."
                logger.warning(msg)
                self.config["last_error"] = msg
                self.config[f"stats_slot{slot_id}_failures"] += 1
            else:
                result = await self.safe_execute(cmd, channel, **kwargs)
                if result is not None:
                    self.config[f"stats_slot{slot_id}_count"] += 1
                else:
                    self.config[f"stats_slot{slot_id}_failures"] += 1

            self.config[last_run_key] = now
            self.config["stats_loops_completed"] += 1
            await self.save_state()
            self._retime()
            self._log_progress_if_due()
            return True
        finally:
            self.active_sequence = "idle"

    # -----------------------------------------------------------------------
    # Main loop
    # -----------------------------------------------------------------------
    @tasks.loop(seconds=DEFAULT_CONFIG["loop_interval"])
    async def farm_loop(self):
        try:
            if self.config.get("is_paused", False):
                self.farm_loop.change_interval(seconds=5.0)
                return

            channel = self.bot.get_channel(CHANNEL_ID)
            if not channel:
                return

            commands_dict = await self.get_commands(channel)
            now = time.time()

            if self.config["last_break_time"] == 0:
                self.config["last_break_time"] = now
            if self.config["stats_start_time"] == 0:
                self.config["stats_start_time"] = now
                await self.save_state()

            if await self._handle_sleep(): return
            if await self._handle_break(now): return
            if await self._handle_planting(channel, commands_dict, now): return
            if await self._handle_refine(channel, commands_dict, now): return
            if await self._handle_fight_horde(channel, commands_dict, now): return
            if await self._handle_hunt(channel, commands_dict, now): return
            if await self._handle_modular_chain(channel, now): return

            chain_seq_str = self.config.get("modular_chain_sequence", "")
            chain_slots = [int(x.strip()) for x in chain_seq_str.split(",") if x.strip().isdigit()] if self.config.get("modular_chain_enabled", False) else []

            for _slot in MODULAR_SLOT_IDS:
                if _slot in chain_slots:
                    continue
                if await self._handle_slot(channel, _slot, now):
                    return

            self.config["stats_loops_completed"] += 1
            self._retime()
            self._log_progress_if_due()

        except Exception as error:
            logger.error(f"Loop runtime mismatch: {error}")
            self.config["last_error"] = f"Crash Tracked: {error}"

            now = time.time()
            self.config["crash_times"].append(now)
            while self.config["crash_times"] and now - self.config["crash_times"][0] > CRASH_WINDOW_SECONDS:
                self.config["crash_times"].pop(0)

            await self.save_state()

            if len(self.config["crash_times"]) > MAX_CRASHES_PER_WINDOW:
                self.farm_loop.stop()
                await self.notify_channel("🛑 Structural Safety Lockdown", "Script failure rate exceeded safety thresholds. Shutting down.")
                return

            self.farm_loop.change_interval(seconds=5.0)

    @tasks.loop(seconds=DEFAULT_CONFIG["command_refresh_interval"])
    async def command_refresh_loop(self):
        try:
            channel = self.bot.get_channel(CHANNEL_ID)
            if channel:
                await self.get_commands(channel, force_refresh=True)
        except Exception as e:
            logger.error(f"Command refresh error handled gracefully: {e}")

    @tasks.loop(seconds=60)
    async def farm_loop_watchdog(self):
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
            if self._user_stopped:
                return
            if self.farm_loop.is_running():
                return

            logger.error("Watchdog: farm_loop was found stopped without an explicit ~~stop. Restarting.")
            self.farm_loop.change_interval(seconds=self.config["loop_interval"])
            self.farm_loop.start()
            await self.notify_channel(
                "🔁 Watchdog Auto-Restart",
                "The farming loop stopped unexpectedly (not via `~~stop`) and has been automatically restarted. "
                "Check `~~status` for `Latest Exceptions` if this keeps happening.",
            )
        except Exception as e:
            logger.error(f"farm_loop_watchdog error handled gracefully: {e}")

    @tasks.loop(seconds=DEFAULT_CONFIG["hq_gather_interval"])
    async def hq_gather_loop(self):
        try:
            if not self.config.get("hq_gather_enabled", False):
                return
            if self.config.get("is_paused", False):
                return
            if self.active_sequence != "idle":
                # Wait till core sequences are totally finished before barking gathers to avoid button overrides or overlaps
                return

            target_id = self.config.get("hq_gather_channel_id") or CHANNEL_ID
            channel = self.bot.get_channel(target_id)
            if not channel:
                msg = f"hq-gather target channel {target_id} not found/accessible."
                if self.config.get("last_error") != msg:
                    logger.warning(msg)
                    self.config["last_error"] = msg
                return

            result = await self.safe_execute(channel.send, self.config["hq_gather_message"])
            if result is not None:
                self.config["stats_hq_gather_count"] += 1
                self.config["last_hq_gather_time"] = time.time()
                await self.save_state()
        except Exception as e:
            logger.error(f"hq_gather_loop error handled gracefully: {e}")

    async def _apply_hq_gather_state(self, enabled: bool):
        self.config["hq_gather_enabled"] = enabled
        if enabled:
            self.hq_gather_loop.change_interval(seconds=self.config["hq_gather_interval"])
            if not self.hq_gather_loop.is_running():
                self.hq_gather_loop.start()
        else:
            if self.hq_gather_loop.is_running():
                self.hq_gather_loop.stop()

    @tasks.loop(seconds=30)
    async def voice_watch_loop(self):
        if not self.config.get("voice_enabled", False):
            return
        channel_id = self.config.get("voice_channel_id", 0)
        if not channel_id:
            return

        try:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                msg = f"Voice target channel {channel_id} not found/accessible."
                if self.config.get("last_error") != msg:
                    logger.warning(msg)
                    self.config["last_error"] = msg
                return

            guild = channel.guild
            bot_member = guild.me
            if bot_member.voice and bot_member.voice.channel and bot_member.voice.channel.id == channel_id:
                return

            await guild.change_voice_state(channel=channel, self_mute=True, self_deaf=True)
            logger.info(f"Voice watchdog (re)connected to channel {channel_id} via WebSocket.")
        except Exception as e:
            logger.error(f"voice_watch_loop error handled gracefully: {e}")

    async def _apply_voice_state(self, enabled: bool):
        self.config["voice_enabled"] = enabled
        if enabled:
            if not self.voice_watch_loop.is_running():
                self.voice_watch_loop.start()
        else:
            if self.voice_watch_loop.is_running():
                self.voice_watch_loop.stop()

    # -----------------------------------------------------------------------
    # Commands
    # -----------------------------------------------------------------------
    @commands.command(name="set")
    async def set_config(self, ctx, key: str, *, value: str):
        if key not in SETTABLE:
            await self.reply(ctx, "❌ Rejection", f"`{key}` is not a valid parameter. Type `~~settings` to view registry.")
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
            await self.reply(ctx, "❌ Type Cast Error", f"Cannot convert input to type `{caster.__name__}`.")
            return

        if key in MIN_VALUES and parsed < MIN_VALUES[key]:
            await self.reply(ctx, "❌ Boundary Error", f"Value is lower than minimum permitted boundary ({MIN_VALUES[key]}).")
            return

        old_val = self.config[key]
        self.config[key] = parsed
        await self.save_state()

        if key in ("sleep_enabled", "sleep_start_hour", "sleep_duration_hours") and self.farm_loop.is_running():
            self._retime()

        await self.reply(ctx, "⚙️ Parameter Logged", f"Successfully mutated parameter `{key}`:\n**{old_val}** ➔ **{parsed}**")

    @commands.command()
    async def settings(self, ctx):
        groups = {
            "⏰ Core Loop & Latency Profile": ["loop_interval", "loop_jitter", "log_summary_interval_loops"],
            "🌾 Agricultural Cultivation Matrix": ["plant_interval", "plant_step_gap", "plant_repeats", "plant_material", "plant_quantity"],
            "🧬 Refinement Sequence": ["refine_interval", "refine_recipe_id", "refine_max_consecutive_failures"],
            "⚔️ Combat & Hunt Protocols": ["fight_horde_interval", "fight_horde_batch_size", "fight_horde_batch_gap", "hunt_interval", "hunt_batch_size", "hunt_batch_gap"],
            "⛓️ Modular Chain Configuration": ["modular_chain_interval", "modular_chain_step_gap", "modular_chain_sequence"],
            "📢 HQ Gather Announcements": ["hq_gather_message", "hq_gather_interval", "hq_gather_channel_id"],
            **{
                f"🧩 Modular Slot {_slot}": [
                    f"slot{_slot}_command", f"slot{_slot}_param1_name", f"slot{_slot}_param1_value",
                    f"slot{_slot}_param2_name", f"slot{_slot}_param2_value",
                    f"slot{_slot}_param3_name", f"slot{_slot}_param3_value",
                    f"slot{_slot}_interval",
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
                curr = self.config[k]
                df = DEFAULT_CONFIG[k]
                mn = MIN_VALUES.get(k, "N/A")
                desc = DESCRIPTIONS.get(k, "No description available.")
                block += f"* `{k}`\n  ↳ **Current:** `{curr}` | **Default:** `{df}` | **Min Bound:** `{mn}`\n  ↳ *Info:* {desc}\n"
            blocks.append(block)

        for chunk in chunk_blocks(blocks):
            await ctx.send(chunk)

    @commands.command(name="toggle")
    async def toggle(self, ctx, target: str = None):
        """Toggle plant / refine / fight_horde / hunt / sequences / modular_chain on or off. Usage: ~~toggle <target>"""
        if not target or target.lower() not in TOGGLE_TARGETS:
            valid = ", ".join(f"`{t}`" for t in TOGGLE_TARGETS)
            await self.reply(ctx, "❌ Rejection", f"Specify a valid target: {valid}.\nUsage: `~~toggle <target>`")
            return

        key, last_run_key, interval_key, emoji, label = TOGGLE_TARGETS[target.lower()]
        new_state = not self.config[key]
        self.config[key] = new_state

        note = None
        if new_state and last_run_key and interval_key:
            self.config[last_run_key] = time.time() - self.config[interval_key]
            note = "Queued for immediate execution."
            if key == "refine_enabled":
                self.config["refine_consecutive_failures"] = 0

        await self.save_state()
        await ctx.message.add_reaction(emoji if new_state else "❌")
        await self.reply(
            ctx,
            f"{emoji if new_state else '❌'} {label} {'enabled' if new_state else 'disabled'}.",
            note,
        )

    @commands.command(name="preview")
    async def preview(self, ctx, target: str = None):
        """Show what a sequence would send without actually dispatching it."""
        t = (target or "").lower()

        if t in MODULAR_SLOT_NAMES:
            slot_id = int(t[len("slot"):])
            command_name = self.config.get(f"slot{slot_id}_command", "").strip()
            if not command_name:
                await self.reply(ctx, f"🔍 Preview: Modular Slot {slot_id}", "Not configured yet. Set a command with `~~set slot{}_command <name>`.".format(slot_id))
                return
            cmd = self.resolve_dynamic_command(command_name)
            cmd_status = "cached ✅" if cmd else "NOT CACHED ⚠️ (check spelling/subcommands, or run `~~start`)"
            kwargs = self.build_slot_kwargs(slot_id)

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
            await self.reply(ctx, "❌ Rejection", f"Specify a valid target: {valid}.\nUsage: `~~preview <target>`")
            return

        step = t
        cmd = self.cached_commands.get(step)
        cmd_status = "cached ✅" if cmd else "NOT CACHED ⚠️ (run `~~start` or wait for the command refresh)"
        kwargs = self.STEP_SPECS[step]()

        lines = [f"**🔍 Preview: `/{step}`**", f"Command resolved: {cmd_status}"]
        if kwargs:
            for k, v in kwargs.items():
                lines.append(f"`{k}` = `{v}`")
        else:
            lines.append("(no parameters)")
        await ctx.send("\n".join(lines))

    @commands.command(name="profile")
    async def profile(self, ctx, action: str = None, name: str = None):
        """Save/load/list/delete a named configuration snapshot."""
        action = (action or "").lower()
        if action not in ("save", "load", "list", "delete") or (action != "list" and not name):
            await self.reply(ctx, "❌ Rejection", "Usage: `~~profile save <name>` | `~~profile load <name>` | `~~profile list` | `~~profile delete <name>`")
            return

        profiles = await asyncio.to_thread(self._load_profiles_sync)

        if action == "list":
            if not profiles:
                await self.reply(ctx, "📁 Profiles", "No saved profiles yet.")
            else:
                await self.reply(ctx, "📁 Saved Profiles", "\n".join(f"`{n}`" for n in profiles))
            return

        if action == "save":
            snapshot = {k: self.config[k] for k in SETTABLE}
            snapshot.update({t[0]: self.config[t[0]] for t in TOGGLE_TARGETS.values()})
            snapshot["hq_gather_enabled"] = self.config["hq_gather_enabled"]
            profiles[name] = snapshot
            await asyncio.to_thread(self._save_profiles_sync, profiles)
            await self.reply(ctx, "💾 Profile Saved", f"Current configuration saved as `{name}`.")
            return

        if action == "delete":
            if name not in profiles:
                await self.reply(ctx, "❌ Not Found", f"No profile named `{name}`.")
                return
            del profiles[name]
            await asyncio.to_thread(self._save_profiles_sync, profiles)
            await self.reply(ctx, "🗑️ Profile Deleted", f"Removed `{name}`.")
            return

        if name not in profiles:
            await self.reply(ctx, "❌ Not Found", f"No profile named `{name}`.")
            return
        toggle_keys = {t[0] for t in TOGGLE_TARGETS.values()}
        skipped = []
        hq_gather_state = None
        for k, v in profiles[name].items():
            if k == "hq_gather_enabled" and isinstance(v, bool):
                hq_gather_state = v
            elif k in SETTABLE:
                if is_valid_settable(k, v):
                    self.config[k] = v
                else:
                    skipped.append(k)
            elif k in toggle_keys and isinstance(v, bool):
                self.config[k] = v
        if hq_gather_state is not None:
            await self._apply_hq_gather_state(hq_gather_state)
        await self.save_state()
        note = f"Configuration `{name}` applied."
        if skipped:
            note += f"\n⚠️ Skipped invalid values for: {', '.join(skipped)}."
        await self.reply(ctx, "📂 Profile Loaded", note)

    @commands.command(name="hq-gather")
    async def hq_gather_cmd(self, ctx, channel: discord.TextChannel = None):
        """Toggle a repeating plain-text message on/off."""
        if channel is not None:
            self.config["hq_gather_channel_id"] = channel.id

        new_state = not self.hq_gather_loop.is_running()
        await self._apply_hq_gather_state(new_state)
        await self.save_state()

        target_id = self.config.get("hq_gather_channel_id") or CHANNEL_ID
        target_channel = self.bot.get_channel(target_id)
        target_desc = target_channel.mention if target_channel else f"channel `{target_id}` (not found yet)"

        if new_state:
            await ctx.message.add_reaction("📢")
            await self.reply(
                ctx,
                "📢 HQ Gather Enabled",
                f"Sending `{self.config['hq_gather_message']}` every {self.config['hq_gather_interval']}s to {target_desc}.\n"
                f"Auto-paused while sequences are running to prevent interruption issues.",
            )
        else:
            await ctx.message.add_reaction("❌")
            await self.reply(ctx, "❌ HQ Gather Disabled", "Repeating message stopped.")

    @commands.command(name="jvc")
    async def jvc(self, ctx, channel_id: int = None):
        """Join and stay connected to a voice channel by ID until ~~lvc is used."""
        if channel_id is None:
            await self.reply(ctx, "❌ Rejection", "Usage: `~~jvc <voice_channel_id>`")
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            await self.reply(ctx, "❌ Not Found", f"Channel `{channel_id}` not found or not accessible from this account's cache.")
            return
        if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
            await self.reply(ctx, "❌ Invalid Channel", f"`{channel_id}` is a `{type(channel).__name__}`, not a voice or stage channel.")
            return

        self.config["voice_channel_id"] = channel_id
        await self._apply_voice_state(True)
        await self.save_state()

        try:
            await channel.guild.change_voice_state(channel=channel, self_mute=True, self_deaf=True)
            await ctx.message.add_reaction("🔊")
            await self.reply(
                ctx,
                "🔊 Voice Joined",
                f"Connected to `{channel.name}` (`{channel_id}`) via WebSocket protocol. Staying connected until `~~lvc`; "
                f"a watchdog will auto-reconnect on drops.",
            )
        except Exception as e:
            await self.reply(ctx, "❌ Connection Failed", str(e))

    @commands.command(name="lvc")
    async def lvc(self, ctx):
        """Leave the current voice channel and stop the auto-reconnect watchdog."""
        await self._apply_voice_state(False)
        channel_id = self.config.get("voice_channel_id", 0)
        self.config["voice_channel_id"] = 0
        await self.save_state()

        disconnected = False
        target_channel = self.bot.get_channel(channel_id) if channel_id else None
        guild = target_channel.guild if target_channel else ctx.guild
        if guild:
            try:
                await guild.change_voice_state(channel=None)
                disconnected = True
            except Exception as e:
                logger.error(f"Error disconnecting voice state: {e}")

        await ctx.message.add_reaction("👋")
        await self.reply(
            ctx,
            "👋 Voice Left",
            "Disconnected and watchdog stopped." if disconnected else "Watchdog stopped (no active connection found).",
        )

    @commands.command()
    async def reset_stats(self, ctx):
        self.config["stats_start_time"] = time.time()
        self.config["stats_plant_count"] = 0
        self.config["stats_harvest_count"] = 0
        self.config["stats_refine_count"] = 0
        self.config["stats_refine_failures"] = 0
        self.config["stats_fight_horde_count"] = 0
        self.config["stats_hunt_count"] = 0
        self.config["stats_hq_gather_count"] = 0
        for _slot in MODULAR_SLOT_IDS:
            self.config[f"stats_slot{_slot}_count"] = 0
            self.config[f"stats_slot{_slot}_failures"] = 0
        self.config["stats_loops_completed"] = 0
        self.config["stats_total_break_time"] = 0
        self.config["first_plant_done"] = False
        await self.save_state()
        await ctx.message.add_reaction("🧹")
        await self.reply(ctx, "🧹 Session Performance Metrics Cleared")

    @commands.command()
    async def start(self, ctx):
        if self.farm_loop.is_running():
            if self.config["is_paused"]:
                self.config["is_paused"] = False
                await self.save_state()
                await ctx.message.add_reaction("▶️")
                await self.reply(ctx, "▶️ Resumed", "Automation unpaused; loop operations restored.")
            else:
                await self.reply(ctx, "⚠️ Status Alert", "Automation cores already online.")
            return

        now = time.time()
        self.config["last_plant_run"] = now - self.config["plant_interval"]
        self.config["last_refine_run"] = now - self.config["refine_interval"]
        self.config["last_fight_horde_run"] = now - self.config["fight_horde_interval"]
        self.config["last_hunt_run"] = now - self.config["hunt_interval"]
        self.config["last_modular_chain_run"] = now - self.config.get("modular_chain_interval", 300)
        self.config["first_plant_done"] = False
        self.config["is_paused"] = False

        if self.config["last_break_time"] == 0:
            self.config["last_break_time"] = now
        if self.config["stats_start_time"] == 0:
            self.config["stats_start_time"] = now
        await self.save_state()

        self.farm_loop.change_interval(seconds=self.config["loop_interval"])
        self.farm_loop.start()
        self._user_stopped = False
        if not self.command_refresh_loop.is_running():
            self.command_refresh_loop.change_interval(seconds=self.config["command_refresh_interval"])
            self.command_refresh_loop.start()
        if not self.farm_loop_watchdog.is_running():
            self.farm_loop_watchdog.start()

        missing = []
        channel = self.bot.get_channel(CHANNEL_ID)
        if channel:
            commands_dict = await self.get_commands(channel, force_refresh=True)
            if self.config["plant_enabled"] and (not commands_dict.get("plant") or not commands_dict.get("harvest_plots")):
                missing.append("plant (plant/harvest_plots)")
            if self.config["refine_enabled"] and not commands_dict.get("refine"):
                missing.append("refine")
            if self.config["fight_horde_enabled"] and not commands_dict.get("fight_horde"):
                missing.append("fight_horde")
            if self.config["hunt_enabled"] and not commands_dict.get("hunt"):
                missing.append("hunt")
            
            # Use dynamic resolution to check slots so we properly parse nested subcommands
            for _slot in MODULAR_SLOT_IDS:
                if not self.config.get(f"slot{_slot}_enabled"):
                    continue
                _name = self.config.get(f"slot{_slot}_command", "").strip()
                if not _name:
                    missing.append(f"slot{_slot} (no command name configured)")
                elif not self.resolve_dynamic_command(_name):
                    missing.append(f"slot{_slot} (/{_name})")

        msg = "Farming routines armed. Sequences primed to execute immediately."
        if missing:
            msg += f"\n⚠️ Missing commands for: {', '.join(missing)}. Those sequences will silently no-op until resolved."

        await ctx.message.add_reaction("✅" if not missing else "⚠️")
        await self.reply(ctx, "✅ System Initialized", msg)

    @commands.command()
    async def stop(self, ctx):
        if not self.farm_loop.is_running():
            return
        self.farm_loop.stop()
        self.config["is_paused"] = False
        self._user_stopped = True
        await self.save_state()
        await ctx.message.add_reaction("🛑")
        await self.reply(ctx, "🛑 Automation Suspended", "Loop system shutdown fully. Use `~~start` to reinitialize.")

    @commands.command()
    async def pause(self, ctx):
        if not self.farm_loop.is_running():
            await self.reply(ctx, "❌ Action Denied", "System loop is not currently active. Run `~~start` first.")
            return
        if self.config["is_paused"]:
            await self.reply(ctx, "⚠️ Notice", "Automation system is already paused.")
            return
        self.config["is_paused"] = True
        await self.save_state()
        await ctx.message.add_reaction("⏸️")
        await self.reply(ctx, "⏸️ Automation Paused", "Operations frozen in place. Use `~~resume` to unfreeze.")

    @commands.command()
    async def resume(self, ctx):
        if not self.farm_loop.is_running():
            await self.reply(ctx, "❌ Action Denied", "System loop thread is dead. Use `~~start` to boot up.")
            return
        if not self.config["is_paused"]:
            await self.reply(ctx, "⚠️ Notice", "System is already running and processing actively.")
            return
        self.config["is_paused"] = False
        await self.save_state()
        self.farm_loop.change_interval(seconds=self.config["loop_interval"])
        await ctx.message.add_reaction("▶️")
        await self.reply(ctx, "▶️ Operations Resumed", "Farming threads active again.")

    @commands.command()
    async def status(self, ctx):
        loop_status = "PAUSED" if self.config.get("is_paused", False) else ("RUNNING" if self.farm_loop.is_running() else "STOPPED")
        plant_status = "ENABLED" if self.config["plant_enabled"] else "DISABLED"
        refine_status = "ENABLED" if self.config["refine_enabled"] else "DISABLED"
        fight_horde_status = "ENABLED" if self.config["fight_horde_enabled"] else "DISABLED"
        hunt_status = "ENABLED" if self.config["hunt_enabled"] else "DISABLED"
        chain_status = "ENABLED" if self.config.get("modular_chain_enabled") else "DISABLED"
        sleep_status = "ACTIVE" if self.config["sleep_enabled"] else "DISABLED"
        hq_gather_status = "RUNNING" if self.hq_gather_loop.is_running() else "STOPPED"

        voice_channel_id = self.config.get("voice_channel_id", 0)
        voice_channel_obj = self.bot.get_channel(voice_channel_id) if voice_channel_id else None
        if voice_channel_obj:
            guild_me = voice_channel_obj.guild.me if voice_channel_obj.guild else None
            if guild_me and guild_me.voice and guild_me.voice.channel and guild_me.voice.channel.id == voice_channel_id:
                voice_status = f"CONNECTED ({voice_channel_obj.name})"
            elif self.voice_watch_loop.is_running():
                voice_status = "RECONNECTING (watchdog active)"
            else:
                voice_status = "DISCONNECTED"
        else:
            voice_status = "DISCONNECTED"

        now = time.time()

        plant_countdown = format_countdown(self.config["last_plant_run"] + self.config["plant_interval"], now) if self.config["last_plant_run"] else "DUE / IMMEDIATE"
        refine_countdown = format_countdown(self.config["last_refine_run"] + self.config["refine_interval"], now) if self.config["last_refine_run"] else "DUE / IMMEDIATE"
        hunt_countdown = format_countdown(self.config["last_hunt_run"] + self.config["hunt_interval"], now) if self.config["last_hunt_run"] else "DUE / IMMEDIATE"
        break_countdown = format_countdown(self.config["last_break_time"] + self.config["break_interval"], now) if self.config["last_break_time"] else "PENDING"
        uptime_str = str(timedelta(seconds=int(now - self.config["stats_start_time"]))) if self.config["stats_start_time"] else "0:00:00"
        current_seq = self.config.get('modular_chain_sequence', '1,2,3')

        dashboard = (
            f"### 📊 FARMING AUTOMATION DASHBOARD (GMT+1)\n"
            f"```ini\n"
            f"[System Status]\n"
            f"Loop Status      = {loop_status}\n"
            f"Watchdog         = {'ACTIVE' if self.farm_loop_watchdog.is_running() else 'INACTIVE'}\n"
            f"Latency Profiler = Gaussian Normal Curve (±{self.config['loop_jitter']}s Jitter)\n"
            f"Plant Sequence   = {plant_status}\n"
            f"Refine Sequence  = {refine_status} (no jitter, fail streak={self.config['refine_consecutive_failures']})\n"
            f"Fight Horde      = {fight_horde_status}\n"
            f"Hunt Sequence    = {hunt_status}\n"
            f"Modular Chain    = {chain_status} (interval={self.config['modular_chain_interval']}s, sequence=[{current_seq}])\n"
            f"HQ Gather        = {hq_gather_status} (blocked during running sequences, currently={self.active_sequence})\n"
            f"Voice Channel    = {voice_status}\n"
            f"Nocturnal Sleep  = {sleep_status}\n"
            f"Next Plant Run   = {plant_countdown}\n"
            f"Next Refine Run  = {refine_countdown}\n"
            f"Next Hunt Run    = {hunt_countdown}\n"
            f"Next Human Break = {break_countdown}\n\n"
            f"[Farming Configurations]\n"
            f"Base Cycle Time  = {self.config['loop_interval']}s\n"
            f"Plant Targeting  = {self.config['plant_material']} (x{self.config['plant_quantity']})\n"
            f"Refine Targeting = {self.config['refine_recipe_id']} (every {self.config['refine_interval']}s)\n\n"
            f"[Modular Slots]\n"
            + "".join(
                f"Slot {_slot}            = "
                f"{'ENABLED' if self.config.get(f'slot{_slot}_enabled') else 'DISABLED'}"
                f" → /{self.config.get(f'slot{_slot}_command') or '(unset)'}"
                f" (ok={self.config.get(f'stats_slot{_slot}_count', 0):,}, fail={self.config.get(f'stats_slot{_slot}_failures', 0):,})\n"
                for _slot in MODULAR_SLOT_IDS
            )
            + "\n"
            f"[Session Metrics Log]\n"
            f"Active Uptime    = {uptime_str}\n"
            f"Loops Completed  = {self.config['stats_loops_completed']:,}\n"
            f"Dispatched /harvest={self.config['stats_harvest_count']:,}\n"
            f"Dispatched /plant= {self.config['stats_plant_count']:,}\n"
            f"Dispatched /refine={self.config['stats_refine_count']:,} (failures={self.config['stats_refine_failures']:,})\n"
            f"Dispatched /fight= {self.config['stats_fight_horde_count']:,}\n"
            f"Dispatched /hunt = {self.config['stats_hunt_count']:,}\n"
            f"HQ Gather Sent   = {self.config['stats_hq_gather_count']:,}\n"
            f"Break Downtime   = {str(timedelta(seconds=self.config['stats_total_break_time']))}\n\n"
            f"[Incident Reports]\n"
            f"Recent Crashes   = {len(self.config['crash_times'])} in last {CRASH_WINDOW_SECONDS // 60}m\n"
            f"Latest Exceptions= {self.config['last_error'] or 'NONE'}\n"
            f"```"
        )
        await ctx.send(dashboard)

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.channel.id != CHANNEL_ID or message.author.id != TARGET_BOT_ID:
            return

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

        depleted = any(dep in text_to_scan for dep in ["not enough", "insufficient", "don't have enough", "missing materials", "lack of", "don't possess"])
        if not depleted:
            return

        if self.config["plant_material"].lower() in text_to_scan or "scroll" in text_to_scan:
            if self.config["plant_enabled"]:
                self.config["plant_enabled"] = False
                await self.save_state()
                await self.notify_channel("⚠️ Component Deactivated", f"Out of `{self.config['plant_material']}`. Disabling planting module loops.")
        elif self.config["refine_recipe_id"].lower() in text_to_scan:
            if self.config["refine_enabled"]:
                self.config["refine_enabled"] = False
                await self.save_state()
                await self.notify_channel("⚠️ Component Deactivated", f"Missing materials for recipe `{self.config['refine_recipe_id']}`. Disabling refine loop.")

bot = commands.Bot(command_prefix="$$", self_bot=True)

@bot.event
async def on_ready():
    if not bot.get_cog("FarmingEngine"):
        await bot.add_cog(FarmingEngine(bot))
    logger.info(f"Self-bot session safely initialized: {bot.user}")

if not TOKEN or not CHANNEL_ID or not TARGET_BOT_ID:
    logger.error("Environment file missing critical deployment requirements.")
else:
    bot.run(TOKEN)
