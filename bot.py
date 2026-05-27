"""
Amazon Jobs Bot — uses raw Telegram Bot API (no python-telegram-bot library)
Compatible with Python 3.14+
"""

import time
import json
import os
import logging
import threading
from datetime import datetime
import requests
import httpx
import asyncio
from monitor import AmazonJobsMonitor

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

CONFIG_FILE = "config.json"
DEFAULT_CONFIG = {
    "telegram_token": "",
    "poll_interval": 300,
    "schedule_filter": "both",
}

monitor_thread = None
monitor_running = False
monitor_obj = None


def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            return {**DEFAULT_CONFIG, **json.load(f)}
    return DEFAULT_CONFIG.copy()


def save_config(cfg):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)


class SimpleTelegramBot:
    def __init__(self, token):
        self.token = token
        self.base = f"https://api.telegram.org/bot{token}"
        self.offset = 0

    def send(self, chat_id, text, parse_mode="Markdown", disable_preview=True):
        try:
            requests.post(f"{self.base}/sendMessage", json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": disable_preview,
            }, timeout=10)
        except Exception as e:
            log.error(f"Send error: {e}")

    def get_updates(self):
        try:
            r = requests.get(f"{self.base}/getUpdates", params={
                "offset": self.offset,
                "timeout": 30,
                "allowed_updates": ["message", "callback_query"]
            }, timeout=35)
            data = r.json()
            if data.get("ok"):
                return data.get("result", [])
        except Exception as e:
            log.error(f"getUpdates error: {e}")
        return []

    def answer_callback(self, callback_id):
        try:
            requests.post(f"{self.base}/answerCallbackQuery",
                json={"callback_query_id": callback_id}, timeout=5)
        except Exception:
            pass

    def send_keyboard(self, chat_id, text, buttons):
        keyboard = {"inline_keyboard": buttons}
        try:
            requests.post(f"{self.base}/sendMessage", json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "reply_markup": json.dumps(keyboard),
            }, timeout=10)
        except Exception as e:
            log.error(f"Keyboard send error: {e}")


# Simple async bridge for monitor
class AsyncBotBridge:
    def __init__(self, bot: SimpleTelegramBot, chat_id):
        self.bot = bot
        self.chat_id = chat_id

    async def send_message(self, chat_id, text, parse_mode="Markdown", disable_web_page_preview=True):
        self.bot.send(chat_id, text, parse_mode, disable_web_page_preview)


def run_monitor_thread(cfg, chat_id, bot):
    global monitor_running, monitor_obj

    bridge = AsyncBotBridge(bot, chat_id)

    async def _run():
        global monitor_obj
        monitor_obj = AmazonJobsMonitor(cfg, chat_id, bridge)
        await monitor_obj.run()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_run())
    finally:
        loop.close()
        monitor_running = False


def handle_message(bot: SimpleTelegramBot, msg):
    global monitor_thread, monitor_running, monitor_obj

    chat_id = msg["chat"]["id"]
    text = msg.get("text", "").strip()

    if text == "/start" or text.startswith("/start"):
        cfg = load_config()
        schedule_label = {"both": "Full & Part-time", "full": "Full-time only", "part": "Part-time only"}.get(cfg.get("schedule_filter", "both"), "Both")
        status = "🟢 Running" if monitor_running else "🔴 Stopped"
        bot.send_keyboard(chat_id,
            f"*🏭 Amazon Jobs Bot*  {status}\n\n"
            f"Watches amazon.jobs for warehouse jobs in:\n"
            f"Toronto · Mississauga · Brampton · Etobicoke + more\n\n"
            f"*Settings:*\n"
            f"  • Schedule: `{schedule_label}`\n"
            f"  • Check every: `{cfg.get('poll_interval', 300)}s`",
            [
                [{"text": "▶️ Start Monitoring", "callback_data": "start"},
                 {"text": "⏹ Stop", "callback_data": "stop"}],
                [{"text": "🔍 Check Now", "callback_data": "check"},
                 {"text": "📊 Stats", "callback_data": "stats"}],
                [{"text": "⚙️ Full-time only", "callback_data": "sched_full"},
                 {"text": "⚙️ Part-time only", "callback_data": "sched_part"},
                 {"text": "⚙️ Both", "callback_data": "sched_both"}],
            ]
        )

    elif text == "/monitor" or text == "/start_monitor":
        start_monitor(bot, chat_id)

    elif text == "/stop":
        stop_monitor(bot, chat_id)

    elif text == "/status":
        send_status(bot, chat_id)

    elif text == "/check":
        do_check_now(bot, chat_id)

    elif text == "/help":
        bot.send(chat_id,
            "*Commands*\n\n"
            "/start — Main menu\n"
            "/monitor — Start monitoring\n"
            "/stop — Stop monitoring\n"
            "/check — Scan right now\n"
            "/status — See stats\n"
            "/help — This message"
        )


def handle_callback(bot: SimpleTelegramBot, cb):
    global monitor_thread, monitor_running

    chat_id = cb["message"]["chat"]["id"]
    data = cb["data"]
    bot.answer_callback(cb["id"])

    if data == "start":
        start_monitor(bot, chat_id)
    elif data == "stop":
        stop_monitor(bot, chat_id)
    elif data == "check":
        do_check_now(bot, chat_id)
    elif data == "stats":
        send_status(bot, chat_id)
    elif data.startswith("sched_"):
        val = data.split("_")[1]
        cfg = load_config()
        cfg["schedule_filter"] = val
        save_config(cfg)
        label = {"both": "Full & Part-time", "full": "Full-time only", "part": "Part-time only"}[val]
        bot.send(chat_id, f"✅ Schedule filter set to: *{label}*", "Markdown")


def start_monitor(bot, chat_id):
    global monitor_thread, monitor_running, monitor_obj
    if monitor_running:
        bot.send(chat_id, "✅ Already monitoring! Use /status to check.")
        return
    cfg = load_config()
    monitor_running = True
    monitor_thread = threading.Thread(
        target=run_monitor_thread, args=(cfg, chat_id, bot), daemon=True
    )
    monitor_thread.start()
    # Confirmation sent by monitor itself after first scan


def stop_monitor(bot, chat_id):
    global monitor_running, monitor_obj
    if monitor_obj:
        monitor_obj.running = False
    monitor_running = False
    bot.send(chat_id, "⏹ *Monitoring stopped.*", "Markdown")


def send_status(bot, chat_id):
    stats = monitor_obj.stats if monitor_obj else {}
    status = "🟢 Running" if monitor_running else "🔴 Stopped"
    bot.send(chat_id,
        f"*📊 Status*\n\n"
        f"Monitoring: {status}\n\n"
        f"*Session stats:*\n"
        f"  Checks done: `{stats.get('checks', 0)}`\n"
        f"  New jobs found: `{stats.get('jobs_found', 0)}`\n"
        f"  Alerts sent: `{stats.get('new_alerts_sent', 0)}`\n"
        f"  Last check: `{stats.get('last_check', 'never')}`\n"
        f"  Errors: `{stats.get('errors', 0)}`"
    )


def do_check_now(bot, chat_id):
    bot.send(chat_id, "🔍 Scanning amazon.jobs now... (takes ~30 seconds)")
    cfg = load_config()

    async def _check():
        bridge = AsyncBotBridge(bot, chat_id)
        temp = AmazonJobsMonitor(cfg, chat_id, bridge)
        if monitor_obj:
            temp.seen_job_ids = monitor_obj.seen_job_ids.copy()
        jobs = await temp.check_now()
        if not jobs:
            bot.send(chat_id, "😔 No *new* warehouse jobs in GTA right now.\n\nKeep monitoring with /monitor — you'll be alerted the second something posts.")
        else:
            bot.send(chat_id, f"✅ Found *{len(jobs)} new job(s)*!", "Markdown")
            for job in jobs[:5]:
                schedule_tag = f" · {job['schedule']}" if job['schedule'] != 'N/A' else ""
                bot.send(chat_id,
                    f"🏭 *{job['title']}*\n"
                    f"📍 {job['location']}\n"
                    f"🗓 Posted: {job['posted']}{schedule_tag}\n\n"
                    f"👉 {job['url']}",
                    "Markdown", False
                )

    loop = asyncio.new_event_loop()
    t = threading.Thread(target=lambda: loop.run_until_complete(_check()), daemon=True)
    t.start()


def main():
    cfg = load_config()
    token = os.environ.get("TELEGRAM_TOKEN") or cfg.get("telegram_token", "")
    if not token or token == "PASTE_YOUR_BOT_TOKEN_HERE":
        print("=" * 50)
        print("❌ No token in config.json!")
        print("   Add your Telegram bot token and restart.")
        print("=" * 50)
        return

    bot = SimpleTelegramBot(token)
    log.info("🤖 Amazon Jobs Bot started (pure requests mode)")
    print("✅ Bot is running! Open Telegram and send /start to your bot.")

    while True:
        updates = bot.get_updates()
        for update in updates:
            bot.offset = update["update_id"] + 1
            if "message" in update:
                try:
                    handle_message(bot, update["message"])
                except Exception as e:
                    log.error(f"Message handler error: {e}")
            elif "callback_query" in update:
                try:
                    handle_callback(bot, update["callback_query"])
                except Exception as e:
                    log.error(f"Callback handler error: {e}")
        time.sleep(0.5)


if __name__ == "__main__":
    main()
