import asyncio

# Pyrogram 2.0.106 expects a current event loop during import.
# Python 3.14 no longer creates one automatically.
asyncio.set_event_loop(asyncio.new_event_loop())

import hashlib
import html
import json
import logging
import mimetypes
import os
import queue
import secrets
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from dotenv import load_dotenv
from pyrogram import Client, utils

# ============================================================
# TRUE STREAM V5
# Telegram Bot API = reliable update/reply layer
# Pyrogram MTProto = storage-channel + real Telegram streaming
# HTTP server = browser / MX Player / VLC / direct download
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"), override=False)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("true-stream-v5")

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
STORAGE_CHANNEL = os.getenv("STORAGE_CHANNEL", "")
BASE_URL = os.getenv("BASE_URL", "").strip().strip("'\"").rstrip("/")
PORT = int(os.getenv("PORT", "10000"))
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().strip("'\"").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip().strip("'\"")
UPI_ID = os.getenv("UPI_ID", "").strip().strip("'\"")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "").strip().lstrip("@")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

APP_VERSION = "True Stream V6"

required = {
    "API_ID": API_ID,
    "API_HASH": API_HASH,
    "BOT_TOKEN": BOT_TOKEN,
    "STORAGE_CHANNEL": STORAGE_CHANNEL,
    "SUPABASE_URL": SUPABASE_URL,
    "SUPABASE_KEY": SUPABASE_KEY,
    "BASE_URL": BASE_URL,
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError("Missing required env: " + ", ".join(missing))

try:
    STORAGE_CHAT_ID = int(STORAGE_CHANNEL)
except ValueError:
    STORAGE_CHAT_ID = STORAGE_CHANNEL

# Known Telegram -100 channel range used by the supplied bot projects.
try:
    utils.MIN_CHANNEL_ID = -1004453734773
    utils.MIN_CHAT_ID = -999999999999
except Exception:
    pass

# Pyrogram is deliberately NOT used for incoming updates.
# That avoids the update-dispatch/event-loop problem present in the old code.
bot = Client(
    "stream007_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir=BASE_DIR,
)

MAIN_LOOP = None
CHUNK_SIZE = 1024 * 1024
CLEANUP_INTERVAL = 30 * 60
WEBHOOK_PATH = "/telegram/webhook"

PLANS = {
    "prime_1d": {"title": "24 Hours", "price": 3, "days": 1},
    "prime_10d": {"title": "10 Days", "price": 23, "days": 10},
    "prime_30d": {"title": "30 Days", "price": 50, "days": 30},
    "prime_365d": {"title": "365 Days", "price": 499, "days": 365},
    "prime_life": {"title": "Lifetime", "price": 1999, "days": None},
}

# ----------------------------
# General helpers
# ----------------------------

def utcnow():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def safe_filename(name):
    return str(name or "file").replace('"', "").replace("\r", "").replace("\n", "")[:200]


def json_bytes(obj):
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")

# ----------------------------
# Supabase REST
# ----------------------------

def sb_request(method, table, params=None, body=None, prefer=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read()
        return json.loads(raw.decode("utf-8")) if raw else None


def sb_count(table, params=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    query = dict(params or {})
    query.setdefault("select", "telegram_id" if table == "users" else "id")
    url += "?" + urllib.parse.urlencode(query, doseq=True)
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "count=exact",
    }
    headers["Range"] = "0-0"
    req = urllib.request.Request(url, headers=headers, method="HEAD")
    with urllib.request.urlopen(req, timeout=20) as r:
        content_range = r.headers.get("Content-Range", "")
    if "/" in content_range:
        raw = content_range.rsplit("/", 1)[1]
        if raw != "*":
            return int(raw)
    return 0


def ensure_user(user):
    return sb_request(
        "POST", "users",
        body={
            "telegram_id": int(user["id"]),
            "username": user.get("username") or "",
            "first_name": user.get("first_name") or "",
            "is_admin": int(user["id"]) in ADMIN_IDS,
            "updated_at": iso(utcnow()),
        },
        prefer="resolution=merge-duplicates,return=representation",
    )


def get_user(user_id):
    rows = sb_request(
        "GET", "users",
        params={"telegram_id": f"eq.{int(user_id)}", "select": "*", "limit": "1"},
    )
    return rows[0] if rows else None


def update_user(user_id, body):
    return sb_request(
        "PATCH", "users",
        params={"telegram_id": f"eq.{int(user_id)}"},
        body=body,
        prefer="return=representation",
    )


def is_membership_active(user):
    if not user:
        return False
    if user.get("is_admin"):
        return True
    if user.get("membership_plan") == "lifetime":
        return True
    until = parse_dt(user.get("membership_until"))
    return bool(until and until > utcnow())


def count_links_24h(user_id):
    cutoff = iso(utcnow() - timedelta(hours=24))
    rows = sb_request(
        "GET", "links",
        params={
            "user_id": f"eq.{int(user_id)}",
            "created_at": f"gte.{cutoff}",
            "select": "id",
            "limit": "1000",
        },
    )
    return len(rows or [])


def new_code():
    return secrets.token_urlsafe(10).replace("-", "").replace("_", "")[:13]


def get_link(code):
    rows = sb_request(
        "GET", "links",
        params={"code": f"eq.{code}", "select": "*", "limit": "1"},
    )
    return rows[0] if rows else None


def insert_link(row):
    return sb_request("POST", "links", body=row, prefer="return=representation")


def delete_link(link_id):
    sb_request("DELETE", "links", params={"id": f"eq.{int(link_id)}"})


def increment_total_links():
    url = f"{SUPABASE_URL}/rest/v1/rpc/increment_total_links"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    req = urllib.request.Request(url, data=b"{}", headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read()
        return json.loads(raw.decode("utf-8")) if raw else None


def get_total_links_generated():
    rows = sb_request(
        "GET", "bot_stats",
        params={"id": "eq.1", "select": "total_links_generated", "limit": "1"},
    )
    return int(rows[0]["total_links_generated"]) if rows else 0

# ----------------------------
# Payment DB
# ----------------------------

def get_payment(payment_id):
    rows = sb_request(
        "GET", "payment_proofs",
        params={"id": f"eq.{int(payment_id)}", "select": "*", "limit": "1"},
    )
    return rows[0] if rows else None


def latest_awaiting_payment(user_id):
    rows = sb_request(
        "GET", "payment_proofs",
        params={
            "user_id": f"eq.{int(user_id)}",
            "status": "eq.awaiting_proof",
            "select": "*",
            "order": "created_at.desc",
            "limit": "1",
        },
    )
    return rows[0] if rows else None


def create_payment(user_id, plan_code):
    sb_request(
        "PATCH", "payment_proofs",
        params={"user_id": f"eq.{int(user_id)}", "status": "eq.awaiting_proof"},
        body={"status": "cancelled", "reviewed_at": iso(utcnow())},
        prefer="return=minimal",
    )
    rows = sb_request(
        "POST", "payment_proofs",
        body={
            "user_id": int(user_id),
            "plan_code": plan_code,
            "amount": PLANS[plan_code]["price"],
            "status": "awaiting_proof",
        },
        prefer="return=representation",
    )
    return rows[0] if rows else None


def update_payment(payment_id, body):
    return sb_request(
        "PATCH", "payment_proofs",
        params={"id": f"eq.{int(payment_id)}"},
        body=body,
        prefer="return=representation",
    )

def format_remaining(until):
    if not until:
        return "Lifetime"
    delta = until - utcnow()
    total_seconds = int(delta.total_seconds())
    if total_seconds <= 0:
        return "Expired"
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")
    return " ".join(parts)


def prime_status_text(user):
    if not user or not is_membership_active(user):
        return (
            "👤 <b>Prime Status</b>\n\n"
            "❌ Prime active nahi hai.\n\n"
            "💎 Prime plans ke liye neeche button use karein."
        )
    if user.get("membership_plan") == "lifetime":
        return (
            "👑 <b>Prime Status</b>\n\n"
            "✅ Prime Active\n"
            "💎 Plan: Lifetime\n"
            "⏳ Expiry: Never"
        )
    until = parse_dt(user.get("membership_until"))
    plan = PLANS.get(user.get("membership_plan"), {})
    title = plan.get("title") or user.get("membership_plan") or "Prime"
    return (
        "👑 <b>Prime Status</b>\n\n"
        "✅ Prime Active\n"
        f"💎 Plan: {html.escape(title)}\n"
        f"⏳ Remaining: <b>{html.escape(format_remaining(until))}</b>\n"
        f"📅 Expires: <code>{html.escape(until.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'))}</code>"
    )


def get_bot_status():
    non_admin = sb_count("users", {"is_admin": "eq.false"})
    lifetime = sb_count("users", {"is_admin": "eq.false", "membership_plan": "eq.lifetime"})
    timed_prime = sb_count("users", {
        "is_admin": "eq.false",
        "membership_plan": "neq.lifetime",
        "membership_until": f"gte.{iso(utcnow())}",
    })
    prime_users = lifetime + timed_prime
    normal_users = max(0, non_admin - prime_users)
    try:
        total_links = get_total_links_generated()
    except Exception:
        total_links = sb_count("links")
    return prime_users, normal_users, total_links


def admin_status_text():
    prime_users, normal_users, total_links = get_bot_status()
    return (
        "📊 <b>Bot Status</b>\n\n"
        f"👑 Prime Users: <b>{prime_users}</b>\n"
        f"👤 Normal Users: <b>{normal_users}</b>\n"
        f"🔗 Total Links Generated: <b>{total_links}</b>"
    )


# ----------------------------
# Telegram Bot API layer
# ----------------------------

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"


def tg_call(method, payload=None, timeout=70):
    url = f"{TG_API}/{method}"
    data = json_bytes(payload or {})
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        result = json.loads(r.read().decode("utf-8"))
    if not result.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {result}")
    return result["result"]


def tg_send_message(chat_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {"chat_id": int(chat_id), "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return tg_call("sendMessage", payload, timeout=30)

async def tg_send_message_async(chat_id, text, reply_markup=None, parse_mode="HTML"):
    return await asyncio.to_thread(tg_send_message, chat_id, text, reply_markup, parse_mode)


def tg_answer_callback(callback_id, text=None, show_alert=False):
    payload = {"callback_query_id": callback_id, "show_alert": bool(show_alert)}
    if text:
        payload["text"] = text
    return tg_call("answerCallbackQuery", payload, timeout=20)


def tg_edit_reply_markup(chat_id, message_id, reply_markup=None):
    payload = {"chat_id": int(chat_id), "message_id": int(message_id)}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return tg_call("editMessageReplyMarkup", payload, timeout=20)


def tg_copy_message(chat_id, from_chat_id, message_id, reply_markup=None):
    payload = {
        "chat_id": int(chat_id),
        "from_chat_id": int(from_chat_id),
        "message_id": int(message_id),
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return tg_call("copyMessage", payload, timeout=60)


def webhook_secret():
    return WEBHOOK_SECRET or hashlib.sha256((BOT_TOKEN + ":webhook").encode()).hexdigest()


def tg_set_webhook():
    url = f"{BASE_URL}{WEBHOOK_PATH}"
    return tg_call(
        "setWebhook",
        {
            "url": url,
            "secret_token": webhook_secret(),
            "allowed_updates": ["message", "callback_query"],
            "drop_pending_updates": False,
            "max_connections": 40,
        },
        timeout=30,
    )


def tg_set_commands():
    commands = [
        {"command": "start", "description": "Open welcome menu"},
        {"command": "prime", "description": "Check Prime status"},
        {"command": "status", "description": "Admin bot status"},
    ]
    tg_call("setMyCommands", {"commands": commands}, timeout=20)
    for admin_id in ADMIN_IDS:
        tg_call(
            "setMyCommands",
            {
                "scope": {"type": "chat", "chat_id": int(admin_id)},
                "commands": [
                    {"command": "start", "description": "Open welcome menu"},
                    {"command": "prime", "description": "Check Prime status"},
                    {"command": "status", "description": "Show bot statistics"},
                    {"command": "id", "description": "Show Telegram ID"},
                ],
            },
            timeout=20,
        )


# ----------------------------
# Inline keyboards
# ----------------------------

def plans_keyboard_dict():
    return {
        "inline_keyboard": [
            [{"text": "₹3 • 24 Hours", "callback_data": "plan:prime_1d"}],
            [{"text": "₹23 • 10 Days", "callback_data": "plan:prime_10d"}],
            [{"text": "₹50 • 30 Days", "callback_data": "plan:prime_30d"}],
            [{"text": "₹499 • 365 Days", "callback_data": "plan:prime_365d"}],
            [{"text": "₹1999 • Lifetime", "callback_data": "plan:prime_life"}],
        ]
    }


def prime_keyboard_dict():
    return {"inline_keyboard": [[{"text": "💎 Prime Plans", "callback_data": "plans"}]]}


def approve_keyboard(payment_id):
    return {
        "inline_keyboard": [[
            {"text": "✅ Approve", "callback_data": f"pay:approve:{payment_id}"},
            {"text": "❌ Reject", "callback_data": f"pay:reject:{payment_id}"},
        ]]
    }

# ----------------------------
# Telegram message/media helpers
# ----------------------------

def get_media_info(message):
    if message.get("video"):
        m = message["video"]
        return m.get("file_name") or f"video_{message['message_id']}.mp4", m.get("mime_type") or "video/mp4", int(m.get("file_size") or 0)
    if message.get("document"):
        m = message["document"]
        return m.get("file_name") or f"document_{message['message_id']}", m.get("mime_type") or "application/octet-stream", int(m.get("file_size") or 0)
    if message.get("audio"):
        m = message["audio"]
        return m.get("file_name") or f"audio_{message['message_id']}.mp3", m.get("mime_type") or "audio/mpeg", int(m.get("file_size") or 0)
    if message.get("photo"):
        p = message["photo"][-1]
        return f"photo_{message['message_id']}.jpg", "image/jpeg", int(p.get("file_size") or 0)
    return f"telegram_{message['message_id']}", "application/octet-stream", 0


def is_media_message(message):
    return any(message.get(k) for k in ("video", "photo", "document", "audio"))


def user_from_message(message):
    return message.get("from") or {}

# ----------------------------
# File link creation
# ----------------------------

async def make_link(message):
    # All blocking REST/urllib calls are moved off the asyncio event loop.
    # This is important because the same loop is also running Telegram polling.
    user = user_from_message(message)
    user_id = int(user["id"])

    await asyncio.to_thread(ensure_user, user)
    db_user = await asyncio.to_thread(get_user, user_id)

    privileged = bool(db_user and (db_user.get("is_admin") or is_membership_active(db_user)))
    if not privileged:
        link_count = await asyncio.to_thread(count_links_24h, user_id)
        if link_count >= 3:
            await tg_send_message_async(
                user_id,
                "⚠️ <b>Aapki free limit 3 links / 24 hours complete ho gayi hai.</b>\n\n"
                "💎 Prime activate karke unlimited links use karein:",
                plans_keyboard_dict(),
            )
            return None

    # Bot API performs the copy without blocking the asyncio loop.
    copied = await asyncio.to_thread(
        tg_copy_message,
        STORAGE_CHAT_ID,
        message["chat"]["id"],
        message["message_id"],
    )
    storage_message_id = int(copied["message_id"])

    name, mime, size = get_media_info(message)
    expires_at = None if privileged else iso(utcnow() + timedelta(hours=24))

    code = new_code()
    while await asyncio.to_thread(get_link, code):
        code = new_code()

    await asyncio.to_thread(insert_link, {
        "code": code,
        "user_id": user_id,
        "storage_chat_id": str(STORAGE_CHAT_ID),
        "storage_message_id": storage_message_id,
        "name": name,
        "mime": mime,
        "size": size,
        "created_at": iso(utcnow()),
        "expires_at": expires_at,
    })
    try:
        await asyncio.to_thread(increment_total_links)
    except Exception:
        # Link creation must not fail only because the optional lifetime counter is unavailable.
        log.exception("Could not increment lifetime link counter")
    return f"{BASE_URL}/show/{code}"

# ----------------------------
# Payment proof
# ----------------------------

async def handle_payment_proof(message):
    # Only photos/documents can be payment proofs.
    # Video/audio must go directly to normal link creation.
    if not message.get("photo") and not message.get("document"):
        return False

    user = user_from_message(message)
    user_id = int(user["id"])

    try:
        payment = await asyncio.to_thread(latest_awaiting_payment, user_id)
    except Exception:
        # If the payment_proofs table is not created yet, do not block normal
        # media processing. The payment feature can be enabled after the SQL
        # table is created.
        log.exception("Payment lookup failed; treating media as normal file")
        return False

    if not payment:
        return False

    if message.get("photo"):
        proof_file_id = message["photo"][-1]["file_id"]
        proof_type = "photo"
    else:
        proof_file_id = message["document"]["file_id"]
        proof_type = "document"

    await asyncio.to_thread(update_payment, payment["id"], {
        "status": "pending_review",
        "proof_file_id": proof_file_id,
        "proof_type": proof_type,
        "proof_message_id": int(message["message_id"]),
        "submitted_at": iso(utcnow()),
    })

    plan = PLANS[payment["plan_code"]]
    username = f"@{user.get('username')}" if user.get("username") else "(no username)"
    admin_text = (
        "💳 <b>PAYMENT PROOF</b>\n\n"
        f"👤 User: {html.escape(user.get('first_name') or 'Unknown')}\n"
        f"🆔 Telegram ID: <code>{user_id}</code>\n"
        f"🔗 Username: {html.escape(username)}\n"
        f"💎 Plan: {html.escape(plan['title'])}\n"
        f"💰 Amount: ₹{plan['price']}\n"
        f"🧾 Payment ID: <code>{payment['id']}</code>\n\n"
        "Verify the proof, then Approve or Reject."
    )

    for admin_id in ADMIN_IDS:
        try:
            await tg_send_message_async(admin_id, admin_text, approve_keyboard(payment["id"]))
            await asyncio.to_thread(
                tg_copy_message, admin_id, message["chat"]["id"], message["message_id"]
            )
        except Exception:
            log.exception("Could not send payment proof to admin %s", admin_id)

    await tg_send_message_async(
        user_id,
        "📩 <b>Payment screenshot receive ho gaya.</b>\n"
        "Admin verification ke baad aapko automatic confirmation milega.",
    )
    return True

# ----------------------------
# Callback processing
# ----------------------------

async def handle_callback(query):
    callback_id = query["id"]
    data = query.get("data") or ""
    message = query.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    message_id = message.get("message_id")
    actor_id = int(query["from"]["id"])

    try:
        if data == "plans":
            await asyncio.to_thread(tg_call, "editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": (
                    "💎 <b>Prime Plans</b>\n\n"
                    "₹3 — 24 hours\n"
                    "₹23 — 10 days\n"
                    "₹50 — 30 days\n"
                    "₹499 — 365 days\n"
                    "₹1999 — Lifetime\n\n"
                    "Plan select karke payment instructions dekhein."
                ),
                "parse_mode": "HTML",
                "reply_markup": plans_keyboard_dict(),
            }, timeout=30)
            await asyncio.to_thread(tg_answer_callback, callback_id)
            return

        if data.startswith("plan:"):
            key = data.split(":", 1)[1]
            plan = PLANS.get(key)
            if not plan:
                await asyncio.to_thread(tg_answer_callback, callback_id, "Invalid plan", True)
                return

            try:
                payment = await asyncio.to_thread(create_payment, actor_id, key)
            except Exception:
                log.exception("Payment creation failed")
                await asyncio.to_thread(tg_answer_callback, callback_id, "Payment system setup incomplete", True)
                return

            if not payment:
                await asyncio.to_thread(tg_answer_callback, callback_id, "Payment request create nahi hua", True)
                return

            contact = f"https://t.me/{ADMIN_USERNAME}" if ADMIN_USERNAME else None
            markup = {"inline_keyboard": []}
            if contact:
                markup["inline_keyboard"].append([{"text": "💬 Contact Admin", "url": contact}])

            await tg_send_message_async(
                chat_id,
                f"💎 <b>Prime — {html.escape(plan['title'])}</b>\n"
                f"💰 Amount: ₹{plan['price']}\n\n"
                f"💳 UPI: <code>{html.escape(UPI_ID or 'UPI_ID_NOT_SET')}</code>\n\n"
                "1️⃣ UPI se payment karein.\n"
                "2️⃣ Payment ka screenshot isi bot ko bhejein.\n"
                "3️⃣ Verification ke baad Prime activate hoga.\n\n"
                f"👤 Telegram ID: <code>{actor_id}</code>\n"
                f"🧾 Payment ID: <code>{payment['id']}</code>",
                markup if markup["inline_keyboard"] else None,
            )
            await asyncio.to_thread(tg_answer_callback, callback_id, "Plan selected")
            return

        if data.startswith("pay:approve:") or data.startswith("pay:reject:"):
            if actor_id not in ADMIN_IDS:
                await asyncio.to_thread(tg_answer_callback, callback_id, "Admin only", True)
                return

            action, raw_id = data.rsplit(":", 1)
            payment = await asyncio.to_thread(get_payment, int(raw_id))
            if not payment:
                await asyncio.to_thread(tg_answer_callback, callback_id, "Payment proof not found", True)
                return
            if payment.get("status") != "pending_review":
                await asyncio.to_thread(tg_answer_callback, callback_id, f"Already {payment.get('status')}", True)
                return

            if action == "pay:reject":
                await asyncio.to_thread(update_payment, payment["id"], {
                    "status": "rejected",
                    "reviewed_by": actor_id,
                    "reviewed_at": iso(utcnow()),
                })
                try:
                    await tg_send_message_async(
                        int(payment["user_id"]),
                        "❌ Payment proof reject ho gaya.\n\nAgar payment sahi hai to admin se contact karein.",
                    )
                except Exception:
                    pass
                await asyncio.to_thread(tg_edit_reply_markup, chat_id, message_id, {"inline_keyboard": []})
                await asyncio.to_thread(tg_answer_callback, callback_id, "Rejected")
                return

            plan = PLANS.get(payment["plan_code"])
            target = await asyncio.to_thread(get_user, payment["user_id"])
            if not plan or not target:
                await asyncio.to_thread(tg_answer_callback, callback_id, "Plan/user missing", True)
                return

            if plan["days"] is None:
                membership_plan = "lifetime"
                membership_until = None
            else:
                current_until = parse_dt(target.get("membership_until"))
                start = current_until if current_until and current_until > utcnow() else utcnow()
                membership_plan = payment["plan_code"]
                membership_until = iso(start + timedelta(days=plan["days"]))

            await asyncio.to_thread(update_user, payment["user_id"], {
                "membership_plan": membership_plan,
                "membership_until": membership_until,
                "updated_at": iso(utcnow()),
            })
            await asyncio.to_thread(update_payment, payment["id"], {
                "status": "approved",
                "reviewed_by": actor_id,
                "reviewed_at": iso(utcnow()),
            })

            try:
                await tg_send_message_async(
                    int(payment["user_id"]),
                    f"✅ <b>Payment verified!</b>\n\n"
                    f"💎 Prime {html.escape(plan['title'])} activated.\n"
                    "Ab aap unlimited links create kar sakte hain.",
                )
            except Exception:
                pass

            await asyncio.to_thread(tg_edit_reply_markup, chat_id, message_id, {"inline_keyboard": []})
            await asyncio.to_thread(tg_answer_callback, callback_id, "Approved — Prime activated")
            return

        await asyncio.to_thread(tg_answer_callback, callback_id)
    except Exception:
        log.exception("Callback handling failed")
        try:
            await asyncio.to_thread(tg_answer_callback, callback_id, "Something went wrong", True)
        except Exception:
            pass

# ----------------------------
# Incoming update processing
# ----------------------------

async def handle_message(message):
    user = user_from_message(message)
    if not user or "id" not in user:
        return
    user_id = int(user["id"])

    text = message.get("text") or ""
    command = text.split()[0].split("@")[0].lower() if text.startswith("/") else ""

    if command == "/start":
        # Send /start first. Database work must never delay the welcome message.
        await tg_send_message_async(
            user_id,
            "👋 <b>Welcome to True Stream V6!</b>\n\n"
            "🎬 Video / Photo / Document / Audio bhejo.\n"
            "🔗 Main streaming webpage link dunga.\n\n"
            "🆓 Free: 3 links / rolling 24 hours\n"
            "💎 Prime: unlimited links\n"
            "👑 Admin: unlimited links",
            {"inline_keyboard": [[{"text": "💎 Prime Plans", "callback_data": "plans"}]]},
        )
        await asyncio.to_thread(ensure_user, user)
        return

    await asyncio.to_thread(ensure_user, user)

    if command == "/prime":
        db_user = await asyncio.to_thread(get_user, user_id)
        await tg_send_message_async(user_id, prime_status_text(db_user), prime_keyboard_dict())
        return

    if command == "/status":
        if user_id not in ADMIN_IDS:
            await tg_send_message_async(user_id, "⛔ <b>Admin only.</b>")
            return
        try:
            await tg_send_message_async(user_id, await asyncio.to_thread(admin_status_text))
        except Exception:
            log.exception("Admin status failed")
            await tg_send_message_async(user_id, "❌ Status load nahi ho paya. Dobara try karein.")
        return

    if command == "/id":
        await tg_send_message_async(user_id, f"🆔 Your Telegram ID: <code>{user_id}</code>")
        return

    if command == "/activate":
        if user_id not in ADMIN_IDS:
            return
        parts = text.split()
        if len(parts) < 3 or parts[2] not in PLANS:
            await tg_send_message_async(user_id, "Usage:\n/activate USER_ID prime_30d\n\nPlans: " + ", ".join(PLANS))
            return
        try:
            target_id = int(parts[1])
        except ValueError:
            await tg_send_message_async(user_id, "Invalid USER_ID")
            return
        key = parts[2]
        plan = PLANS[key]
        target = await asyncio.to_thread(get_user, target_id)
        if not target:
            await tg_send_message_async(user_id, "User ne pehle /start nahi kiya.")
            return
        if plan["days"] is None:
            until = None
            membership_plan = "lifetime"
        else:
            until = iso(utcnow() + timedelta(days=plan["days"]))
            membership_plan = key
        await asyncio.to_thread(update_user, target_id, {
            "membership_plan": membership_plan,
            "membership_until": until,
            "updated_at": iso(utcnow()),
        })
        await tg_send_message_async(user_id, "✅ Membership activated.")
        await tg_send_message_async(target_id, f"💎 Prime {html.escape(plan['title'])} activated by admin.")
        return

    if is_media_message(message):
        # If the user has an active payment request, a photo/document is treated as proof.
        if await handle_payment_proof(message):
            return
        try:
            link = await make_link(message)
            if link:
                await tg_send_message_async(
                    user_id,
                    f"✅ <b>Link Ready!</b>\n\n{html.escape(link)}\n\n"
                    "🌐 Browser se stream karein ya MX Player / VLC button use karein.",
                )
        except Exception:
            log.exception("Media handler failed")
            await tg_send_message_async(user_id, "❌ File process nahi ho payi. Dobara try karein.")
        return

    if text:
        await tg_send_message_async(user_id, "🎬 File bhejiye — video, photo, document ya audio.")


def dispatch_update(update):
    if "callback_query" in update:
        asyncio.create_task(handle_callback(update["callback_query"]))
    elif "message" in update:
        asyncio.create_task(handle_message(update["message"]))


# ----------------------------
# HTTP player page
# ----------------------------

async def get_storage_message_async(item):
    chat_id = (
        int(item["storage_chat_id"])
        if str(item["storage_chat_id"]).lstrip("-").isdigit()
        else item["storage_chat_id"]
    )
    message_id = int(item["storage_message_id"])
    return await bot.get_messages(chat_id, message_id)


def get_storage_message(item):
    future = asyncio.run_coroutine_threadsafe(
        get_storage_message_async(item),
        MAIN_LOOP
    )
    return future.result(timeout=30)


def delete_storage_message(item):
    chat_id = int(item["storage_chat_id"]) if str(item["storage_chat_id"]).lstrip("-").isdigit() else item["storage_chat_id"]
    future = asyncio.run_coroutine_threadsafe(bot.delete_messages(chat_id, int(item["storage_message_id"])), MAIN_LOOP)
    return future.result(timeout=30)


def make_intent(target_url, package=None, mime="video/*", title=""):
    p = urllib.parse.urlsplit(target_url)
    host_path = p.netloc + p.path
    if p.query:
        host_path += "?" + p.query
    extras = [
        "scheme=" + p.scheme,
        "action=android.intent.action.VIEW",
        "type=" + mime,
    ]
    if package:
        extras.append("package=" + package)
    if title:
        extras.append("S.title=" + urllib.parse.quote(title, safe=""))
    return "intent://" + host_path + "#Intent;" + ";".join(extras) + ";end"


def webpage(item, code):
    name = safe_filename(item.get("name") or "file")
    mime = item.get("mime") or "application/octet-stream"
    stream_url = f"{BASE_URL}/stream/{code}"
    download_url = f"{BASE_URL}/download/{code}"
    is_video = mime.startswith("video/")
    is_audio = mime.startswith("audio/")
    is_image = mime.startswith("image/")

    mx_url = make_intent(stream_url, "com.mxtech.videoplayer.ad", mime if is_video else "*/*", name)
    vlc_url = make_intent(stream_url, "org.videolan.vlc", mime if is_video else "*/*", name)

    if is_video:
        preview = f'<video controls playsinline preload="auto" src="{html.escape(stream_url, quote=True)}"></video>'
        mx_button = f'<a class="btn mx" href="{html.escape(mx_url, quote=True)}">▶ Open in MX Player</a>'
        vlc_button = f'<a class="btn vlc" href="{html.escape(vlc_url, quote=True)}">▶ Open in VLC</a>'
    elif is_audio:
        preview = f'<audio controls preload="metadata" src="{html.escape(stream_url, quote=True)}"></audio>'
        mx_button = f'<a class="btn mx" href="{html.escape(mx_url, quote=True)}">▶ Open in MX Player</a>'
        vlc_button = f'<a class="btn vlc" href="{html.escape(vlc_url, quote=True)}">▶ Open in VLC</a>'
    elif is_image:
        preview = f'<img src="{html.escape(stream_url, quote=True)}" alt="file">'
        mx_button = ""
        vlc_button = ""
    else:
        preview = '<p class="small">Preview is not available for this file type. Use Download.</p>'
        mx_button = ""
        vlc_button = ""

    generic = f'<a class="btn generic" href="{html.escape(stream_url, quote=True)}">🌐 Open Stream URL</a>'
    return f"""<!doctype html>
<html><head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#080c12">
<title>{html.escape(name)}</title>
<style>
*{{box-sizing:border-box}}body{{margin:0;background:#080c12;color:#f5f7fa;font-family:Arial,sans-serif}}
.wrap{{max-width:850px;margin:auto;padding:18px}}.card{{background:#141a22;border-radius:20px;padding:18px;box-shadow:0 10px 35px #0008}}
h2{{font-size:20px;line-height:1.35;word-break:break-word}}video,audio,img{{display:block;width:100%;max-height:70vh;margin:15px 0;border-radius:14px;background:#000}}
.btn{{display:block;text-decoration:none;text-align:center;padding:15px;margin:10px 0;border-radius:13px;font-weight:700}}
.mx{{background:#2679f5;color:#fff}}.vlc{{background:#ff9800;color:#111}}.generic{{background:#7c3aed;color:#fff}}.dl{{background:#22c55e;color:#06120a}}
.small{{color:#aeb8c5;font-size:13px;line-height:1.5}}
</style></head><body><div class="wrap"><div class="card">
<h2>🎬 {html.escape(name)}</h2>{preview}
{mx_button}{vlc_button}{generic}
<a class="btn dl" href="{html.escape(download_url, quote=True)}">⬇ Fast Download</a>
<p class="small">⚡ True HTTP Range streaming. Browser/MX Player/VLC can request only the required part of the Telegram file.</p>
</div></div></body></html>"""

# ----------------------------
# True HTTP Range streaming
# ----------------------------

async def stream_producer(message, out_queue, stop_event, start_chunk, first_skip, wanted_bytes):
    """Stream Telegram media without blocking the asyncio loop on queue.put()."""
    sent = 0

    async def queue_put(data):
        while not stop_event.is_set():
            try:
                out_queue.put_nowait(data)
                return True
            except queue.Full:
                await asyncio.sleep(0.01)
        return False

    log.info("STREAM START offset_chunk=%s first_skip=%s wanted=%s", start_chunk, first_skip, wanted_bytes)
    try:
        async for chunk in bot.stream_media(message, offset=start_chunk):
            if stop_event.is_set():
                log.info("STREAM STOP REQUESTED sent=%s", sent)
                break
            if first_skip:
                if first_skip >= len(chunk):
                    first_skip -= len(chunk)
                    continue
                chunk = chunk[first_skip:]
                first_skip = 0
            remaining = wanted_bytes - sent
            if remaining <= 0:
                break
            chunk = chunk[:remaining]
            if chunk:
                if not await queue_put(chunk):
                    log.info("STREAM QUEUE STOPPED sent=%s", sent)
                    break
                sent += len(chunk)
            if sent >= wanted_bytes:
                break
        log.info("STREAM PRODUCER FINISHED sent=%s/%s", sent, wanted_bytes)
    except asyncio.CancelledError:
        log.info("STREAM PRODUCER CANCELLED sent=%s/%s", sent, wanted_bytes)
        raise
    except Exception:
        log.exception("Telegram media streaming failed sent=%s/%s", sent, wanted_bytes)
    finally:
        try:
            out_queue.put_nowait(None)
        except queue.Full:
            pass


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        log.info("HTTP " + fmt, *args)

    def send_text(self, status, text, content_type="text/plain; charset=utf-8"):
        try:
            data = text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
        except BrokenPipeError:
            pass

    def serve_media(self, item, download=False):
        try:
            size = int(item.get("size") or 0)
            mime = item.get("mime") or mimetypes.guess_type(item.get("name", ""))[0] or "application/octet-stream"

            if size <= 0:
                msg = get_storage_message(item)
                media = msg.video or msg.document or msg.audio or msg.photo
                size = int(getattr(media, "file_size", 0) or 0)

            if size <= 0:
                self.send_text(500, "Unknown media size")
                return

            start, end = 0, size - 1
            status = 200
            range_header = self.headers.get("Range")
            if range_header and range_header.startswith("bytes="):
                spec = range_header[6:].split(",", 1)[0].strip()
                a, b = spec.split("-", 1)
                try:
                    if a:
                        start = int(a)
                        if b:
                            end = int(b)
                    else:
                        suffix = int(b)
                        if suffix <= 0:
                            raise ValueError
                        start = max(0, size - suffix)
                    if start < 0 or start >= size or end < start:
                        raise ValueError
                    end = min(end, size - 1)
                    status = 206
                except ValueError:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{size}")
                    self.end_headers()
                    return

            length = end - start + 1
            self.send_response(status)
            self.send_header("Content-Type", mime)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(length))
            self.send_header("Cache-Control", "public, max-age=3600")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Content-Encoding", "identity")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            disposition = "attachment" if download else "inline"
            self.send_header("Content-Disposition", f'{disposition}; filename="{safe_filename(item.get("name"))}"')
            self.end_headers()

            start_chunk = start // CHUNK_SIZE
            first_skip = start % CHUNK_SIZE
            q = queue.Queue(maxsize=16)
            stop_event = threading.Event()
            msg = get_storage_message(item)
            future = asyncio.run_coroutine_threadsafe(
                stream_producer(msg, q, stop_event, start_chunk, first_skip, length), MAIN_LOOP
            )

            remaining = length
            try:
                while remaining > 0:
                    try:
                        chunk = q.get(timeout=1)
                    except queue.Empty:
                        if future.done() or stop_event.is_set():
                            break
                        continue
                    if chunk is None:
                        break
                    try:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        log.info("CLIENT DISCONNECTED sent=%s/%s", length - remaining, length)
                        stop_event.set()
                        break
                    remaining -= len(chunk)
            finally:
                stop_event.set()
                if not future.done():
                    future.cancel()
                try:
                    future.result(timeout=2)
                except BaseException:
                    pass
        except (BrokenPipeError, ConnectionResetError):
            stop_event.set()
            log.info("HTTP CLIENT DISCONNECTED")
        except Exception:
            log.exception("HTTP media failed")
            try:
                self.send_text(500, "Stream failed")
            except Exception:
                pass

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if path != WEBHOOK_PATH:
            self.send_text(404, "Not found")
            return
        if not secrets.compare_digest(
            self.headers.get("X-Telegram-Bot-Api-Secret-Token", ""),
            webhook_secret(),
        ):
            self.send_response(403)
            self.end_headers()
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 2 * 1024 * 1024:
                self.send_text(400, "Bad request")
                return
            update = json.loads(self.rfile.read(length).decode("utf-8"))
            # Telegram gets a fast 200 response; bot processing continues asynchronously.
            self.send_text(200, "OK")
            if MAIN_LOOP and MAIN_LOOP.is_running():
                MAIN_LOOP.call_soon_threadsafe(dispatch_update, update)
        except Exception:
            log.exception("Webhook request failed")
            try:
                self.send_text(500, "Webhook error")
            except Exception:
                pass

    def do_HEAD(self):
        path = urllib.parse.urlparse(self.path).path
        if path.startswith("/stream/") or path.startswith("/download/"):
            code = path.split("/", 2)[2]
            item = get_link(code)
            if not item:
                self.send_response(404)
                self.end_headers()
                return
            size = int(item.get("size") or 0)
            mime = item.get("mime") or "application/octet-stream"
            self.send_response(200)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(size))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            return
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/health":
            self.send_text(200, "OK")
            return
        if path == "/":
            self.send_text(200, "True Stream V6 is running\n")
            return
        if path.startswith("/show/"):
            code = path.split("/", 2)[2]
            item = get_link(code)
            if not item:
                self.send_text(404, "Link not found or expired")
                return
            self.send_text(200, webpage(item, code), "text/html; charset=utf-8")
            return
        if path.startswith("/stream/") or path.startswith("/download/"):
            code = path.split("/", 2)[2]
            item = get_link(code)
            if not item:
                self.send_text(404, "Link not found or expired")
                return
            self.serve_media(item, download=path.startswith("/download/"))
            return
        self.send_text(404, "Not found")


def start_http():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    log.info("HTTP server listening on 0.0.0.0:%s", PORT)
    server.serve_forever()

# ----------------------------
# Cleanup
# ----------------------------

def cleanup_expired_links():
    try:
        rows = sb_request(
            "GET", "links",
            params={"expires_at": f"lte.{iso(utcnow())}", "select": "*", "limit": "200"},
        ) or []
        for item in rows:
            try:
                delete_storage_message(item)
            except Exception:
                log.exception("Could not delete expired storage message %s", item.get("id"))
            try:
                delete_link(item["id"])
            except Exception:
                log.exception("Could not delete expired DB link %s", item.get("id"))
    except Exception:
        log.exception("Expired-link cleanup failed")


async def cleanup_loop():
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        await asyncio.to_thread(cleanup_expired_links)

# ----------------------------
# Main
# ----------------------------

async def main():
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    log.info("Starting %s", APP_VERSION)

    # Pyrogram only handles MTProto operations required for storage/streaming.
    await bot.start()
    me = await bot.get_me()
    log.info("PYROGRAM CONNECTED: @%s id=%s", me.username, me.id)
    chat = await bot.get_chat(STORAGE_CHAT_ID)
    log.info("STORAGE ACCESSIBLE: %s id=%s", chat.title or chat.username, chat.id)

    threading.Thread(target=start_http, daemon=True, name="http-server").start()
    await asyncio.sleep(0.5)

    try:
        await asyncio.to_thread(tg_set_webhook)
        await asyncio.to_thread(tg_set_commands)
        log.info("TELEGRAM WEBHOOK ACTIVE: %s%s", BASE_URL, WEBHOOK_PATH)
    except Exception:
        log.exception("Could not configure Telegram webhook")
        raise

    asyncio.create_task(cleanup_loop())

    await asyncio.Event().wait()


if __name__ == "__main__":
    # Keep the same event loop for Pyrogram and the Bot API/HTTP bridge.
    # This avoids the Python 3.14 + Pyrogram loop mismatch.
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        try:
            loop.run_until_complete(bot.stop())
        except Exception:
            pass
        loop.close()
