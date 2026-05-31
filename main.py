"""
╭━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╮
  📌 Pinterest DL Bot — Premium
  Fast • Clean • Reliable
╰━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╯
"""

# ═══════════════════════════════════════
# IMPORTS
# ═══════════════════════════════════════

import re
import os
import asyncio
import logging
import tempfile
import time
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient

from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InlineQuery,
    InlineQueryResultArticle,
    InlineQueryResultPhoto,
    InlineQueryResultVideo,
    InputTextMessageContent,
    CallbackQuery,
    Message,
)
from pyrogram.enums import ChatAction

logging.basicConfig(
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("PinterestDL")

# ═══════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

BOT_USERNAME = os.getenv("BOT_USERNAME", "yoripinbot")
OWNER_ID = int(os.getenv("OWNER_ID", "7728424218"))

START_IMAGE = "https://files.catbox.moe/u5xnzb.png"
MONGO_URI = os.getenv(
    "MONGO_URI")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) "
        "AppleWebKit/537.36 Chrome/125.0.6422.113 Mobile Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

PINTEREST_REGEX = r"(https?://(?:www\.)?(?:pin\.it|pinterest\.\w+)/[^\s]+)"
MEDIA_CAPTION = "📌 ᴘɪɴᴛᴇʀsᴛ ᴅʟ\n@YoriFederation"

# ═══════════════════════════════════════
# MONGODB
# ═══════════════════════════════════════

mongo_client = AsyncIOMotorClient(MONGO_URI)
mongo_db = mongo_client["pinterest_bot"]
users_col = mongo_db["users"]
cache_col = mongo_db["cache"]

# ═══════════════════════════════════════
# AIOHTTP SESSION
# ═══════════════════════════════════════

_aio_session: aiohttp.ClientSession | None = None


async def get_session() -> aiohttp.ClientSession:
    global _aio_session
    if _aio_session is None or _aio_session.closed:
        _aio_session = aiohttp.ClientSession(headers=HEADERS)
    return _aio_session


# ═══════════════════════════════════════
# PYROGRAM CLIENT
# ═══════════════════════════════════════

app = Client(
    "pinterest_dl",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# ═══════════════════════════════════════
# PER-USER QUEUE
# ═══════════════════════════════════════

user_locks: dict[str, asyncio.Lock] = {}
user_queue_ctr: dict[str, int] = {}


def _get_lock(uid: str) -> asyncio.Lock:
    if uid not in user_locks:
        user_locks[uid] = asyncio.Lock()
    return user_locks[uid]


# ═══════════════════════════════════════
# AUTO-DELETE HELPER
# ═══════════════════════════════════════

async def _delete_after(message: Message, delay: int = 30):
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass


# ═══════════════════════════════════════
# DATABASE HELPERS
# ═══════════════════════════════════════

async def _ensure_user(uid: str) -> dict:
    user = await users_col.find_one({"_id": uid})
    if not user:
        user = {
            "_id": uid,
            "username": "",
            "first_name": "",
            "history": [],
            "bulk_mode": False,
            "search_mode": False,
            "search_results": [],
            "search_offset": 0,
            "search_query": "",
        }
        await users_col.insert_one(user)
    return user


async def _register_user(uid: str, username: str = "", first_name: str = ""):
    await users_col.update_one(
        {"_id": uid},
        {
            "$setOnInsert": {
                "history": [],
                "bulk_mode": False,
                "search_mode": False,
                "search_results": [],
                "search_offset": 0,
                "search_query": "",
            },
            "$set": {
                "username": username,
                "first_name": first_name,
            },
        },
        upsert=True,
    )


# ═══════════════════════════════════════
# MEDIA EXTRACTION — Pinterest
# ═══════════════════════════════════════

def extract_url(text: str) -> str | None:
    m = re.search(PINTEREST_REGEX, text)
    return m.group(1) if m else None


async def expand_short(url: str) -> str:
    session = await get_session()
    try:
        async with session.get(
            url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            return str(resp.url)
    except Exception:
        return url


def _upgrade(img_url: str) -> str:
    """Replace known size paths with /originals/."""
    for s in ("/236x/", "/170x/", "/474x/", "/564x/", "/736x/"):
        if s in img_url:
            return img_url.replace(s, "/originals/")
    return img_url


async def get_media(url: str) -> list[dict] | None:
    """Return list of {'type':'image'|'video', 'url':…} or None."""
    session = await get_session()
    try:
        async with session.get(
            url, timeout=aiohttp.ClientTimeout(total=25)
        ) as resp:
            html = await resp.text()
    except Exception as exc:
        logger.error("fetch fail %s — %s", url, exc)
        return None

    # ── VIDEO ──────────────────────────
    vid = re.search(
        r"https://v\d+\.pinimg\.com/videos/[^\s\"'\\]+\.mp4", html
    )
    if not vid:
        vid = re.search(r"\"url\"\s*:\s*\"(https?://[^\"]+\.mp4[^\"]*)\"", html)
    if vid:
        vurl = (vid.group(1) if vid.lastindex else vid.group(0)).replace(
            "\\/", "/"
        )
        return [{"type": "video", "url": vurl}]

    # ── IMAGES — originals ─────────────
    origs = re.findall(
        r"https://i\.pinimg\.com/originals/[^\s\"'\\<>]+", html
    )
    if origs:
        uniq = list(dict.fromkeys(u.replace("\\/", "/") for u in origs))
        return [{"type": "image", "url": u} for u in uniq]

    # ── IMAGES — any pinimg ─────────────
    all_img = re.findall(
        r"https://i\.pinimg\.com/[^\s\"'\\<>]+", html
    )
    if all_img:
        uniq = list(dict.fromkeys(u.replace("\\/", "/") for u in all_img))
        return [{"type": "image", "url": _upgrade(u)} for u in uniq[:10]]

    # ── og:image fallback ──────────────
    soup = BeautifulSoup(html, "html.parser")
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return [{"type": "image", "url": og["content"]}]

    return None


async def _download(url: str, path: str):
    session = await get_session()
    async with session.get(
        url, timeout=aiohttp.ClientTimeout(total=180)
    ) as resp:
        resp.raise_for_status()
        with open(path, "wb") as f:
            async for chunk in resp.content.iter_chunked(8192):
                f.write(chunk)


# ═══════════════════════════════════════
# PINTEREST TEXT SEARCH
# ═══════════════════════════════════════

async def search_pinterest(query: str, max_results: int = 20) -> list[str]:
    """Search Pinterest by text and return best-quality image URLs."""
    import urllib.parse

    encoded = urllib.parse.quote(query, safe="")

    url = (
        f"https://www.pinterest.com/search/pins/?q={encoded}"
        f"&rs=typed&etslf=4966"
    )
    session = await get_session()
    try:
        async with session.get(
            url,
            headers={
                **HEADERS,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://www.pinterest.com/",
            },
            timeout=aiohttp.ClientTimeout(total=25),
        ) as resp:
            html = await resp.text()
    except Exception as exc:
        logger.error("Pinterest search failed for '%s': %s", query, exc)
        return []

    # collect all pinimg URLs from HTML + embedded JSON
    raw = re.findall(
        r"https://i\.pinimg\.com/[^\s\"'\\<>)\]\s]+", html
    )

    originals = []
    high = []
    mid = []
    seen = set()

    for u in raw:
        u = u.replace("\\/", "/").split("?")[0]
        if u in seen or "avatar" in u or "user" in u or "logo" in u:
            continue
        seen.add(u)

        if "/originals/" in u:
            originals.append(u)
        elif any(s in u for s in ("/736x/", "/564x/")):
            high.append(_upgrade(u))
        elif any(s in u for s in ("/474x/", "/236x/", "/170x/")):
            mid.append(_upgrade(u))

    # priority: originals > high > mid — dedup
    result = []
    added = set()
    for pool in (originals, high, mid):
        for u in pool:
            if u not in added:
                added.add(u)
                result.append(u)

    return result[:max_results]


# ═══════════════════════════════════════
# KEYBOARDS
# ═══════════════════════════════════════

def kb_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🫴 MY LORD", url="https://t.me/yorichiiprime"),
            ],
            [
                InlineKeyboardButton("🤝 Updates", url="https://t.me/YoriFederation"),
                InlineKeyboardButton("🫰 Support", url="https://t.me/youryori7"),
            ],
            [
                InlineKeyboardButton("📥 Bulk Download", callback_data="bulk"),
                InlineKeyboardButton("📜 History", callback_data="history"),
            ],
            [
                InlineKeyboardButton("❓ Help", callback_data="help"),
            ],
        ]
    )


def kb_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="back")]]
    )


def kb_bulk() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Done", callback_data="bulk_done"),
                InlineKeyboardButton("❌ Cancel", callback_data="bulk_cancel"),
            ]
        ]
    )


def kb_search(has_more: bool = False) -> InlineKeyboardMarkup:
    rows = []
    if has_more:
        rows.append(
            [InlineKeyboardButton("🔍 ᴍᴏʀᴇ ʀᴇsᴜʟᴛs", callback_data="search_more")]
        )
    rows.append(
        [
            InlineKeyboardButton("✅ Done", callback_data="search_done"),
            InlineKeyboardButton("❌ Cancel", callback_data="search_cancel"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def kb_history(has_items: bool) -> InlineKeyboardMarkup:
    rows = []
    if has_items:
        rows.append(
            [
                InlineKeyboardButton("📥 ɢᴇᴛ ᴀʟʟ", callback_data="hist_get"),
                InlineKeyboardButton("🗑️ ᴄʟᴇᴀʀ", callback_data="hist_clear"),
            ]
        )
    rows.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="back")])
    return InlineKeyboardMarkup(rows)


# ═══════════════════════════════════════
# PRETTY TEXT SNIPPETS
# ═══════════════════════════════════════

CAPTION_START = (
    "╭━━━━━ 📌 ━━━━━╮\n"
    "ᴘɪɴᴛᴇʀsᴛ ᴅʟ\n"
    "╰━━━━━━━━━━━━━╯\n\n"
    "sᴇɴᴅ ᴀ ᴘɪɴ ʟɪɴᴋ ᴛᴏ:\n\n"
    "▸ ɢᴇᴛ ɪᴍᴀɢᴇs\n"
    "▸ ɢᴇᴛ ᴠɪᴅᴇᴏs\n"
    "▸ ɢᴇᴛ ʙᴇsᴛ ǫᴜᴀʟɪᴛʏ\n\n"
    "⚡ ғᴀsᴛ • ᴄʟᴇᴀɴ • ʀᴇʟɪᴀʙʟᴇ"
)

TEXT_HELP = (
    "╭━━━━━ ❓ ━━━━━╮\n"
    "ɢᴜɪᴅᴇ — ʜᴏᴡ ᴛᴏ ᴜsᴇ\n"
    "╰━━━━━━━━━━━━━╯\n\n"
    "📌 sɪɴɢʟᴇ ᴅᴏᴡɴʟᴏᴀᴅ:\n"
    "▸ ᴊᴜsᴛ sᴇɴᴅ ᴀ ᴘɪɴᴛᴇʀsᴛ ʟɪɴᴋ\n"
    "▸ ʙᴏᴛ ᴀᴜᴛᴏ ғᴇᴛᴄʜᴇs ʜǫ ᴍᴇᴅɪᴀ\n"
    "▸ ɢᴇᴛs ɪᴍᴀɢᴇs + ᴠɪᴅᴇᴏs\n\n"
    "🔍 ᴛᴇxᴛ sᴇᴀʀᴄʜ:\n"
    "▸ ᴜsᴇ /search <ᴛᴇxᴛ> ɪɴ ᴀɴʏ ᴄʜᴀᴛ\n"
    f"▸ ᴏʀ ᴛʏᴘᴇ @{BOT_USERNAME} <ᴛᴇxᴛ> ɪɴʟɪɴᴇ\n"
    "▸ ɢᴇᴛ ʀᴇʟᴇᴠᴀɴᴛ ɪᴍᴀɢᴇs ɪɴsᴛᴀɴᴛʟʏ\n\n"
    "📥 ʙᴜʟᴋ ᴅᴏᴡɴʟᴏᴀᴅ:\n"
    "▸ ᴛᴀᴘ \"ʙᴜʟᴋ ᴅᴏᴡɴʟᴏᴀᴅ\" ʙᴜᴛᴛᴏɴ\n"
    "▸ sᴇɴᴅ ᴍᴜʟᴛɪᴘʟᴇ ʟɪɴᴋs ᴏɴᴇ ʙʏ ᴏɴᴇ\n"
    "▸ ᴇᴀᴄʜ ʟɪɴᴋ ᴘʀᴏᴄᴇssᴇᴅ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ\n"
    "▸ ᴛᴀᴘ \"ᴅᴏɴᴇ\" ᴡʜᴇɴ ꜰɪɴɪsʜᴇᴅ\n\n"
    "📜 ᴅᴏᴡɴʟᴏᴀᴅ ʜɪsᴛᴏʀʏ:\n"
    "▸ ᴛᴀᴘ \"ʜɪsᴛᴏʀʏ\" ᴛᴏ sᴇᴇ ᴀʟʟ\n"
    "▸ ʀᴇ-ᴅᴏᴡɴʟᴏᴀᴅ ᴀɴʏᴛɪᴍᴇ ɪɴsᴛᴀɴᴛʟʏ ⚡\n\n"
    "💡 ᴘʀᴏ ᴛɪᴘs:\n"
    f"▸ ᴜsᴇ @{BOT_USERNAME} <link> ɪɴ ᴀɴʏ ᴄʜᴀᴛ\n"
    "▸ sᴀᴍᴇ ʟɪɴᴋ = ɪɴsᴛᴀɴᴛ ᴅᴏᴡɴʟᴏᴀᴅ ⚡\n"
    "▸ ᴜɴʟɪᴍɪᴛᴇᴅ — ɴᴏ ʀᴀᴛᴇ ʟɪᴍɪᴛs\n"
    "▸ ǫᴜᴀʟɪᴛʏ > ǫᴜᴀɴᴛɪᴛʏ"
)

CAPTION_BULK = (
    "╭━━━━━ 📥 ━━━━━╮\n"
    "ʙᴜʟᴋ ᴅᴏᴡɴʟᴏᴀᴅ ᴍᴏᴅᴇ\n"
    "╰━━━━━━━━━━━━━╯\n\n"
    "▸ sᴇɴᴅ ᴘɪɴᴛᴇʀsᴛ ʟɪɴᴋs ᴏɴᴇ ʙʏ ᴏɴᴇ\n"
    "▸ ᴇᴀᴄʜ ʟɪɴᴋ ᴘʀᴏᴄᴇssᴇᴅ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ\n"
    "▸ ᴛᴀᴘ \"ᴅᴏɴᴇ\" ᴏʀ ᴛʏᴘᴇ /done ᴡʜᴇɴ ʀᴇᴀᴅʏ\n"
    "▸ ᴛᴀᴘ \"ᴄᴀɴᴄᴇʟ\" ᴏʀ ᴛʏᴘᴇ /cancel ᴛᴏ ᴇxɪᴛ"
)

TEXT_BULK_EXIT = (
    "╭━━━━━━━━━━━━━━━━━━╮\n"
    "  ✅ ʙᴜʟᴋ ᴍᴏᴅᴇ ᴇxɪᴛᴇᴅ\n"
    "╰━━━━━━━━━━━━━━━━━━╯\n\n"
    "ʏᴏᴜ'ʀᴇ ʙᴀᴄᴋ ᴛᴏ ɴᴏʀᴍᴀʟ ᴍᴏᴅᴇ"
)

CAPTION_SEARCH = (
    "╭━━━━━ 🔍 ━━━━━╮\n"
    "ᴘɪɴᴛᴇʀsᴛ sᴇᴀʀᴄʜ\n"
    "╰━━━━━━━━━━━━━╯\n\n"
    "ᴛʏᴘᴇ ᴀɴʏᴛʜɪɴɢ ᴛᴏ sᴇᴀʀᴄʜ ᴘɪɴᴛᴇʀsᴛ ɪᴍᴀɢᴇs!\n\n"
    "▸ ᴛʏᴘᴇ ʏᴏᴜʀ sᴇᴀʀᴄʜ ᴛᴇʀᴍ\n"
    "▸ ɢᴇᴛ ᴛᴏᴘ ʀᴇsᴜʟᴛs ɪɴsᴛᴀɴᴛʟʏ\n"
    "▸ ᴛᴀᴘ \"ᴍᴏʀᴇ\" ꜰᴏʀ ᴀᴅᴅɪᴛɪᴏɴᴀʟ ɪᴍᴀɢᴇs\n\n"
    "ᴏʀ ᴜsᴇ /search <ᴛᴇxᴛ> ᴛᴏ sᴇᴀʀᴄʜ ᴅɪʀᴇᴄᴛʟʏ"
)

TEXT_SEARCH_EXIT = (
    "╭━━━━━━━━━━━━━━━━━━╮\n"
    "  ✅ sᴇᴀʀᴄʜ ᴍᴏᴅᴇ ᴇxɪᴛᴇᴅ\n"
    "╰━━━━━━━━━━━━━━━━━━╯\n\n"
    "ʏᴏᴜ'ʀᴇ ʙᴀᴄᴋ ᴛᴏ ɴᴏʀᴍᴀʟ ᴍᴏᴅᴇ"
)

# ═══════════════════════════════════════
# STATUS ANIMATION FRAMES
# ═══════════════════════════════════════

FRAME_FETCH = (
    "╭━━━━━━━━━━━━━━━━━━╮\n"
    "  ⬇️ ғᴇᴛᴄʜɪɴɢ\n"
    "╰━━━━━━━━━━━━━━━━━━╯\n\n"
    "ʟᴏᴏᴋɪɴɢ ᴜᴘ ʏᴏᴜʀ ᴘɪɴ…"
)

FRAME_DL_40 = (
    "╭━━━━━━━━━━━━━━━━━━╮\n"
    "  ⬇️ ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ\n"
    "╰━━━━━━━━━━━━━━━━━━╯\n\n"
    "[████░░░░░░] 40 %"
)

FRAME_DL_70 = (
    "╭━━━━━━━━━━━━━━━━━━╮\n"
    "  ⬇️ ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ\n"
    "╰━━━━━━━━━━━━━━━━━━╯\n\n"
    "[███████░░░] 70 %"
)

FRAME_DL_100 = (
    "╭━━━━━━━━━━━━━━━━━━╮\n"
    "  ⬇️ ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ\n"
    "╰━━━━━━━━━━━━━━━━━━╯\n\n"
    "[██████████] 100 %"
)

FRAME_SEND = (
    "╭━━━━━━━━━━━━━━━━━━╮\n"
    "  ⬆️ sᴇɴᴅɪɴɢ\n"
    "╰━━━━━━━━━━━━━━━━━━╯\n\n"
    "ᴅᴇʟɪᴠᴇʀɪɴɢ ᴛᴏ ʏᴏᴜ…"
)

FRAME_CACHE = (
    "╭━━━━━━━━━━━━━━━━━━╮\n"
    "  ⚡ ɪɴsᴛᴀɴᴛ\n"
    "╰━━━━━━━━━━━━━━━━━━╯\n\n"
    "ɢᴏᴛ ɪᴛ ꜰʀᴏᴍ ᴄᴀᴄʜᴇ! sᴇɴᴅɪɴɢ…"
)

FRAME_DONE = "✅ ᴅᴏɴᴇ!"

FRAME_SEARCHING = (
    "╭━━━━━━━━━━━━━━━━━━╮\n"
    "  🔍 sᴇᴀʀᴄʜɪɴɢ\n"
    "╰━━━━━━━━━━━━━━━━━━╯\n\n"
    "ʟᴏᴏᴋɪɴɢ ᴜᴘ ɪᴍᴀɢᴇs…"
)


def _queue_frame(n: int) -> str:
    return (
        "╭━━━━━━━━━━━━━━━━━━╮\n"
        f"  ⏳ ǫᴜᴇᴜᴇ #{n}\n"
        "╰━━━━━━━━━━━━━━━━━━╯\n\n"
        "ᴘʟᴇᴀsᴇ ᴡᴀɪᴛ ʏᴏᴜʀ ᴛᴜʀɴ… ⏳"
    )


def _error_frame(msg: str) -> str:
    return (
        "╭━━━━━━━━━━━━━━━━━━╮\n"
        "  ❌ ᴇʀʀᴏʀ\n"
        "╰━━━━━━━━━━━━━━━━━━╯\n\n"
        f"{msg}"
    )


# ═══════════════════════════════════════
# CORE DOWNLOAD PROCESSOR
# ═══════════════════════════════════════


async def process_download(client, message: Message, url: str, uid: str):
    """Download a Pinterest URL, send media, cache & store history."""

    lock = _get_lock(uid)

    # ── queue tracking ─────────────────
    queue_num = 0
    if lock.locked():
        user_queue_ctr[uid] = user_queue_ctr.get(uid, 0) + 1
        queue_num = user_queue_ctr[uid]

    # ── delete user's link msg ──────────
    try:
        await message.delete()
    except Exception:
        pass

    # ── initial status ─────────────────
    if queue_num > 0:
        status = await client.send_message(
            message.chat.id, _queue_frame(queue_num)
        )
    else:
        status = await client.send_message(
            message.chat.id, FRAME_FETCH
        )

    async with lock:
        # decrement counter
        if uid in user_queue_ctr:
            user_queue_ctr[uid] = max(0, user_queue_ctr.get(uid, 0) - 1)

        try:
            await _do_download(client, message, status, url, uid)
        except Exception as exc:
            logger.exception("download error for %s", url)
            try:
                await status.edit_text(
                    _error_frame("sᴏᴍᴇᴛʜɪɴɢ ᴡᴇɴᴛ ᴡʀᴏɴɢ.\nᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ.")
                )
            except Exception:
                pass


async def _do_download(client, message, status, url, uid):
    """Inner download logic — already inside per-user lock."""

    # ── expand short URLs ───────────────
    expanded = url
    if "pin.it" in url:
        expanded = await expand_short(url)

    # ── multi-pin share guard ───────────
    if "multi-pin-share" in expanded:
        await status.edit_text(
            "╭━━━━━━━━━━━━━━━━━━╮\n"
            "  ⚠️ ᴍᴜʟᴛɪ-ᴘɪɴ ʟɪɴᴋ\n"
            "╰━━━━━━━━━━━━━━━━━━╯\n\n"
            "ᴘɪɴᴛᴇʀsᴛ ᴅᴏᴇsɴ'ᴛ ᴇxᴘᴏsᴇ ᴇᴀᴄʜ ɪᴍᴀɢᴇ\n"
            "ɪɴ ᴀ ᴍᴜʟᴛɪ-ᴘɪɴ sʜᴀʀᴇ ʟɪɴᴋ.\n\n"
            "▸ ᴏᴘᴇɴ ᴛʜᴇ ʟɪɴᴋ\n"
            "▸ ᴛᴀᴘ ᴇᴀᴄʜ ɪᴍᴀɢᴇ\n"
            "▸ sᴇɴᴅ ᴛʜᴏsᴇ ʟɪɴᴋs ᴏɴᴇ ʙʏ ᴏɴᴇ"
        )
        return

    # ── check cache ─────────────────────
    cached = await cache_col.find_one({"_id": expanded})

    if cached and cached.get("file_ids"):
        # INSTANT from cache ⚡
        await status.edit_text(FRAME_CACHE)

        fids = cached["file_ids"]
        mtype = cached["type"]

        if mtype == "video":
            await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_VIDEO)
            for fid in fids:
                sent = await client.send_video(
                    message.chat.id,
                    fid,
                    supports_streaming=True,
                    caption=MEDIA_CAPTION,
                )
                asyncio.create_task(_delete_after(sent, 30))
        else:
            await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_PHOTO)
            for fid in fids:
                sent = await client.send_photo(
                    message.chat.id,
                    fid,
                    caption=MEDIA_CAPTION,
                )
                asyncio.create_task(_delete_after(sent, 30))

        await status.edit_text(FRAME_DONE)
        await asyncio.sleep(2)
        try:
            await status.delete()
        except Exception:
            pass
        return

    # ── not cached — fetch from Pinterest ─
    await status.edit_text(FRAME_DL_40)

    media_list = await get_media(expanded)

    if not media_list:
        await status.edit_text(
            _error_frame("ᴄᴏᴜʟᴅɴ'ᴛ ꜰɪɴᴅ ᴀɴʏ ᴍᴇᴅɪᴀ.\nᴛʀʏ ᴀ ᴅɪꜰꜰᴇʀᴇɴᴛ ʟɪɴᴋ?")
        )
        return

    await status.edit_text(FRAME_DL_70)

    # ── download & send each item ────────
    file_ids: list[str] = []
    direct_urls: list[str] = []
    media_type = media_list[0]["type"]

    with tempfile.TemporaryDirectory() as tmp:
        for idx, media in enumerate(media_list):
            ext = ".mp4" if media["type"] == "video" else ".jpg"
            fpath = os.path.join(tmp, f"pin_{idx}{ext}")

            # download to disk
            try:
                await _download(media["url"], fpath)
            except Exception as dl_err:
                logger.warning("download failed %s — %s", media["url"], dl_err)
                # if quality-upgraded URL failed, skip
                continue

            direct_urls.append(media["url"])

            # sending animation update
            if len(media_list) > 1:
                await status.edit_text(
                    f"╭━━━━━━━━━━━━━━━━━━╮\n"
                    f"  ⬆️ sᴇɴᴅɪɴɢ\n"
                    f"╰━━━━━━━━━━━━━━━━━━╯\n\n"
                    f"sᴇɴᴅɪɴɢ {idx + 1}/{len(media_list)}…"
                )
            else:
                await status.edit_text(FRAME_DL_100)
                await asyncio.sleep(0.3)
                await status.edit_text(FRAME_SEND)

            # send to user
            try:
                if media["type"] == "video":
                    await client.send_chat_action(
                        message.chat.id, ChatAction.UPLOAD_VIDEO
                    )
                    sent = await client.send_video(
                        message.chat.id,
                        fpath,
                        supports_streaming=True,
                        caption=MEDIA_CAPTION,
                    )
                    file_ids.append(sent.video.file_id)
                else:
                    await client.send_chat_action(
                        message.chat.id, ChatAction.UPLOAD_PHOTO
                    )
                    sent = await client.send_photo(
                        message.chat.id,
                        fpath,
                        caption=MEDIA_CAPTION,
                    )
                    file_ids.append(sent.photo.file_id)
                asyncio.create_task(_delete_after(sent, 30))
            except Exception as send_err:
                logger.warning("send failed — %s", send_err)

    if not file_ids:
        await status.edit_text(
            _error_frame("ᴅᴏᴡɴʟᴏᴀᴅ ꜰᴀɪʟᴇᴅ.\nᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ.")
        )
        return

    # ── cache + history ─────────────────
    await cache_col.update_one(
        {"_id": expanded},
        {
            "$set": {
                "file_ids": file_ids,
                "direct_urls": direct_urls,
                "type": media_type,
            }
        },
        upsert=True,
    )

    entry = {
        "pin_url": expanded,
        "file_ids": file_ids,
        "direct_urls": direct_urls,
        "type": media_type,
        "date": datetime.now().isoformat(),
        "count": len(file_ids),
    }
    await users_col.update_one(
        {"_id": uid},
        {"$push": {"history": {"$each": [entry], "$slice": -200}}},
        upsert=True,
    )

    # ── done ────────────────────────────
    await status.edit_text(FRAME_DONE)
    await asyncio.sleep(2)
    try:
        await status.delete()
    except Exception:
        pass


# ═══════════════════════════════════════
# PINTEREST TEXT SEARCH PROCESSOR
# ═══════════════════════════════════════

SEARCH_BATCH = 5


async def process_search(client, message: Message, query: str, uid: str):
    """Search Pinterest by text and send image results."""

    # delete user's message
    try:
        await message.delete()
    except Exception:
        pass

    status = await client.send_message(
        message.chat.id,
        FRAME_SEARCHING
        + f'\n\nsᴇᴀʀᴄʜɪɴɢ: "{query}"'
    )

    results = await search_pinterest(query, max_results=20)

    if not results:
        await status.edit_text(
            _error_frame(
                f'ɴᴏ ɪᴍᴀɢᴇs ꜰᴏᴜɴᴅ ꜰᴏʀ:\n"{query}"\n\n'
                "ᴛʀʏ ᴀ ᴅɪꜰꜰᴇʀᴇɴᴛ sᴇᴀʀᴄʜ ᴛᴇʀᴍ"
            )
        )
        return

    # store results
    await users_col.update_one(
        {"_id": uid},
        {
            "$set": {
                "search_results": results,
                "search_offset": 0,
                "search_query": query,
            }
        },
        upsert=True,
    )

    # send first batch
    batch = results[:SEARCH_BATCH]
    await _send_search_batch(client, message.chat.id, batch, status, len(results))


async def _send_search_batch(client, chat_id, batch, status, total):
    """Send a batch of search result images."""
    sent = 0
    with tempfile.TemporaryDirectory() as tmp:
        for idx, img_url in enumerate(batch):
            fpath = os.path.join(tmp, f"search_{idx}.jpg")
            try:
                await _download(img_url, fpath)
            except Exception:
                continue

            try:
                await client.send_chat_action(chat_id, ChatAction.UPLOAD_PHOTO)
                sent_msg = await client.send_photo(
                    chat_id,
                    fpath,
                    caption=MEDIA_CAPTION,
                )
                asyncio.create_task(_delete_after(sent_msg, 30))
                sent += 1
            except Exception:
                pass

    # status with more button or done
    remaining = total - SEARCH_BATCH  # approximate
    if total > SEARCH_BATCH:
        await status.edit_text(
            "╭━━━━━ 🔍 ━━━━━╮\n"
            "sᴇᴀʀᴄʜ ʀᴇsᴜʟᴛs\n"
            "╰━━━━━━━━━━━━━╯\n\n"
            f"✅ sᴇɴᴛ {sent} ɪᴍᴀɢᴇs\n"
            f"📦 {total - sent} ᴍᴏʀᴇ ᴀᴠᴀɪʟᴀʙʟᴇ",
            reply_markup=kb_search(has_more=True),
        )
    else:
        await status.edit_text(
            "╭━━━━━ 🔍 ━━━━━╮\n"
            "sᴇᴀʀᴄʜ ᴅᴏɴᴇ\n"
            "╰━━━━━━━━━━━━━╯\n\n"
            f"✅ sᴇɴᴛ {sent} ɪᴍᴀɢᴇ(s)"
        )
        await asyncio.sleep(2)
        try:
            await status.delete()
        except Exception:
            pass


async def _exit_search(uid: str):
    await users_col.update_one(
        {"_id": uid},
        {
            "$set": {
                "search_mode": False,
                "search_results": [],
                "search_offset": 0,
                "search_query": "",
            }
        },
    )


async def _exit_bulk(uid: str):
    await users_col.update_one(
        {"_id": uid},
        {"$set": {"bulk_mode": False}},
        upsert=True,
    )


# ═══════════════════════════════════════
# /START
# ═══════════════════════════════════════


@app.on_message(filters.command("start"))
async def cmd_start(client, message: Message):
    # deep-link support  /start dl_<encoded_url>
    if len(message.command) > 1:
        param = message.command[1]
        if param.startswith("dl_"):
            import urllib.parse

            pin_url = urllib.parse.unquote(param[3:])
            if pin_url:
                uid = str(message.from_user.id)
                await process_download(client, message, pin_url, uid)
                return

    # register / update user info
    uid = str(message.from_user.id)
    uname = message.from_user.username or ""
    fname = message.from_user.first_name or ""

    await _register_user(uid, uname, fname)

    await message.reply_photo(
        photo=START_IMAGE,
        caption=CAPTION_START,
        reply_markup=kb_main(),
    )


# ═══════════════════════════════════════
# /HELP
# ═══════════════════════════════════════


@app.on_message(filters.command("help"))
async def cmd_help(client, message: Message):
    await message.reply_text(TEXT_HELP, reply_markup=kb_back())


# ═══════════════════════════════════════
# /HISTORY
# ═══════════════════════════════════════


@app.on_message(filters.command("history"))
async def cmd_history(client, message: Message):
    uid = str(message.from_user.id)
    user = await _ensure_user(uid)
    history = user.get("history", [])

    if not history:
        await message.reply_text(
            "╭━━━━━ 📜 ━━━━━╮\n"
            "ʜɪsᴛᴏʀʏ\n"
            "╰━━━━━━━━━━━━━╯\n\n"
            "❌ ɴᴏ ᴅᴏᴡɴʟᴏᴀᴅs ʏᴇᴛ\n\n"
            "sᴇɴᴅ ᴀ ᴘɪɴᴛᴇʀsᴛ ʟɪɴᴋ ᴛᴏ sᴛᴀʀᴛ!",
            reply_markup=kb_back(),
        )
        return

    await _send_history_list(client, message.chat.id, history, as_new=True)


# ═══════════════════════════════════════
# /CLEAR
# ═══════════════════════════════════════


@app.on_message(filters.command("clear"))
async def cmd_clear(client, message: Message):
    uid = str(message.from_user.id)
    await users_col.update_one(
        {"_id": uid},
        {"$set": {"history": []}},
        upsert=True,
    )
    await message.reply_text(
        "╭━━━━━ 🗑️ ━━━━━╮\n"
        "ʜɪsᴛᴏʀʏ ᴄʟᴇᴀʀᴇᴅ\n"
        "╰━━━━━━━━━━━━━╯\n\n"
        "✅ ᴀʟʟ ʏᴏᴜʀ ᴅᴏᴡɴʟᴏᴀᴅ ʜɪsᴛᴏʀʏ ʜᴀs ʙᴇᴇɴ ᴅᴇʟᴇᴛᴇᴅ"
    )


# ═══════════════════════════════════════
# /DONE  /CANCEL  (bulk mode exit)
# ═══════════════════════════════════════


@app.on_message(filters.command("done"))
async def cmd_done(client, message: Message):
    uid = str(message.from_user.id)
    user = await _ensure_user(uid)
    if user.get("bulk_mode"):
        await _exit_bulk(uid)
        await message.reply_text(TEXT_BULK_EXIT, reply_markup=kb_main())


@app.on_message(filters.command("cancel"))
async def cmd_cancel(client, message: Message):
    uid = str(message.from_user.id)
    user = await _ensure_user(uid)
    if user.get("bulk_mode"):
        await _exit_bulk(uid)
        await message.reply_text(
            "╭━━━━━━━━━━━━━━━━━━╮\n"
            "  ❌ ʙᴜʟᴋ ᴄᴀɴᴄᴇʟʟᴇᴅ\n"
            "╰━━━━━━━━━━━━━━━━━━╯\n\n"
            "ʏᴏᴜ'ʀᴇ ʙᴀᴄᴋ ᴛᴏ ɴᴏʀᴍᴀʟ ᴍᴏᴅᴇ",
            reply_markup=kb_main(),
        )


# ═══════════════════════════════════════
# CALLBACK QUERY HANDLER
# ═══════════════════════════════════════


@app.on_callback_query()
async def on_callback(client, cb: CallbackQuery):
    uid = str(cb.from_user.id)
    data = cb.data

    # ── BACK ────────────────────────────
    if data == "back":
        try:
            await cb.message.edit_caption(
                caption=CAPTION_START, reply_markup=kb_main()
            )
        except Exception:
            # might be a text-only message
            try:
                await cb.message.delete()
            except Exception:
                pass
            await cb.message.reply_photo(
                photo=START_IMAGE,
                caption=CAPTION_START,
                reply_markup=kb_main(),
            )
        await cb.answer()
        return

    # ── HELP ────────────────────────────
    if data == "help":
        try:
            await cb.message.edit_caption(
                caption=TEXT_HELP, reply_markup=kb_back()
            )
        except Exception:
            await cb.message.edit_text(TEXT_HELP, reply_markup=kb_back())
        await cb.answer()
        return

    # ── BULK MODE ON ────────────────────
    if data == "bulk":
        await users_col.update_one(
            {"_id": uid},
            {"$set": {"bulk_mode": True}},
            upsert=True,
        )
        try:
            await cb.message.edit_caption(
                caption=CAPTION_BULK, reply_markup=kb_bulk()
            )
        except Exception:
            await cb.message.edit_text(CAPTION_BULK, reply_markup=kb_bulk())
        await cb.answer("✅ ʙᴜʟᴋ ᴍᴏᴅᴇ ᴀᴄᴛɪᴠᴀᴛᴇᴅ!")
        return

    # ── BULK DONE ───────────────────────
    if data == "bulk_done":
        await _exit_bulk(uid)
        try:
            await cb.message.edit_caption(
                caption=CAPTION_START, reply_markup=kb_main()
            )
        except Exception:
            pass
        await cb.answer("✅ ʙᴜʟᴋ ᴍᴏᴅᴇ ᴇxɪᴛᴇᴅ!")
        return

    # ── BULK CANCEL ─────────────────────
    if data == "bulk_cancel":
        await _exit_bulk(uid)
        try:
            await cb.message.edit_caption(
                caption=CAPTION_START, reply_markup=kb_main()
            )
        except Exception:
            pass
        await cb.answer("❌ ʙᴜʟᴋ ᴄᴀɴᴄᴇʟʟᴇᴅ!")
        return

    # ── HISTORY ─────────────────────────
    if data == "history":
        user = await _ensure_user(uid)
        history = user.get("history", [])

        if not history:
            try:
                await cb.message.edit_caption(
                    caption=(
                        "╭━━━━━ 📜 ━━━━━╮\n"
                        "ʜɪsᴛᴏʀʏ\n"
                        "╰━━━━━━━━━━━━━╯\n\n"
                        "❌ ɴᴏ ᴅᴏᴡɴʟᴏᴀᴅs ʏᴇᴛ\n\n"
                        "sᴇɴᴅ ᴀ ᴘɪɴᴛᴇʀsᴛ ʟɪɴᴋ ᴛᴏ sᴛᴀʀᴛ!"
                    ),
                    reply_markup=kb_back(),
                )
            except Exception:
                await cb.message.edit_text(
                    "╭━━━━━ 📜 ━━━━━╮\n"
                    "ʜɪsᴛᴏʀʏ\n"
                    "╰━━━━━━━━━━━━━╯\n\n"
                    "❌ ɴᴏ ᴅᴏᴡɴʟᴏᴀᴅs ʏᴇᴛ",
                    reply_markup=kb_back(),
                )
        else:
            await _send_history_list(
                client, cb.message.chat.id, history, edit_msg=cb.message
            )

        await cb.answer()
        return

    # ── HISTORY GET ALL ─────────────────
    if data == "hist_get":
        user = await _ensure_user(uid)
        history = user.get("history", [])

        if not history:
            await cb.answer("❌ ɴᴏᴛʜɪɴɢ ᴛᴏ sᴇɴᴅ", show_alert=True)
            return

        await cb.answer("📤 sᴇɴᴅɪɴɢ ᴀʟʟ ʏᴏᴜʀ ᴅᴏᴡɴʟᴏᴀᴅs…")

        sent = 0
        for item in reversed(history[-30:]):
            for fid in item.get("file_ids", []):
                try:
                    if item["type"] == "video":
                        sent_msg = await client.send_video(
                            cb.message.chat.id,
                            fid,
                            supports_streaming=True,
                            caption=MEDIA_CAPTION,
                        )
                    else:
                        sent_msg = await client.send_photo(
                            cb.message.chat.id,
                            fid,
                            caption=MEDIA_CAPTION,
                        )
                    asyncio.create_task(_delete_after(sent_msg, 30))
                    sent += 1
                    await asyncio.sleep(0.3)  # gentle pacing
                except Exception as e:
                    logger.warning("hist resend fail — %s", e)

        await client.send_message(
            cb.message.chat.id,
            f"╭━━━━━ 📜 ━━━━━╮\n"
            f"ʜɪsᴛᴏʀʏ sᴇɴᴛ\n"
            f"╰━━━━━━━━━━━━━╯\n\n"
            f"✅ ᴅᴇʟɪᴠᴇʀᴇᴅ {sent} ꜰɪʟᴇ(s)",
        )
        return

    # ── HISTORY CLEAR ───────────────────
    if data == "hist_clear":
        await users_col.update_one(
            {"_id": uid},
            {"$set": {"history": []}},
        )
        try:
            await cb.message.edit_caption(
                caption=(
                    "╭━━━━━ 🗑️ ━━━━━╮\n"
                    "ʜɪsᴛᴏʀʏ ᴄʟᴇᴀʀᴇᴅ\n"
                    "╰━━━━━━━━━━━━━╯\n\n"
                    "✅ ᴀʟʟ ʜɪsᴛᴏʀʏ ᴅᴇʟᴇᴛᴇᴅ"
                ),
                reply_markup=kb_back(),
            )
        except Exception:
            pass
        await cb.answer("✅ ʜɪsᴛᴏʀʏ ᴄʟᴇᴀʀᴇᴅ!")
        return

    # ── SEARCH MORE ────────────────────
    if data == "search_more":
        db_user = await _ensure_user(uid)
        results = db_user.get("search_results", [])
        offset = db_user.get("search_offset", 0)

        if not results or offset >= len(results):
            await cb.answer("❌ ɴᴏ ᴍᴏʀᴇ ʀᴇsᴜʟᴛs", show_alert=True)
            return

        await cb.answer("🔍 ʟᴏᴀᴅɪɴɢ ᴍᴏʀᴇ…")

        batch = results[offset : offset + SEARCH_BATCH]
        sent = 0
        with tempfile.TemporaryDirectory() as tmp:
            for idx, img_url in enumerate(batch):
                fpath = os.path.join(tmp, f"more_{idx}.jpg")
                try:
                    await _download(img_url, fpath)
                except Exception:
                    continue
                try:
                    sent_msg = await client.send_photo(
                        cb.message.chat.id,
                        fpath,
                        caption=MEDIA_CAPTION,
                    )
                    asyncio.create_task(_delete_after(sent_msg, 30))
                    sent += 1
                except Exception:
                    pass

        new_offset = offset + SEARCH_BATCH

        await users_col.update_one(
            {"_id": uid},
            {"$set": {"search_offset": new_offset}},
        )

        remaining = len(results) - new_offset
        if remaining > 0:
            try:
                await cb.message.edit_text(
                    "╭━━━━━ 🔍 ━━━━━╮\n"
                    "sᴇᴀʀᴄʜ ʀᴇsᴜʟᴛs\n"
                    "╰━━━━━━━━━━━━━╯\n\n"
                    f"✅ sᴇɴᴛ {sent} ᴍᴏʀᴇ ɪᴍᴀɢᴇs\n"
                    f"📦 {remaining} ʀᴇᴍᴀɪɴɪɴɢ",
                    reply_markup=kb_search(has_more=True),
                )
            except Exception:
                pass
        else:
            try:
                await cb.message.edit_text(
                    "╭━━━━━ 🔍 ━━━━━╮\n"
                    "sᴇᴀʀᴄʜ ᴄᴏᴍᴘʟᴇᴛᴇ\n"
                    "╰━━━━━━━━━━━━━╯\n\n"
                    f"✅ ᴀʟʟ ʀᴇsᴜʟᴛs sᴇɴᴛ!"
                )
            except Exception:
                pass
        return

    # ── SEARCH DONE ────────────────────
    if data == "search_done":
        await _exit_search(uid)
        try:
            await cb.message.edit_caption(
                caption=CAPTION_START, reply_markup=kb_main()
            )
        except Exception:
            await cb.message.edit_text(TEXT_SEARCH_EXIT, reply_markup=kb_main())
        await cb.answer("✅ sᴇᴀʀᴄʜ ᴍᴏᴅᴇ ᴇxɪᴛᴇᴅ!")
        return

    # ── SEARCH CANCEL ──────────────────
    if data == "search_cancel":
        await _exit_search(uid)
        try:
            await cb.message.edit_caption(
                caption=CAPTION_START, reply_markup=kb_main()
            )
        except Exception:
            await cb.message.edit_text(TEXT_SEARCH_EXIT, reply_markup=kb_main())
        await cb.answer("❌ sᴇᴀʀᴄʜ ᴄᴀɴᴄᴇʟʟᴇᴅ!")
        return

    # ── HISTORY PAGE ────────────────────
    if data.startswith("histp_"):
        page = int(data.split("_", 1)[1])
        user = await _ensure_user(uid)
        history = user.get("history", [])
        await _send_history_list(
            client, cb.message.chat.id, history, page=page, edit_msg=cb.message
        )
        await cb.answer()
        return

    await cb.answer()


# ═══════════════════════════════════════
# HISTORY LIST RENDERER
# ═══════════════════════════════════════

PER_PAGE = 10


async def _send_history_list(
    client, chat_id, history, page=0, edit_msg=None, as_new=False
):
    total = len(history)
    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    page = min(page, total_pages - 1)

    start = page * PER_PAGE
    end = min(start + PER_PAGE, total)
    slice_ = list(reversed(history))[start:end]

    lines = [
        "╭━━━━━ 📜 ━━━━━╮",
        "ʏᴏᴜʀ ʜɪsᴛᴏʀʏ",
        "╰━━━━━━━━━━━━━╯\n",
    ]

    for i, item in enumerate(slice_, start + 1):
        icon = "🎬" if item["type"] == "video" else "📷"
        d = item.get("date", "")[:10]
        cnt = item.get("count", len(item.get("file_ids", [])))
        lines.append(f" {i}. {icon} {d}  ({cnt} ꜰɪʟᴇs)")

    lines.append(f"\nᴛᴏᴛᴀʟ {total} ᴅᴏᴡɴʟᴏᴀᴅ(s) • ᴘᴀɢᴇ {page + 1}/{total_pages}")

    text = "\n".join(lines)

    # buttons
    rows = []
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ ᴘʀᴇᴠ", callback_data=f"histp_{page - 1}"))
    if page + 1 < total_pages:
        nav.append(InlineKeyboardButton("ɴᴇxᴛ ➡️", callback_data=f"histp_{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append(
        [
            InlineKeyboardButton("📥 ɢᴇᴛ ᴀʟʟ", callback_data="hist_get"),
            InlineKeyboardButton("🗑️ ᴄʟᴇᴀʀ", callback_data="hist_clear"),
        ]
    )
    rows.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="back")])
    kb = InlineKeyboardMarkup(rows)

    if edit_msg:
        try:
            await edit_msg.edit_caption(caption=text, reply_markup=kb)
            return
        except Exception:
            try:
                await edit_msg.edit_text(text, reply_markup=kb)
                return
            except Exception:
                pass

    if as_new:
        await client.send_message(chat_id, text, reply_markup=kb)


# ═══════════════════════════════════════
# /SEARCH — quick search without mode
# ═══════════════════════════════════════


@app.on_message(filters.command("search"))
async def cmd_search(client, message: Message):
    uid = str(message.from_user.id)

    if message.from_user.id != OWNER_ID:
        # also register user
        uname = message.from_user.username or ""
        fname = message.from_user.first_name or ""
        await _register_user(uid, uname, fname)

    # /search <query>
    if len(message.command) > 1:
        query = " ".join(message.command[1:])
        await process_search(client, message, query, uid)
        return

    # just /search → enter search mode
    await users_col.update_one(
        {"_id": uid},
        {"$set": {"search_mode": True}},
        upsert=True,
    )

    await message.reply_text(CAPTION_SEARCH, reply_markup=kb_search())


# ═══════════════════════════════════════
# TEXT MESSAGE HANDLER (links + search + bulk)
# ═══════════════════════════════════════


@app.on_message(
    filters.text
    & ~filters.command(["start", "help", "history", "clear", "done", "cancel", "stats", "bcast", "search"])
)
async def on_text(client, message: Message):
    uid = str(message.from_user.id)
    text = message.text.strip()

    # register / update user info
    uname = message.from_user.username or ""
    fname = message.from_user.first_name or ""

    await _register_user(uid, uname, fname)

    # ── search mode: treat text as search query
    user = await _ensure_user(uid)
    if user.get("search_mode"):
        await process_search(client, message, text, uid)
        return

    # ── link mode: check for pinterest link
    url = extract_url(text)
    if url:
        await process_download(client, message, url, uid)
        return

    # ── no link, no search mode → silently ignore
    return


# ═══════════════════════════════════════
# 👑 OWNER ONLY — /stats & /bcast
# ═══════════════════════════════════════

owner_filter = filters.user(OWNER_ID)


@app.on_message(filters.command("stats") & owner_filter)
async def cmd_stats(client, message: Message):
    """👑 Owner only — send user stats as a formatted .txt file."""

    pipeline = [
        {
            "$project": {
                "username": 1,
                "first_name": 1,
                "dl_count": {"$size": {"$ifNull": ["$history", []]}},
            }
        },
        {"$sort": {"dl_count": -1}},
    ]

    rows = []
    total_users = 0
    total_downloads = 0

    async for doc in users_col.aggregate(pipeline):
        total_users += 1
        dl_count = doc.get("dl_count", 0)
        total_downloads += dl_count
        rows.append((doc["_id"], doc.get("username", ""), doc.get("first_name", ""), dl_count))

    if not rows:
        await message.reply_text("❌ ɴᴏ ᴜsᴇʀs ɪɴ ᴅᴀᴛᴀʙᴀsᴇ ʏᴇᴛ.")
        return

    # sort by total downloads descending
    rows.sort(key=lambda r: r[3], reverse=True)

    # build pretty .txt content
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sep = "─" * 72

    lines = [
        "╭━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╮",
        "  📊 ᴘɪɴᴛᴇʀsᴛ ᴅʟ ʙᴏᴛ — ᴜsᴇʀ sᴛᴀᴛs",
        "╰━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╯",
        "",
        f"  📅 ɢᴇɴᴇʀᴀᴛᴇᴅ : {now}",
        f"  👥 ᴛᴏᴛᴀʟ ᴜsᴇʀs : {total_users}",
        f"  📥 ᴛᴏᴛᴀʟ ᴅᴏᴡɴʟᴏᴀᴅs : {total_downloads}",
        "",
        sep,
        f"  {'#':<4}  {'@ᴜsᴇʀɴᴀᴍᴇ':<22}  {'ɪᴅ':<14}  {'ᴅᴏᴡɴʟᴏᴀᴅs':<10}",
        sep,
    ]

    for idx, (uid, uname, fname, dl_count) in enumerate(rows, 1):
        uname_display = f"@{uname}" if uname else f"{fname[:18]}" if fname else "—"
        lines.append(
            f"  {idx:<4}  {uname_display:<22}  {uid:<14}  {dl_count:<10}"
        )

    lines.append(sep)
    lines.append("")
    lines.append(f"  ᴛᴏᴛᴀʟ ᴜsᴇʀs: {total_users}  •  ᴛᴏᴛᴀʟ ᴅʟs: {total_downloads}")

    txt_content = "\n".join(lines)

    # write to temp file and send
    stats_path = os.path.join(tempfile.gettempdir(), f"stats_{int(time.time())}.txt")
    with open(stats_path, "w", encoding="utf-8") as f:
        f.write(txt_content)

    try:
        await message.reply_document(
            document=stats_path,
            file_name=f"stats_{now.replace(':', '-').replace(' ', '_')}.txt",
            caption=(
                "╭━━━━━ 📊 ━━━━━╮\n"
                "ᴜsᴇʀ sᴛᴀᴛs\n"
                "╰━━━━━━━━━━━━━╯\n\n"
                f"👥 {total_users} ᴜsᴇʀs\n"
                f"📥 {total_downloads} ᴅᴏᴡɴʟᴏᴀᴅs"
            ),
        )
    finally:
        try:
            os.unlink(stats_path)
        except Exception:
            pass


@app.on_message(filters.command("bcast") & owner_filter)
async def cmd_bcast(client, message: Message):
    """👑 Owner only — reply to any message with /bcast to broadcast."""

    if not message.reply_to_message:
        await message.reply_text(
            "╭━━━━━━━━━━━━━━━━━━╮\n"
            "  📢 ʙʀᴏᴀᴅᴄᴀsᴛ\n"
            "╰━━━━━━━━━━━━━━━━━━╯\n\n"
            "ʀᴇᴘʟʏ ᴛᴏ ᴀɴʏ ᴍᴇssᴀɢᴇ ᴡɪᴛʜ /bcast\n"
            "ᴛᴏ sᴇɴᴅ ɪᴛ ᴛᴏ ᴀʟʟ ᴜsᴇʀs."
        )
        return

    reply_msg = message.reply_to_message

    uids = [doc["_id"] async for doc in users_col.find({}, {"_id": 1})]

    if not uids:
        await message.reply_text("❌ ɴᴏ ᴜsᴇʀs ᴛᴏ ʙʀᴏᴀᴅᴄᴀsᴛ ᴛᴏ.")
        return

    total = len(uids)

    # status message — updated live
    status = await message.reply_text(
        "╭━━━━━━━━━━━━━━━━━━╮\n"
        "  📢 ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ\n"
        "╰━━━━━━━━━━━━━━━━━━╯\n\n"
        f"ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ ᴛᴏ {total} ᴜsᴇʀs…"
    )

    sent_ok = 0
    blocked = 0
    failed = 0

    for uid in uids:
        try:
            # copy_message preserves everything:
            # text formatting, photos, videos, documents,
            # inline buttons, forward tags, etc.
            await client.copy_message(
                chat_id=int(uid),
                from_chat_id=reply_msg.chat.id,
                message_id=reply_msg.id,
            )
            sent_ok += 1
        except Exception as e:
            err = str(e).lower()
            if "forbidden" in err or "blocked" in err or "deactivated" in err or "user is blocked" in err:
                blocked += 1
            else:
                failed += 1
                logger.warning("bcast fail %s — %s", uid, e)

        # update status every 20 users
        if (sent_ok + blocked + failed) % 20 == 0:
            try:
                await status.edit_text(
                    "╭━━━━━━━━━━━━━━━━━━╮\n"
                    "  📢 ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ\n"
                    "╰━━━━━━━━━━━━━━━━━━╯\n\n"
                    f"✅ sᴇɴᴛ: {sent_ok}\n"
                    f"🚫 ʙʟᴏᴄᴋᴇᴅ: {blocked}\n"
                    f"❌ ꜰᴀɪʟᴇᴅ: {failed}\n\n"
                    f"ʀᴇᴍᴀɪɴɪɴɢ: {total - sent_ok - blocked - failed}"
                )
            except Exception:
                pass

        # gentle pacing to avoid rate limits
        await asyncio.sleep(0.05)

    # final status
    await status.edit_text(
        "╭━━━━━━━━━━━━━━━━━━━━━━╮\n"
        "  📢 ʙʀᴏᴀᴅᴄᴀsᴛ ᴅᴏɴᴇ!\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
        f"👥 ᴛᴏᴛᴀʟ ᴜsᴇʀs: {total}\n"
        f"✅ ᴅᴇʟɪᴠᴇʀᴇᴅ: {sent_ok}\n"
        f"🚫 ʙʟᴏᴄᴋᴇᴅ: {blocked}\n"
        f"❌ ꜰᴀɪʟᴇᴅ: {failed}"
    )


# ═══════════════════════════════════════
# INLINE MODE  — @yoripinbot <link/text>
# ═══════════════════════════════════════


@app.on_inline_query()
async def on_inline(client, inline_query: InlineQuery):
    query = inline_query.query.strip()

    # ── empty query ─────────────────────
    if not query:
        results = [
            InlineQueryResultArticle(
                id="empty_help",
                title="📌 Pinterest DL",
                description=f"Type a Pinterest link or text after @{BOT_USERNAME} to download or search",
                input_message_content=InputTextMessageContent(
                    f"📌 ᴘɪɴᴛᴇʀsᴛ ᴅʟ\n\n"
                    f"ᴛʏᴘᴇ @{BOT_USERNAME} <link> ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ\n"
                    f"ᴛʏᴘᴇ @{BOT_USERNAME} <ᴛᴇxᴛ> ᴛᴏ sᴇᴀʀᴄʜ"
                ),
                thumb_url=START_IMAGE,
            )
        ]
        await inline_query.answer(results, cache_time=30)
        return

    url = extract_url(query)

    if url:
        # ── LINK FLOW ────────────────────
        try:
            expanded = url
            if "pin.it" in url:
                expanded = await expand_short(url)

            # check cache for direct URLs
            cached = await cache_col.find_one({"_id": expanded})

            if cached and cached.get("direct_urls"):
                results = _build_inline_results(cached["direct_urls"], cached["type"])
                await inline_query.answer(results, cache_time=300)
                return

            # fetch from Pinterest
            media_list = await get_media(expanded)

            if not media_list:
                results = [
                    InlineQueryResultArticle(
                        id="no_media",
                        title="❌ No Media Found",
                        description="Couldn't extract media from this link",
                        input_message_content=InputTextMessageContent(
                            "❌ ᴄᴏᴜʟᴅɴ'ᴛ ꜰɪɴᴅ ᴍᴇᴅɪᴀ"
                        ),
                    )
                ]
                await inline_query.answer(results, cache_time=0)
                return

            direct_urls = [m["url"] for m in media_list[:5]]
            mtype = media_list[0]["type"]

            # cache direct URLs for next time
            await cache_col.update_one(
                {"_id": expanded},
                {"$set": {"direct_urls": direct_urls, "type": mtype}},
                upsert=True,
            )

            results = _build_inline_results(direct_urls, mtype)
            await inline_query.answer(results, cache_time=300)

        except Exception as exc:
            logger.error("inline error — %s", exc)
            results = [
                InlineQueryResultArticle(
                    id="error",
                    title="❌ Error",
                    description="Failed to process this link",
                    input_message_content=InputTextMessageContent(
                        "❌ ᴇʀʀᴏʀ ᴘʀᴏᴄᴇssɪɴɢ ʟɪɴᴋ. ᴛʀʏ sᴇɴᴅɪɴɢ ɪᴛ ᴛᴏ ᴛʜᴇ ʙᴏᴛ ᴅɪʀᴇᴄᴛʟʏ."
                    ),
                )
            ]
            await inline_query.answer(results, cache_time=0)
        return

    # ── TEXT SEARCH FLOW ─────────────────
    try:
        results_urls = await search_pinterest(query, max_results=8)
        if not results_urls:
            results = [
                InlineQueryResultArticle(
                    id="no_search",
                    title="❌ No results",
                    description=f"No Pinterest images found for '{query}'",
                    input_message_content=InputTextMessageContent(
                        f"❌ ɴᴏ ɪᴍᴀɢᴇs ꜰᴏᴜɴᴅ ꜰᴏʀ {query}"
                    ),
                )
            ]
            await inline_query.answer(results, cache_time=0)
            return

        results = []
        ts = int(time.time())
        for i, murl in enumerate(results_urls):
            uid_str = f"s{i}_{ts}"
            results.append(
                InlineQueryResultPhoto(
                    id=uid_str,
                    photo_url=murl,
                    thumb_url=murl,
                    caption=MEDIA_CAPTION,
                )
            )
        await inline_query.answer(results, cache_time=300)

    except Exception as exc:
        logger.error("inline search error — %s", exc)
        results = [
            InlineQueryResultArticle(
                id="search_err",
                title="❌ Search Error",
                description="Failed to search Pinterest",
                input_message_content=InputTextMessageContent(
                    "❌ sᴇᴀʀᴄʜ ғᴀɪʟᴇᴅ. ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ."
                ),
            )
        ]
        await inline_query.answer(results, cache_time=0)


def _build_inline_results(urls: list[str], mtype: str) -> list:
    results = []
    ts = int(time.time())
    for i, murl in enumerate(urls):
        uid_str = f"{mtype[0]}{i}_{ts}"
        if mtype == "video":
            results.append(
                InlineQueryResultVideo(
                    id=uid_str,
                    video_url=murl,
                    thumb_url=START_IMAGE,
                    caption=MEDIA_CAPTION,
                    mime_type="video/mp4",
                )
            )
        else:
            results.append(
                InlineQueryResultPhoto(
                    id=uid_str,
                    photo_url=murl,
                    thumb_url=murl,
                    caption=MEDIA_CAPTION,
                )
            )
    return results


# ═══════════════════════════════════════
# RUN
# ═══════════════════════════════════════

print()
print("╭━━━━━━━━━━━━━━━━━━━━━━━━━╮")
print("  📌 ᴘɪɴᴛᴇʀsᴛ ᴅʟ ʙᴏᴛ")
print("  ✅ ʙᴏᴛ sᴛᴀʀᴛᴇᴅ!")
print("╰━━━━━━━━━━━━━━━━━━━━━━━━━╮")
print()

app.run()
