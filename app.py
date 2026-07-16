import asyncio
import os
import re
import time
import uuid
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any

import asyncpg
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    KeyboardButton,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
    MenuButtonDefault,
    FSInputFile,
)

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, SessionPasswordNeededError

from truck import register_truck_module, is_truck_offer_message, TRUCK_UI_SESSIONS


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("cargo-bot")

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("Asia/Tashkent")
except Exception:
    LOCAL_TZ = timezone(timedelta(hours=5))

load_dotenv()

BOT_TOKEN         = os.getenv("BOT_TOKEN")
DATABASE_URL      = os.getenv("DATABASE_URL")
TG_API_ID         = os.getenv("TG_API_ID")
TG_API_HASH       = os.getenv("TG_API_HASH")
TG_SESSION        = os.getenv("TG_SESSION")
BOT_NAME          = os.getenv("BOT_NAME", "Cargo Bot")
WELCOME_PHOTO     = os.getenv("WELCOME_PHOTO")
WELCOME_ANIMATION = os.getenv("WELCOME_ANIMATION")
ADMIN_IDS_RAW     = os.getenv("ADMIN_IDS", "")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN topilmadi")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL topilmadi")
if not TG_API_ID or not TG_API_HASH:
    raise RuntimeError("TG_API_ID / TG_API_HASH topilmadi")
if not TG_SESSION:
    raise RuntimeError("TG_SESSION topilmadi")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

TG_API_ID = int(TG_API_ID)

# Admin IDs
ADMIN_IDS = set()
for _aid in ADMIN_IDS_RAW.split(","):
    _aid = _aid.strip()
    if _aid.isdigit():
        ADMIN_IDS.add(int(_aid))

def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

PAGE_SIZE           = 5
AD_TTL_DAYS         = 2
DEDUP_TTL_HOURS     = 2
CACHE_TTL           = 300
GC_EVERY            = 30
EXPIRE_SWEEP_EVERY  = 600
MAX_PLACES_TO_INDEX = 6

INGEST_QUEUE: asyncio.Queue         = asyncio.Queue(maxsize=5000)
WORKER_BATCH_MAX                    = 120
WORKER_BATCH_WINDOW_SEC             = 0.2

RAM_DEDUP: Dict[str, float]         = {}
RAM_DEDUP_TTL                       = DEDUP_TTL_HOURS * 3600

CHAT_CACHE: Dict[int, Tuple[str, Optional[str], float]] = {}
CHAT_CACHE_TTL = 6 * 3600

SEARCH_SESSIONS: Dict[str, Dict[str, Any]] = {}
CACHE: Dict[
    Tuple[str, str, str, str, str],
    Tuple[float, List[dict], Optional[Tuple[datetime, str]], bool]
] = {}

DB_POOL: Optional[asyncpg.Pool] = None


# ═══════════════════════════════════════════════════════════════════════
#  FILTER
# ═══════════════════════════════════════════════════════════════════════

TRUCK_KEYWORDS = [
    # o'zbekcha (lotin)
    "yuk kerak",
    "yuk kk",
    "yuk kere",
    "mashina bor",
    "fura bor",
    "isuzu bor",
    "ref bor",
    "tent bor",
    "kamaz bor",
    "hovo bor",
    "howo bor",
    "gazel bor",
    "sprinter bor",
    "labo bor",
    "man bor",
    "yuk olamiz",
    "yuk bolsa olamiz",
    "yuk bo'lsa olamiz",
    "bosh mashina",
    "yuksiz",
    "jonab ketamiz",

    # o'zbekcha (kiril)
    "юк керак",
    "юк кк",
    "юк кере",
    "машина бор",
    "фура бор",
    "исузу бор",
    "реф бор",
    "тент бор",
    "камаз бор",
    "хово бор",
    "газель бор",
    "спринтер бор",
    "лабо бор",
    "юк оламиз",
    "юк булса оламиз",
    "юк бўлса оламиз",
    "буш машина",

    # ruscha
    "груз нужен",
    "груз нужна",
    "груз нужно",
    "нужен груз",
    "нужна загрузка",
    "ищу груз",
    "возьму груз",
    "есть машина",
    "есть фура",
    "есть реф",
    "есть тент",
    "есть камаз",
    "есть isuzu",
    "без груза",
    "пустая машин",
    "пустой машин",
]


def is_truck_ad(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in TRUCK_KEYWORDS)


CARGO_KEYWORDS = [
    "yuk bor", "юк бор", "юк есть",
    " tonna", " тонна",
    "kg yuk", "кг юк",
    "fura kerak", "фура керак", "фура нужн", "нужна фура",
    "tent kerak", "тент керак", "тент нужн", "нужен тент",
    "ref kerak", "реф керак", "реф нужн", "нужен реф",
    "mashina kerak", "машина керак", "машина нужн", "нужна машина",
    "isuzu kerak", "kamaz kerak", "камаз керак", "камаз нужн",
    "hovo kerak", "howo kerak", "хово керак",
    "gazel kerak", "газель керак",
    "sprinter kerak", "спринтер керак",
    "labo kerak", "лабо керак",
    "mandarin", "мандарин",
    "kartoshka", "картошка",
    "piyoz", "лук",
    "sabzi", "морковь",
    "karam", "капуста",
    "olma", "яблоко",
    "banan", "банан",
    "limon", "лимон",
    "apelsin", "апельсин",
    "anor", "гранат",
    "uzum", "виноград",
    "pomidor", "помидор",
    "bodring", "огурец",
    "qalampir", "перец",
    "mebel", "мебель",
    "sement", "цемент",
    "gips", "гипс",
    "armatura", "арматура",
    "taxta", "доска",
    "un ", "мука",
    "shakar", "сахар",
    "guruch", "рис",
    "texnika", "техника",
]


def is_cargo_ad(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in CARGO_KEYWORDS)


# ═══════════════════════════════════════════════════════════════════════
#  TRUCK PARSERS
# ═══════════════════════════════════════════════════════════════════════

TRUCK_VEHICLE_MAP = [
    ("Tent",     ["tent", "тент"]),
    ("Ref",      ["ref ", "реф ", "refka", "рефка", "refrijerator", "рефрижератор"]),
    ("Fura",     ["fura", "фура"]),
    ("Kamaz",    ["kamaz", "камаз"]),
    ("Hovo",     ["howo", "hovo", "хово"]),
    ("Isuzu",    ["isuzu"]),
    ("Gazel",    ["gazel", "газел", "газель"]),
    ("MAN",      [" man "]),
    ("Sprinter", ["sprinter", "спринтер"]),
    ("Labo",     ["labo", "лабо"]),
]

TRUCK_REGION_MAP = {
    "toshkent":    {"label": "Toshkent",         "aliases": ["toshkent", "ташкент"]},
    "samarqand":   {"label": "Samarqand",        "aliases": ["samarqand", "самарканд"]},
    "buxoro":      {"label": "Buxoro",           "aliases": ["buxoro", "бухоро"]},
    "navoiy":      {"label": "Navoiy",           "aliases": ["navoiy", "навои"]},
    "qashqadaryo": {"label": "Qashqadaryo",      "aliases": ["qashqadaryo", "qarshi", "карши"]},
    "surxondaryo": {"label": "Surxondaryo",      "aliases": ["surxondaryo", "termiz", "термиз"]},
    "andijon":     {"label": "Andijon",          "aliases": ["andijon", "андижан"]},
    "fargona":     {"label": "Farg'ona",         "aliases": ["fargona", "фаргона"]},
    "namangan":    {"label": "Namangan",         "aliases": ["namangan", "наманган"]},
    "xorazm":      {"label": "Xorazm",           "aliases": ["xorazm", "urgench", "ургенч"]},
    "jizzax":      {"label": "Jizzax",           "aliases": ["jizzax", "джизак"]},
    "sirdaryo":    {"label": "Sirdaryo",         "aliases": ["sirdaryo", "сирдарё"]},
    "qoraqalpoq":  {"label": "Qoraqalpog'iston", "aliases": ["nukus", "нукус", "qoraqalpoq"]},
}

TRUCK_COUNTRY_EXTRAS = {
    "russia":     {"label": "Rossiya",     "aliases": ["россия", "rossiya", "russia", "москва", "moskva", "питер", "piter"]},
    "kazakhstan": {"label": "Qozog'iston", "aliases": ["казахстан", "kazakhstan", "алматы", "almaty", "астана", "astana"]},
    "kyrgyzstan": {"label": "Qirg'iziston","aliases": ["кыргызстан", "kyrgyzstan", "бишкек", "bishkek"]},
    "tajikistan": {"label": "Tojikiston",  "aliases": ["таджикистан", "tajikistan", "душанбе", "dushanbe"]},
    "turkey":     {"label": "Turkiya",     "aliases": ["турция", "turkey", "стамбул", "istanbul"]},
    "china":      {"label": "Xitoy",       "aliases": ["китай", "china", "пекин", "beijing"]},
}


def parse_truck_vehicle(text: str) -> Optional[str]:
    t     = text.lower()
    found = []
    for name, keywords in TRUCK_VEHICLE_MAP:
        if any(kw in t for kw in keywords):
            found.append(name)
    return ", ".join(dict.fromkeys(found)) if found else None


def detect_truck_location(text: str):
    t = text.lower()
    for region_id, info in TRUCK_REGION_MAP.items():
        if any(a in t for a in info["aliases"]):
            return "uzbekistan", "O'zbekiston", region_id, info["label"]
    for country_id, info in TRUCK_COUNTRY_EXTRAS.items():
        if any(a in t for a in info["aliases"]):
            return country_id, info["label"], None, None
    return "uzbekistan", "O'zbekiston", None, None


def parse_truck_phone(text: str) -> Optional[str]:
    for p in [
        r"(\+998[\s\-]?\(?\d{2}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2})",
        r"(\b998[\s\-]?\(?\d{2}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}\b)",
        r"(\b\d{2}[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}\b)",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return re.sub(r"\s+", " ", re.sub(r"[^\d+]", " ", m.group(1))).strip()
    return None


def parse_truck_weight(text: str) -> Optional[str]:
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(tonna|t\b|kg|тонна|кг)", text.lower())
    if not m:
        return None
    val, unit = m.group(1).replace(",", "."), m.group(2).lower()
    if unit in ("t", "тонна"):
        unit = "tonna"
    elif unit == "кг":
        unit = "kg"
    return f"{val} {unit}"


# ═══════════════════════════════════════════════════════════════════════
#  CARGO PARSERS
# ═══════════════════════════════════════════════════════════════════════

VEHICLE_PATTERNS = [
    ("Tent",     [r"\btent\b",     r"\bтент\b"]),
    ("Ref",      [r"\bref\b",      r"\bреф\b",  r"\bрефрижератор\b", r"\brefrijerator\b"]),
    ("Fura",     [r"\bfura\b",     r"\bфура\b"]),
    ("Kamaz",    [r"\bkamaz\b",    r"\bкамаз\b"]),
    ("Hovo",     [r"\bhowo\b",     r"\bhovo\b",  r"\bхово\b"]),
    ("Isuzu",    [r"\bisuzu\b"]),
    ("Gazel",    [r"\bgazel\b",    r"\bгазель\b"]),
    ("MAN",      [r"\bman\b"]),
    ("Sprinter", [r"\bsprinter\b", r"\bспринтер\b"]),
    ("Labo",     [r"\blabo\b",     r"\bлабо\b"]),
]

CARGO_WORDS_LIST = [
    "mandarin", "мандарин", "piyoz", "лук", "kartoshka", "картошка",
    "sabzi", "морковь", "karam", "капуста", "olma", "яблоко",
    "banan", "банан", "limon", "лимон", "apelsin", "апельсин",
    "anor", "гранат", "uzum", "виноград", "pomidor", "помидор",
    "bodring", "огурец", "qalampir", "перец", "mebel", "мебель",
    "sement", "цемент", "gips", "гипс", "armatura", "арматура",
    "taxta", "доска", "un", "мука", "shakar", "сахар",
    "guruch", "рис", "texnika", "техника",
]

PHONE_PATTERNS_CARGO = [
    r"(\+998[\s\-]?\(?\d{2}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2})",
    r"(\b998[\s\-]?\(?\d{2}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}\b)",
    r"(\b\d{2}[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}\b)",
]


# ═══════════════════════════════════════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════════════════════════════════════

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def escape_html(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def source_key(chat_id: int, message_id: int) -> str:
    return f"{chat_id}_{message_id}"

def make_session_id() -> str:
    return uuid.uuid4().hex[:10]

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def shorten_text(text: str, max_len: int = 170) -> str:
    t = normalize_text(text)
    if len(t) <= max_len:
        return t
    cut = t[:max_len].rsplit(" ", 1)[0].strip()
    return (cut or t[:max_len]).strip() + "..."

def text_hash_norm(text: str) -> str:
    t = normalize_text(text).lower()
    t = re.sub(r"[^\w\s]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return hashlib.sha1(t.encode("utf-8")).hexdigest()

def normalize_place_token(s: str) -> str:
    s = normalize_text(s).lower().replace("ё", "е")
    s = re.sub(r"[^\w\s\-]", " ", s)
    s = re.sub(r"\b(sh|shahar|tumani|tuman|viloyati|viloyat|rayon|oblast|город|район|область)\b\.?", "", s).strip()
    return re.sub(r"\s+", " ", s)

# Lotin → Kiril transliteratsiya
_LAT_TO_CYR = {
    "yo": "ё", "yu": "ю", "ya": "я", "ye": "е",
    "sh": "ш", "ch": "ч", "ng": "нг",
    "a": "а", "b": "б", "v": "в", "g": "г", "d": "д",
    "e": "е", "z": "з", "i": "и", "y": "й",
    "k": "к", "l": "л", "m": "м", "n": "н", "o": "о",
    "p": "п", "r": "р", "s": "с", "t": "т", "u": "у",
    "f": "ф", "x": "х", "j": "ж", "h": "ҳ",
    "q": "қ", "w": "в",
}

_CYR_TO_LAT = {
    "ш": "sh", "ч": "ch", "ё": "yo", "ю": "yu", "я": "ya",
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
    "е": "e", "ж": "j", "з": "z", "и": "i", "й": "y",
    "к": "k", "л": "l", "м": "m", "н": "n", "о": "o",
    "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "x", "ҳ": "h", "қ": "q", "ғ": "g",
    "ў": "o", "э": "e", "ы": "i", "ц": "s",
}

def lat_to_cyr(text: str) -> str:
    result = []
    i = 0
    t = text.lower()
    while i < len(t):
        if i + 1 < len(t) and t[i:i+2] in _LAT_TO_CYR:
            result.append(_LAT_TO_CYR[t[i:i+2]])
            i += 2
        elif t[i] in _LAT_TO_CYR:
            result.append(_LAT_TO_CYR[t[i]])
            i += 1
        else:
            result.append(t[i])
            i += 1
    return "".join(result)

def cyr_to_lat(text: str) -> str:
    result = []
    for ch in text.lower():
        if ch in _CYR_TO_LAT:
            result.append(_CYR_TO_LAT[ch])
        else:
            result.append(ch)
    return "".join(result)

def transliterate_place(place: str) -> List[str]:
    """Har bir joy nomi uchun lotin va kiril variantlarini qaytaradi"""
    place = place.lower().strip()
    variants = {place}
    # Lotin harflar bormi?
    if re.search(r"[a-z]", place):
        variants.add(lat_to_cyr(place))
    # Kiril harflar bormi?
    if re.search(r"[а-яқғҳўё]", place):
        variants.add(cyr_to_lat(place))
    return list(variants)

def make_message_link(chat_id: int, message_id: int, chat_username: Optional[str]) -> Optional[str]:
    if chat_username:
        return f"https://t.me/{chat_username}/{message_id}"
    s = str(chat_id)
    if s.startswith("-100"):
        return f"https://t.me/c/{s[4:]}/{message_id}"
    # Oddiy guruhlar (supergroup emas) — manfiy id
    if chat_id < 0:
        return f"https://t.me/c/{abs(chat_id)}/{message_id}"
    return None

def is_valid_message_link(link: Optional[str]) -> bool:
    """Public (t.me/username/id) va private (t.me/c/id/id) linklarni qabul qiladi."""
    if not link:
        return False
    return bool(re.match(
        r"^https://t\.me/(?:c/\d+|[A-Za-z0-9_]{4,})/\d+$",
        link,
    ))

def cache_get(key):
    v = CACHE.get(key)
    if not v:
        return None
    ts, items, next_cursor, has_next = v
    if time.time() - ts > CACHE_TTL:
        CACHE.pop(key, None)
        return None
    return items, next_cursor, has_next

def cache_set(key, items, next_cursor, has_next):
    CACHE[key] = (time.time(), items, next_cursor, has_next)

def parse_phone(text: str) -> Optional[str]:
    for p in PHONE_PATTERNS_CARGO:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return re.sub(r"\s+", " ", re.sub(r"[^\d+]", " ", m.group(1))).strip()
    return None

def parse_weight(text: str) -> Optional[str]:
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(tonna|t\b|kg|тонна|кг)", text.lower())
    if not m:
        return None
    val, unit = m.group(1).replace(",", "."), m.group(2).lower()
    if unit in ("t", "тонна"):
        unit = "tonna"
    elif unit == "кг":
        unit = "kg"
    return f"{val} {unit}"

def parse_vehicle_need(text: str) -> Optional[str]:
    t     = " " + text.lower() + " "
    found = []
    for name, patterns in VEHICLE_PATTERNS:
        if any(re.search(p, t, re.IGNORECASE) for p in patterns):
            found.append(name)
    return ", ".join(dict.fromkeys(found)) if found else None

def parse_route_from_to(text: str) -> Tuple[Optional[str], Optional[str]]:
    t = normalize_place_token(text)
    for p in [
        r"([a-zа-яқғҳўё\- ]+?)dan\s*[-–—]?\s*([a-zа-яқғҳўё\- ]+?)ga\b",
        r"([a-zа-яқғҳўё\- ]+?)дан\s*[-–—]?\s*([a-zа-яқғҳўё\- ]+?)га\b",
        r"\b([a-zа-яқғҳўё]{3,})\s*[-–—]\s*([a-zа-яқғҳўё]{3,})\b",
    ]:
        m = re.search(p, t, re.IGNORECASE)
        if m:
            a, b = normalize_place_token(m.group(1)), normalize_place_token(m.group(2))
            return (a or None), (b or None)
    return None, None

def extract_place_candidates(text: str) -> List[str]:
    places: List[str] = []
    a, b = parse_route_from_to(text)
    if a: places.append(a)
    if b and b != a: places.append(b)
    stop = {"yuk","юк","bor","есть","kerak","нужен","машина","mashina","tonna","тонна","kg","кг","dan","ga","дан","га","telefon","tel","тел","fura","фура","tent","тент","ref","реф","kamaz","камаз","isuzu","gazel","газель","sprinter","спринтер","labo","лабо"}
    seen = set(places)
    for w in re.findall(r"[a-zа-яқғҳўё\-]{3,}", normalize_place_token(text), re.IGNORECASE):
        w = normalize_place_token(w)
        if not w or w in stop or w in seen:
            continue
        places.append(w)
        seen.add(w)
        if len(places) >= MAX_PLACES_TO_INDEX:
            break
    return places

def parse_cargo_name(text: str) -> Optional[str]:
    t = normalize_text(text.lower())
    for p in [r"(?:yuk|юк|груз)\s*[:\-]\s*([^\n,|]{3,40})", r"(?:yuk|юк|груз)\s+([^\n,|]{3,40})"]:
        m = re.search(p, t, re.IGNORECASE)
        if m:
            val = re.sub(r"\b(kerak|bor|есть|нужен|mashina|машина|tonna|тонна|kg|кг)\b", "", normalize_text(m.group(1)), flags=re.IGNORECASE).strip(" -.,")
            if val and len(val) >= 3:
                return val[:40]
    found = [w for w in CARGO_WORDS_LIST if re.search(rf"\b{re.escape(w)}\b", t, re.IGNORECASE)]
    return ", ".join(list(dict.fromkeys(found))[:3]) if found else None

def _media_input(value: Optional[str]):
    if not value:
        return None
    if os.path.isfile(value):
        return FSInputFile(value)
    return value

def welcome_caption() -> str:
    return (
        f"🚛 <b>{escape_html(BOT_NAME)}</b>\n\n"
        "Assalomu alaykum! Xush kelibsiz!\n\n"
        "📦 <b>Yuk e'lonlari</b> — yuk bor, mashina kerak bo'lgan e'lonlarni qidiring\n"
        "🚚 <b>Yuk mashinalar</b> — bo'sh mashina bor, yuk qidirayotgan haydovchilar e'lonlari\n\n"
        "🔍 Shahar yoki viloyat nomi bo'yicha qulay qidiruv\n"
        "📋 Har bir e'lonning to'liq ma'lumotini ko'rish\n"
        "🔄 Yangi e'lonlar real vaqtda yangilanadi\n\n"
        "👇 Davom etish uchun pastdagi tugmani bosing."
    )

async def setup_bot_ui():
    try:
        await bot.delete_my_commands()
    except Exception:
        try:
            await bot.set_my_commands([])
        except Exception:
            log.exception("delete/set_my_commands error")

    try:
        await bot.set_chat_menu_button(menu_button=MenuButtonDefault())
    except Exception:
        log.exception("set_chat_menu_button error")

def kb_continue():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Davom etish")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

async def send_welcome_media(message: types.Message):
    caption = welcome_caption()

    photo = _media_input(WELCOME_PHOTO)
    if photo:
        try:
            await message.answer_photo(
                photo=photo,
                caption=caption,
                parse_mode="HTML",
                reply_markup=kb_continue(),
            )
            return
        except Exception:
            log.exception("welcome photo send error")

    await message.answer(
        caption,
        parse_mode="HTML",
        reply_markup=kb_continue(),
    )


# ═══════════════════════════════════════════════════════════════════════
#  DB
# ═══════════════════════════════════════════════════════════════════════

def get_pool() -> asyncpg.Pool:
    if DB_POOL is None:
        raise RuntimeError("DB pool initialized emas")
    return DB_POOL

async def init_db():
    global DB_POOL
    DB_POOL = await asyncpg.create_pool(
        dsn=DATABASE_URL, min_size=1, max_size=10,
        command_timeout=60, statement_cache_size=0,
    )
    async with DB_POOL.acquire() as conn:
        await conn.execute("select 1")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                uid BIGINT PRIMARY KEY,
                phone TEXT,
                tg_username TEXT,
                fullname TEXT,
                registered BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS cargo_ads (
                ad_id TEXT PRIMARY KEY,
                source_chat_id BIGINT NOT NULL,
                source_message_id BIGINT NOT NULL,
                source_key TEXT,
                source_title TEXT,
                source_username TEXT,
                text_full TEXT NOT NULL,
                text_short TEXT,
                text_norm TEXT,
                from_place TEXT,
                to_place TEXT,
                places TEXT[],
                cargo_name TEXT,
                vehicle_need TEXT,
                phone TEXT,
                weight TEXT,
                link TEXT,
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMPTZ NOT NULL
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS truck_ads (
                ad_id TEXT PRIMARY KEY,
                source_chat_id BIGINT NOT NULL,
                source_message_id BIGINT NOT NULL,
                source_key TEXT,
                source_title TEXT,
                source_username TEXT,
                text_full TEXT NOT NULL,
                text_short TEXT,
                text_norm TEXT,
                vehicle TEXT,
                phone TEXT,
                weight TEXT,
                country TEXT,
                country_label TEXT,
                region TEXT,
                region_label TEXT,
                link TEXT,
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMPTZ NOT NULL
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dedup_hashes (
                hash TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS visits (
                id SERIAL PRIMARY KEY,
                uid BIGINT NOT NULL,
                visited_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS manual_ads (
                id SERIAL PRIMARY KEY,
                ad_type VARCHAR(10) NOT NULL DEFAULT 'cargo',
                text_full TEXT NOT NULL,
                posted_by BIGINT NOT NULL,
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                expires_at TIMESTAMPTZ
            )
        """)

        # Eski jadvalga yangi ustun qo'shish (agar yo'q bo'lsa)
        await conn.execute("""
            ALTER TABLE manual_ads
            ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cargo_ads_active_created
            ON cargo_ads (active, created_at DESC, ad_id DESC)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cargo_ads_expires
            ON cargo_ads (expires_at)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cargo_ads_from_place
            ON cargo_ads (from_place)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_cargo_ads_to_place
            ON cargo_ads (to_place)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_truck_ads_active_country
            ON truck_ads (active, country, created_at DESC, ad_id DESC)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_truck_ads_active_region
            ON truck_ads (active, country, region, created_at DESC, ad_id DESC)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_truck_ads_expires
            ON truck_ads (expires_at)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_dedup_hashes_expires
            ON dedup_hashes (expires_at)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_visits_uid_at
            ON visits (uid, visited_at DESC)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_manual_ads_expires
            ON manual_ads (expires_at)
        """)

    log.info("Postgres ulandi, jadvallar tayyor.")

async def get_user(uid: int) -> Optional[dict]:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "select uid,phone,tg_username,fullname,registered,created_at,updated_at from users where uid=$1",
            uid
        )
    return dict(row) if row else None

async def save_user(uid: int, data: dict):
    async with get_pool().acquire() as conn:
        await conn.execute(
            "insert into users(uid,phone,tg_username,fullname,registered,created_at,updated_at) "
            "values($1,$2,$3,$4,$5,coalesce($6,now()),$7) "
            "on conflict(uid) do update set "
            "phone=coalesce(excluded.phone,users.phone),"
            "tg_username=coalesce(excluded.tg_username,users.tg_username),"
            "fullname=coalesce(excluded.fullname,users.fullname),"
            "registered=coalesce(excluded.registered,users.registered),"
            "updated_at=excluded.updated_at",
            uid, data.get("phone"), data.get("tgUsername"), data.get("fullname"),
            data.get("registered"), data.get("createdAt"), data.get("updatedAt", now_utc()),
        )

async def is_registered(uid: int) -> bool:
    u = await get_user(uid)
    return bool(u and u.get("registered") is True)

async def log_visit(uid: int):
    """Har bir foydalanuvchi tashrifi yoziladi"""
    try:
        async with get_pool().acquire() as conn:
            await conn.execute("INSERT INTO visits(uid, visited_at) VALUES($1, NOW())", uid)
    except Exception:
        log.exception("log_visit error")

async def try_dedup_lock(kind: str, text: str) -> Tuple[bool, str]:
    base   = text_hash_norm(text)
    th     = f"{kind}_{base}" if kind != "cargo" else base
    now_ts = time.time()
    if (exp := RAM_DEDUP.get(th)) and exp > now_ts:
        return False, th
    RAM_DEDUP[th] = now_ts + RAM_DEDUP_TTL
    async with get_pool().acquire() as conn:
        inserted = await conn.fetchval(
            "insert into dedup_hashes(hash,kind,expires_at) values($1,$2,$3) "
            "on conflict(hash) do nothing returning hash",
            th, kind, now_utc() + timedelta(hours=DEDUP_TTL_HOURS),
        )
    return bool(inserted), th


# ═══════════════════════════════════════════════════════════════════════
#  SAQLASH
# ═══════════════════════════════════════════════════════════════════════

async def save_cargo_ad(chat_id, message_id, chat_title, chat_username, text, msg_date_utc):
    if not is_cargo_ad(text):
        return False, "filtered"

    created_at = msg_date_utc if msg_date_utc.tzinfo else msg_date_utc.replace(tzinfo=timezone.utc)
    is_new, ad_id = await try_dedup_lock("cargo", text)
    if not is_new:
        return False, "dup"

    from_place, to_place = parse_route_from_to(text)
    places = extract_place_candidates(text)
    link   = make_message_link(chat_id, message_id, chat_username)
    saved_at   = now_utc()
    expires_at = saved_at + timedelta(days=AD_TTL_DAYS)
    updated_at = saved_at

    async with get_pool().acquire() as conn:
        await conn.execute(
            """insert into cargo_ads(
                   ad_id,source_chat_id,source_message_id,source_key,source_title,source_username,
                   text_full,text_short,text_norm,from_place,to_place,places,cargo_name,
                   vehicle_need,phone,weight,link,active,created_at,updated_at,expires_at
               )
               values(
                   $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
                   $14,$15,$16,$17,true,$18,$19,$20
               )
               on conflict(ad_id) do update set
                   source_chat_id=excluded.source_chat_id,
                   source_message_id=excluded.source_message_id,
                   source_key=excluded.source_key,
                   source_title=excluded.source_title,
                   source_username=excluded.source_username,
                   text_full=excluded.text_full,
                   text_short=excluded.text_short,
                   text_norm=excluded.text_norm,
                   from_place=excluded.from_place,
                   to_place=excluded.to_place,
                   places=excluded.places,
                   cargo_name=excluded.cargo_name,
                   vehicle_need=excluded.vehicle_need,
                   phone=excluded.phone,
                   weight=excluded.weight,
                   link=excluded.link,
                   active=true,
                   updated_at=excluded.updated_at,
                   expires_at=excluded.expires_at""",
            ad_id, chat_id, message_id, source_key(chat_id, message_id),
            chat_title, chat_username,
            text, shorten_text(text, 170), normalize_text(text).lower(),
            from_place, to_place, places[:MAX_PLACES_TO_INDEX],
            parse_cargo_name(text), parse_vehicle_need(text), parse_phone(text), parse_weight(text),
            link, created_at, updated_at, expires_at,
        )
    log.info("cargo SAVED chat=%s msg=%s", chat_id, message_id)
    return True, "saved"

async def save_truck_ad(chat_id, message_id, chat_title, chat_username, text, msg_date_utc):
    if not is_truck_offer_message(text):
        return False, "filtered"

    link = make_message_link(chat_id, message_id, chat_username)
    # Guruh/kanal e'lonlari uchun link bo'lishi kerak (public yoki private /c/).
    # Qo'lda (admin/user) joylashtirilganda chat_id > 0 bo'ladi — link shart emas.
    is_manual = isinstance(chat_id, int) and chat_id > 0
    if not is_manual and not is_valid_message_link(link):
        return False, "no_link"

    created_at = msg_date_utc if msg_date_utc.tzinfo else msg_date_utc.replace(tzinfo=timezone.utc)

    is_new, ad_id = await try_dedup_lock("truck", text)
    if not is_new:
        return False, "dup"

    vehicle                                             = parse_truck_vehicle(text)
    phone                                               = parse_truck_phone(text)
    weight                                              = parse_truck_weight(text)
    country_id, country_label, region_id, region_label = detect_truck_location(text)

    text_norm  = normalize_text(text).lower()
    text_short = shorten_text(text, 160)
    saved_at   = now_utc()
    expires_at = saved_at + timedelta(days=AD_TTL_DAYS)
    updated_at = saved_at

    async with get_pool().acquire() as conn:
        exists = await conn.fetchval(
            """
            select 1
            from truck_ads
            where active = true
              and source_chat_id = $1
              and text_norm = $2
            limit 1
            """,
            chat_id, text_norm
        )
        if exists:
            return False, "dup_db"

        await conn.execute(
            """insert into truck_ads(
                   ad_id,source_chat_id,source_message_id,source_key,
                   source_title,source_username,text_full,text_short,text_norm,
                   vehicle,phone,weight,country,country_label,region,region_label,
                   link,active,created_at,updated_at,expires_at
               )
               values(
                   $1,$2,$3,$4,$5,$6,$7,$8,$9,
                   $10,$11,$12,$13,$14,$15,$16,
                   $17,true,$18,$19,$20
               )
               on conflict(ad_id) do update set
                   source_chat_id=excluded.source_chat_id,
                   source_message_id=excluded.source_message_id,
                   source_key=excluded.source_key,
                   source_title=excluded.source_title,
                   source_username=excluded.source_username,
                   text_full=excluded.text_full,
                   text_short=excluded.text_short,
                   text_norm=excluded.text_norm,
                   vehicle=excluded.vehicle,
                   phone=excluded.phone,
                   weight=excluded.weight,
                   country=excluded.country,
                   country_label=excluded.country_label,
                   region=excluded.region,
                   region_label=excluded.region_label,
                   link=excluded.link,
                   active=true,
                   updated_at=excluded.updated_at,
                   expires_at=excluded.expires_at""",
            ad_id, chat_id, message_id, source_key(chat_id, message_id),
            chat_title, chat_username,
            text, text_short, text_norm,
            vehicle, phone, weight,
            country_id, country_label, region_id, region_label,
            link, created_at, updated_at, expires_at,
        )

    log.info(
        "truck SAVED chat=%s msg=%s vehicle=%s country=%s region=%s link=%s",
        chat_id, message_id, vehicle, country_id, region_id, bool(link)
    )
    return True, "saved"

async def deactivate_by_source(chat_id: int, message_id: int) -> int:
    async with get_pool().acquire() as conn:
        r1 = await conn.execute(
            "update cargo_ads set active=false,updated_at=$3 "
            "where source_chat_id=$1 and source_message_id=$2 and active=true",
            chat_id, message_id, now_utc()
        )
        r2 = await conn.execute(
            "update truck_ads set active=false,updated_at=$3 "
            "where source_chat_id=$1 and source_message_id=$2 and active=true",
            chat_id, message_id, now_utc()
        )
    return int(r1.split()[-1]) + int(r2.split()[-1])


# ═══════════════════════════════════════════════════════════════════════
#  CARGO QUERY
# ═══════════════════════════════════════════════════════════════════════

async def query_cargo_page(place1: str, place2: Optional[str], cursor: Optional[Tuple[datetime, str]]):
    # Transliteratsiya — lotin va kiril variantlarini qidirish
    place1_variants = transliterate_place(place1)
    place2_variants = transliterate_place(place2) if place2 else []

    # place1 uchun OR condition
    place1_conds = []
    params: List[Any] = []
    idx = 1
    for v in place1_variants:
        place1_conds.append(f"(from_place = ${idx} or to_place = ${idx} or ${idx} = any(places))")
        params.append(v)
        idx += 1
    conditions = ["active = true", f"({' or '.join(place1_conds)})"]

    # place2 uchun OR condition
    if place2_variants:
        place2_conds = []
        for v in place2_variants:
            place2_conds.append(f"(from_place = ${idx} or to_place = ${idx} or ${idx} = any(places))")
            params.append(v)
            idx += 1
        conditions.append(f"({' or '.join(place2_conds)})")

    if cursor:
        conditions.append(f"(created_at, ad_id) < (${idx}, ${idx+1})")
        params.extend([cursor[0], cursor[1]])

    sql = f"""select ad_id,text_short as text,text_norm,from_place,to_place,places,cargo_name,
                     vehicle_need,phone,weight,link,source_title,source_username,source_chat_id,
                     source_message_id,active,created_at,updated_at,expires_at
              from cargo_ads
              where {" and ".join(conditions)}
              order by created_at desc, ad_id desc
              limit {PAGE_SIZE+1}"""

    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, *params)

    has_next = len(rows) > PAGE_SIZE
    rows = rows[:PAGE_SIZE]
    return [dict(r) for r in rows], (rows[-1]["created_at"], rows[-1]["ad_id"]) if rows else None, has_next

async def fallback_scan_ads(place1: str, place2: Optional[str], offset: int = 0):
    p1_variants = transliterate_place(normalize_place_token(place1))
    p2_variants = transliterate_place(normalize_place_token(place2)) if place2 else []

    # Har bir variant uchun LIKE condition
    p1_conds = []
    params: List[Any] = []
    idx = 1
    for v in p1_variants:
        p1_conds.append(f"text_norm like ${idx}")
        params.append(f"%{v}%")
        idx += 1
    conditions = ["active = true", f"({' or '.join(p1_conds)})"]

    if p2_variants:
        p2_conds = []
        for v in p2_variants:
            p2_conds.append(f"text_norm like ${idx}")
            params.append(f"%{v}%")
            idx += 1
        conditions.append(f"({' or '.join(p2_conds)})")

    sql = f"""select ad_id,text_short as text,text_norm,from_place,to_place,places,cargo_name,
                     vehicle_need,phone,weight,link,source_title,source_username,source_chat_id,
                     source_message_id,active,created_at,updated_at,expires_at
              from cargo_ads
              where {" and ".join(conditions)}
              order by created_at desc, ad_id desc
              offset {offset}
              limit {PAGE_SIZE+1}"""

    async with get_pool().acquire() as conn:
        rows = await conn.fetch(sql, *params)

    has_next = len(rows) > PAGE_SIZE
    rows = rows[:PAGE_SIZE]
    return [dict(r) for r in rows], offset + PAGE_SIZE, has_next


# ═══════════════════════════════════════════════════════════════════════
#  FSM / KEYBOARDS
# ═══════════════════════════════════════════════════════════════════════

class Register(StatesGroup):
    waiting_fullname = State()
    waiting_contact  = State()

class CargoSearch(StatesGroup):
    waiting_place = State()

class AdminPostAd(StatesGroup):
    choosing_type    = State()
    waiting_text     = State()
    confirm          = State()

class UserPostAd(StatesGroup):
    choosing_type    = State()
    waiting_text     = State()
    confirm          = State()

def kb_request_contact():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Kontakt ulashish", request_contact=True)],
            [KeyboardButton(text="❌ Bekor qilish")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def kb_main_menu(uid: int = 0):
    rows = [
        [KeyboardButton(text="📦 Yuk e'lonlari")],
        [KeyboardButton(text="🚚 Yuk mashinalar e'lonlari")],
        [KeyboardButton(text="📝 E'lon joylash")],
    ]
    if is_admin(uid):
        rows.append([KeyboardButton(text="🛠 Admin Panel")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def kb_cancel():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Bekor qilish")]],
        resize_keyboard=True
    )

def build_page_keyboard(session_id, page, has_prev, has_next, items=None):
    rows = []
    if items:
        detail_row = []
        for i, ad in enumerate(items):
            ad_id = ad.get("ad_id") or ""
            detail_row.append(InlineKeyboardButton(
                text=f"📋 {i+1}",
                callback_data=f"cargo_detail:{ad_id}"
            ))
            if len(detail_row) == 5:
                rows.append(detail_row)
                detail_row = []
        if detail_row:
            rows.append(detail_row)
    nav_row = []
    if has_prev:
        nav_row.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=f"cargo_nav:{session_id}:{page-1}"))
    if has_next:
        nav_row.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=f"cargo_nav:{session_id}:{page+1}"))
    if nav_row:
        rows.append(nav_row)
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


# ═══════════════════════════════════════════════════════════════════════
#  CARGO UI
# ═══════════════════════════════════════════════════════════════════════

def short_route(ad):
    if ad.get("from_place") and ad.get("to_place"):
        return f"{ad['from_place']} → {ad['to_place']}"
    places = ad.get("places") or []
    if len(places) >= 2:
        return f"{places[0]} → {places[1]}"
    return places[0] if places else "—"

def format_ad_item(ad, idx):
    ad_id         = ad.get("ad_id") or ""
    cargo_name    = ad.get("cargo_name") or "Aniqlanmadi"
    vehicle_need  = ad.get("vehicle_need") or "Ko'rsatilmagan"
    weight        = ad.get("weight") or "—"
    phone         = ad.get("phone") or "Ko'rsatilmagan"
    text          = ad.get("text") or ""
    link          = ad.get("link")
    link_part     = f'\n🔗 <a href="{link}">Manba</a>' if link else ""
    return (
        f"<b>{idx}.</b> 📍 <b>Manzil:</b> {escape_html(short_route(ad))}\n"
        f"📦 <b>Yuk:</b> {escape_html(cargo_name)}\n"
        f"🚚 <b>Mashina:</b> {escape_html(vehicle_need)}\n"
        f"⚖️ <b>Vazn:</b> {escape_html(weight)}\n"
        f"📞 <b>Telefon:</b> {escape_html(phone)}\n"
        f"📝 <b>Qisqa:</b> {escape_html(text)}"
        f"{link_part}"
    )

def build_page_text(items, place1, place2, page):
    q = f"{place1} + {place2}" if place2 else place1
    lines = [
        "📦 <b>Topilgan e'lonlar</b>",
        f"🔎 <b>So'rov:</b> {escape_html(q)}",
        f"📄 <b>Sahifa:</b> {page+1}",
        ""
    ]
    for i, ad in enumerate(items, 1):
        lines.append(format_ad_item(ad, i))
        if i != len(items):
            lines.append("────────────")
    text = "\n".join(lines)
    if len(text) <= 3900:
        return text

    short = [
        "📦 <b>Topilgan e'lonlar</b>",
        f"🔎 <b>So'rov:</b> {escape_html(q)}",
        f"📄 <b>Sahifa:</b> {page+1}",
        ""
    ]
    for i, ad in enumerate(items, 1):
        link    = ad.get("link")
        details = f'<a href="{link}">Batafsil</a>' if link else "Batafsil yo'q"
        short.append(
            f"<b>{i}.</b> {escape_html(short_route(ad))} | "
            f"{escape_html(ad.get('cargo_name') or '—')} | "
            f"{escape_html(ad.get('phone') or '—')} | {details}"
        )
    return "\n".join(short)

async def delete_session_messages(bot, chat_id, session_id):
    sess = SEARCH_SESSIONS.get(session_id)
    if not sess:
        return
    mids = sess.get("sent_msg_ids") or []
    if mids:
        await asyncio.gather(
            *[bot.delete_message(chat_id, mid) for mid in mids],
            return_exceptions=True
        )
    sess["sent_msg_ids"] = []
    SEARCH_SESSIONS[session_id] = sess

async def send_page(bot, chat_id, session_id, page):
    sess = SEARCH_SESSIONS.get(session_id)
    if not sess:
        await bot.send_message(chat_id, "Sessiya tugagan. Qayta qidiring.", reply_markup=kb_main_menu())
        return

    await delete_session_messages(bot, chat_id, session_id)
    place1, place2 = sess["place1"], sess.get("place2")
    mode = sess.get("mode", "index")

    if mode == "index":
        cursors = sess.setdefault("cursors", [None])
        while len(cursors) <= page:
            cursors.append(None)
        cursor = cursors[page]
        key = ("idx", place1, place2 or "-", cursor[0].isoformat() if cursor else "-", cursor[1] if cursor else "-")
        cached = cache_get(key)
        if cached:
            items, next_cursor, has_next = cached
        else:
            items, next_cursor, has_next = await query_cargo_page(place1, place2, cursor)
            cache_set(key, items, next_cursor, has_next)

        if not items:
            sess["mode"] = "fallback"
            sess["fb_offsets"] = {0: 0}
            SEARCH_SESSIONS[session_id] = sess
            await send_page(bot, chat_id, session_id, 0)
            return

        if next_cursor:
            if len(cursors) <= page + 1:
                cursors.append(next_cursor)
            else:
                cursors[page + 1] = next_cursor
        SEARCH_SESSIONS[session_id] = sess
    else:
        offsets = sess.setdefault("fb_offsets", {0: 0})
        offset = offsets.get(page, 0)
        items, next_offset, has_next = await fallback_scan_ads(place1, place2, offset)
        offsets[page + 1] = next_offset
        SEARCH_SESSIONS[session_id] = sess
        if not items:
            q = f"{place1} + {place2}" if place2 else place1
            await bot.send_message(chat_id, f"❗️<b>{escape_html(q)}</b> bo'yicha e'lon topilmadi.", parse_mode="HTML")
            return

    msg = await bot.send_message(
        chat_id,
        build_page_text(items, place1, place2, page),
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=build_page_keyboard(session_id, page, page > 0, has_next, items),
    )
    sess["sent_msg_ids"] = [msg.message_id]
    SEARCH_SESSIONS[session_id] = sess


# ═══════════════════════════════════════════════════════════════════════
#  BOT INIT
# ═══════════════════════════════════════════════════════════════════════

bot             = Bot(token=BOT_TOKEN)
dp              = Dispatcher()
telethon_client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)

register_truck_module(dp, bot, {
    "get_db_pool": get_pool,
    "is_registered": is_registered,
    "kb_main_menu": kb_main_menu,
    "escape_html": escape_html,
    "make_session_id": make_session_id,
    "telethon_client": telethon_client,
})


# ═══════════════════════════════════════════════════════════════════════
#  AIOGRAM HANDLERS
# ═══════════════════════════════════════════════════════════════════════

@dp.error()
async def on_error(event: types.ErrorEvent):
    log.exception("BOT ERROR: %s", event.exception)
    try:
        msg = getattr(getattr(event, "update", None), "message", None)
        if msg and msg.chat.type == "private":
            await msg.answer(f"❌ Xatolik: {escape_html(str(event.exception))}", parse_mode="HTML")
    except Exception:
        pass
    return True

@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    if message.chat.type != "private":
        await message.answer("Botdan foydalanish uchun menga private yozing: /start")
        return

    uid = message.from_user.id
    await log_visit(uid)

    if await is_registered(uid):
        await state.clear()
        await message.answer(
            f"✅ <b>{escape_html(BOT_NAME)}</b>\n\nMenyu:",
            parse_mode="HTML",
            reply_markup=kb_main_menu(uid),
        )
        return

    await state.set_state(Register.waiting_fullname)
    await send_welcome_media(message)

@dp.message(F.text == "🛠 Admin Panel")
async def admin_panel_button(message: types.Message, state: FSMContext):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        await message.answer("⛔️ Sizda admin huquqi yo'q.")
        return
    await state.clear()
    await message.answer(
        "🛠 <b>Admin Panel</b>\n\nQuyidagi bo'limlardan birini tanlang:",
        parse_mode="HTML",
        reply_markup=kb_admin_menu()
    )

@dp.message(F.text == "❌ Bekor qilish")
async def cancel(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Bekor qilindi. /start ni bosing.", reply_markup=types.ReplyKeyboardRemove())

@dp.message(Register.waiting_fullname, F.text == "✅ Davom etish")
async def continue_register(message: types.Message, state: FSMContext):
    await message.answer(
        "👤 <b>Ism familiyangizni kiriting</b>\n\nMasalan: <b>Ali Valiyev</b>",
        parse_mode="HTML",
        reply_markup=types.ReplyKeyboardRemove()
    )

@dp.message(Register.waiting_fullname, F.text)
async def got_fullname(message: types.Message, state: FSMContext):
    if message.text == "✅ Davom etish":
        return
    fullname = normalize_text(message.text)
    if len(fullname) < 3 or " " not in fullname:
        await message.answer("Ism va familiyani to'liq kiriting. Masalan: Ali Valiyev")
        return

    await state.update_data(fullname=fullname)
    await state.set_state(Register.waiting_contact)
    await message.answer(
        "📱 <b>Kontaktingizni ulashing</b>\n\nPastdagi tugmani bosing.",
        parse_mode="HTML",
        reply_markup=kb_request_contact()
    )

@dp.message(Register.waiting_contact, F.contact)
async def got_contact(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if message.contact.user_id != uid:
        await message.answer("O'zingizning kontaktingizni yuboring.")
        return

    data = await state.get_data()
    fullname = data.get("fullname", "")

    await save_user(uid, {
        "phone": message.contact.phone_number,
        "tgUsername": message.from_user.username,
        "fullname": fullname,
        "registered": True,
        "createdAt": now_utc(),
        "updatedAt": now_utc()
    })
    await state.clear()
    await message.answer(
        f"✅ Ro'yxatdan o'tdingiz, <b>{escape_html(fullname)}</b>!\n\nMenyu:",
        parse_mode="HTML",
        reply_markup=kb_main_menu(uid)
    )

@dp.message(Register.waiting_contact)
async def contact_required(message: types.Message):
    await message.answer("📱 Kontakt ulashish tugmasini bosing.", reply_markup=kb_request_contact())

@dp.message(Register.waiting_fullname)
async def fullname_required(message: types.Message):
    await message.answer("👤 Ism familiyangizni kiriting. Masalan: Ali Valiyev")

@dp.message(F.text == "📝 E'lon joylash")
async def user_post_ad_start(message: types.Message, state: FSMContext):
    if message.chat.type != "private":
        return
    if not await is_registered(message.from_user.id):
        await message.answer("Avval /start orqali ro'yxatdan o'ting.")
        return
    await log_visit(message.from_user.id)
    await state.set_state(UserPostAd.choosing_type)
    await message.answer(
        "📝 <b>E'lon turini tanlang:</b>",
        parse_mode="HTML",
        reply_markup=kb_ad_type()
    )


@dp.message(UserPostAd.choosing_type, F.text.in_(["📦 Yuk e'loni", "🚚 Mashina e'loni"]))
async def user_choose_ad_type(message: types.Message, state: FSMContext):
    ad_type = "cargo" if "Yuk" in message.text else "truck"
    await state.update_data(ad_type=ad_type)
    await state.set_state(UserPostAd.waiting_text)

    example = (
        "Yuk bor\nToshkent - Moskva\n20 tonna\nKartoshka\nTel: +998 90 123 45 67"
        if ad_type == "cargo"
        else "Mashina bor\nFura tent\nToshkent - Buxoro\nTel: +998 90 123 45 67"
    )
    await message.answer(
        f"✍️ <b>E'lon matnini yozing:</b>\n\n<i>Misol:</i>\n<code>{example}</code>",
        parse_mode="HTML",
        reply_markup=kb_cancel()
    )


@dp.message(UserPostAd.choosing_type, F.text == "🔙 Admin panel")
async def user_post_back_to_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Menyu:", reply_markup=kb_main_menu(message.from_user.id))


@dp.message(UserPostAd.waiting_text, F.text)
async def user_ad_text_entered(message: types.Message, state: FSMContext):
    if message.text == "❌ Bekor qilish":
        await state.clear()
        await message.answer("Menyu:", reply_markup=kb_main_menu(message.from_user.id))
        return

    text = message.text.strip()
    if len(text) < 10:
        await message.answer("E'lon matni kamida 10 ta belgi bo'lishi kerak.")
        return

    await state.update_data(ad_text=text)
    await state.set_state(UserPostAd.confirm)

    data = await state.get_data()
    ad_type_label = "📦 Yuk" if data["ad_type"] == "cargo" else "🚚 Mashina"
    await message.answer(
        f"📋 <b>E'lon tekshiruvi:</b>\n\n"
        f"<b>Turi:</b> {ad_type_label}\n"
        f"<b>Matn:</b>\n{escape_html(text)}\n\n"
        f"Joylashni tasdiqlaysizmi?",
        parse_mode="HTML",
        reply_markup=kb_confirm_ad()
    )


@dp.message(UserPostAd.confirm, F.text == "✅ Tasdiqlash")
async def user_confirm_ad(message: types.Message, state: FSMContext):
    data = await state.get_data()
    ad_type = data["ad_type"]
    ad_text = data["ad_text"]
    uid = message.from_user.id
    msg_date = now_utc()
    fake_msg_id = int(uuid.uuid4().int % 2_000_000_000)

    if ad_type == "cargo":
        ok, status = await save_cargo_ad(
            chat_id=uid, message_id=fake_msg_id,
            chat_title="Bot - Foydalanuvchi",
            chat_username=None,
            text=ad_text,
            msg_date_utc=msg_date
        )
    else:
        ok, status = await save_truck_ad(
            chat_id=uid, message_id=fake_msg_id,
            chat_title="Bot - Foydalanuvchi",
            chat_username=None,
            text=ad_text,
            msg_date_utc=msg_date
        )

    # Manual ads ga ham yozish
    manual_expires = now_utc() + timedelta(days=AD_TTL_DAYS)
    async with get_pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO manual_ads(ad_type, text_full, posted_by, expires_at) VALUES($1, $2, $3, $4)",
            ad_type, ad_text, uid, manual_expires
        )

    await state.clear()

    if ok:
        await message.answer(
            "✅ <b>E'loningiz muvaffaqiyatli joylashtirildi!</b>",
            parse_mode="HTML",
            reply_markup=kb_main_menu(message.from_user.id)
        )
    else:
        await message.answer(
            "✅ <b>E'loningiz qabul qilindi!</b>\n\n"
            "(E'lon moderatsiyadan keyin ko'rinadi)",
            parse_mode="HTML",
            reply_markup=kb_main_menu(message.from_user.id)
        )


@dp.message(UserPostAd.confirm, F.text == "❌ Bekor qilish")
async def user_cancel_ad(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Bekor qilindi.", reply_markup=kb_main_menu(message.from_user.id))


@dp.message(F.text == "📦 Yuk e'lonlari")
async def cargo_menu(message: types.Message, state: FSMContext):
    if message.chat.type != "private":
        return
    if not await is_registered(message.from_user.id):
        await message.answer("Avval /start orqali ro'yxatdan o'ting.")
        return
    await log_visit(message.from_user.id)

    # Oldingi detail xabarlarini tozalash
    uid = message.from_user.id
    detail_key = f"detail_{uid}"
    old_mids = SEARCH_SESSIONS.get(detail_key, {}).get("mids", [])
    if old_mids:
        await asyncio.gather(
            *[bot.delete_message(message.chat.id, mid) for mid in old_mids],
            return_exceptions=True,
        )
        SEARCH_SESSIONS.pop(detail_key, None)

    await state.set_state(CargoSearch.waiting_place)
    await message.answer(
        "🏙 <b>Joy nomini kiriting</b>\n\n1 ta joy: <i>Buxoro</i>\n2 ta joy: <i>Buxoro Moskva</i>",
        parse_mode="HTML",
        reply_markup=kb_cancel()
    )

@dp.message(CargoSearch.waiting_place, F.text)
async def cargo_place_entered(message: types.Message, state: FSMContext):
    if message.text == "❌ Bekor qilish":
        await state.clear()
        await message.answer("Menyu:", reply_markup=kb_main_menu(message.from_user.id))
        return

    raw = normalize_text(message.text)
    parts = [normalize_place_token(x) for x in raw.split() if normalize_place_token(x)]
    if not parts:
        await message.answer("Joy nomini to'g'ri kiriting.")
        return

    place1 = parts[0]
    place2 = parts[1] if len(parts) > 1 else None

    await state.clear()
    await message.answer("🔎 Qidiryapman...", reply_markup=kb_main_menu(message.from_user.id))

    sid = make_session_id()
    SEARCH_SESSIONS[sid] = {
        "uid": message.from_user.id,
        "place1": place1,
        "place2": place2,
        "created": time.time(),
        "sent_msg_ids": [],
        "mode": "index",
        "cursors": [None],
        "fb_offsets": {0: 0}
    }
    await send_page(bot, message.chat.id, sid, 0)

@dp.callback_query(F.data.startswith("cargo_nav:"))
async def cargo_nav(callback: types.CallbackQuery):
    try:
        _, sid, page_s = callback.data.split(":", 2)
        page = int(page_s)
    except Exception:
        await callback.answer("Xato", show_alert=True)
        return

    sess = SEARCH_SESSIONS.get(sid)
    if not sess:
        await callback.answer("Qayta qidiring: 📦 Yuk e'lonlari", show_alert=True)
        return
    if callback.from_user.id != sess.get("uid"):
        await callback.answer("Bu sizniki emas", show_alert=True)
        return

    sess["created"] = time.time()
    await callback.answer()
    await send_page(bot, callback.message.chat.id, sid, page)


@dp.callback_query(F.data.startswith("cargo_detail:"))
async def cargo_detail(callback: types.CallbackQuery):
    try:
        _, ad_id = callback.data.split(":", 1)
    except Exception:
        await callback.answer("Xato", show_alert=True)
        return

    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT text_full, link, phone, source_chat_id, source_message_id, active "
            "FROM cargo_ads WHERE ad_id=$1", ad_id
        )

    if not row:
        await callback.answer("E'lon topilmadi", show_alert=True)
        return

    await callback.answer()
    uid = callback.from_user.id
    chat_id = callback.message.chat.id

    # Oldingi detail xabarni o'chirish
    detail_key = f"detail_{uid}"
    old_mids = SEARCH_SESSIONS.get(detail_key, {}).get("mids", [])
    if old_mids:
        await asyncio.gather(
            *[bot.delete_message(chat_id, mid) for mid in old_mids],
            return_exceptions=True,
        )

    text_full = row["text_full"] or "Matn yo'q"
    phone = row["phone"] or ""
    link = row["link"] or ""
    active = row["active"]

    if active:
        msg = f"📋 <b>To'liq e'lon:</b>\n\n{escape_html(text_full)}"
    else:
        msg = "⚠️ <b>Xabar guruhdan o'chirilgan.</b>\n\n"
        msg += f"📋 <b>Saqlangan matn:</b>\n\n{escape_html(text_full)}"

    if phone:
        msg += f"\n\n📞 <b>Telefon:</b> {escape_html(phone)}"
    if link:
        msg += f"\n\n🔗 <a href=\"{link}\">Manba</a>"

    if len(msg) > 4000:
        msg = msg[:4000] + "..."

    m = await bot.send_message(
        chat_id,
        msg,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
    SEARCH_SESSIONS[detail_key] = {"mids": [m.message_id], "created": time.time()}


# ═══════════════════════════════════════════════════════════════════════
#  ADMIN PANEL
# ═══════════════════════════════════════════════════════════════════════

def kb_admin_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Statistika"), KeyboardButton(text="👥 Foydalanuvchilar")],
            [KeyboardButton(text="📅 Oylik hisobot"), KeyboardButton(text="📈 Tashriflar")],
            [KeyboardButton(text="📝 Qo'lda e'lon qo'shish"), KeyboardButton(text="🗑 E'lonlarni boshqarish")],
            [KeyboardButton(text="🔙 Asosiy menyu")],
        ],
        resize_keyboard=True
    )

def kb_ad_type():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📦 Yuk e'loni"), KeyboardButton(text="🚚 Mashina e'loni")],
            [KeyboardButton(text="❌ Bekor qilish")],
        ],
        resize_keyboard=True
    )

def kb_confirm_ad():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Tasdiqlash"), KeyboardButton(text="❌ Bekor qilish")],
        ],
        resize_keyboard=True
    )


@dp.message(Command("admin"))
async def admin_panel_cmd(message: types.Message, state: FSMContext):
    if message.chat.type != "private":
        return
    if not is_admin(message.from_user.id):
        await message.answer("⛔️ Sizda admin huquqi yo'q.")
        return
    await state.clear()
    await message.answer(
        "🛠 <b>Admin Panel</b>\n\nQuyidagi bo'limlardan birini tanlang:",
        parse_mode="HTML",
        reply_markup=kb_admin_menu()
    )


@dp.message(F.text == "🔙 Admin panel")
async def back_to_admin(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("🛠 <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin_menu())


@dp.message(F.text == "🔙 Asosiy menyu")
async def back_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Menyu:", reply_markup=kb_main_menu(message.from_user.id))


@dp.message(F.text == "📊 Statistika")
async def admin_stats(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    async with get_pool().acquire() as conn:
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE registered=true")
        total_cargo = await conn.fetchval("SELECT COUNT(*) FROM cargo_ads WHERE active=true")
        total_truck = await conn.fetchval("SELECT COUNT(*) FROM truck_ads WHERE active=true")
        today_visits = await conn.fetchval(
            "SELECT COUNT(*) FROM visits WHERE visited_at >= CURRENT_DATE"
        )
        today_users = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE created_at >= CURRENT_DATE"
        )
        manual_ads = await conn.fetchval("SELECT COUNT(*) FROM manual_ads WHERE active=true")

    text = (
        "📊 <b>Umumiy Statistika</b>\n\n"
        f"👥 <b>Jami foydalanuvchilar:</b> {total_users}\n"
        f"🆕 <b>Bugungi yangi userlar:</b> {today_users}\n"
        f"👁 <b>Bugungi tashriflar:</b> {today_visits}\n\n"
        f"📦 <b>Faol yuk e'lonlari:</b> {total_cargo}\n"
        f"🚚 <b>Faol mashina e'lonlari:</b> {total_truck}\n"
        f"📝 <b>Admin e'lonlari:</b> {manual_ads}\n"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=kb_admin_menu())


@dp.message(F.text == "👥 Foydalanuvchilar")
async def admin_users(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    async with get_pool().acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM users WHERE registered=true")
        recent = await conn.fetch(
            "SELECT uid, fullname, phone, tg_username, created_at "
            "FROM users WHERE registered=true ORDER BY created_at DESC LIMIT 10"
        )

    lines = [f"👥 <b>Foydalanuvchilar</b> (jami: {total})\n\n<b>Oxirgi 10 ta:</b>\n"]
    for i, r in enumerate(recent, 1):
        name = r["fullname"] or "—"
        phone = r["phone"] or "—"
        username = f"@{r['tg_username']}" if r["tg_username"] else "—"
        dt = r["created_at"].strftime("%d.%m.%Y") if r["created_at"] else "—"
        lines.append(
            f"{i}. {escape_html(name)}\n"
            f"   📞 {escape_html(phone)} | {escape_html(username)}\n"
            f"   📅 {dt}\n"
        )
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=kb_admin_menu())


@dp.message(F.text == "📅 Oylik hisobot")
async def admin_monthly_report(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    async with get_pool().acquire() as conn:
        # Oxirgi 6 oy statistikasi
        monthly = await conn.fetch("""
            SELECT
                TO_CHAR(created_at, 'YYYY-MM') as month,
                COUNT(*) as cnt
            FROM users
            WHERE registered=true AND created_at >= NOW() - INTERVAL '6 months'
            GROUP BY TO_CHAR(created_at, 'YYYY-MM')
            ORDER BY month DESC
        """)
        cargo_monthly = await conn.fetch("""
            SELECT
                TO_CHAR(created_at, 'YYYY-MM') as month,
                COUNT(*) as cnt
            FROM cargo_ads
            WHERE created_at >= NOW() - INTERVAL '6 months'
            GROUP BY TO_CHAR(created_at, 'YYYY-MM')
            ORDER BY month DESC
        """)
        truck_monthly = await conn.fetch("""
            SELECT
                TO_CHAR(created_at, 'YYYY-MM') as month,
                COUNT(*) as cnt
            FROM truck_ads
            WHERE created_at >= NOW() - INTERVAL '6 months'
            GROUP BY TO_CHAR(created_at, 'YYYY-MM')
            ORDER BY month DESC
        """)

    lines = ["📅 <b>Oylik Hisobot</b> (oxirgi 6 oy)\n"]

    lines.append("\n<b>👥 Yangi foydalanuvchilar:</b>")
    for r in monthly:
        lines.append(f"  {r['month']}: <b>{r['cnt']}</b> ta")

    lines.append("\n<b>📦 Yuk e'lonlari:</b>")
    for r in cargo_monthly:
        lines.append(f"  {r['month']}: <b>{r['cnt']}</b> ta")

    lines.append("\n<b>🚚 Mashina e'lonlari:</b>")
    for r in truck_monthly:
        lines.append(f"  {r['month']}: <b>{r['cnt']}</b> ta")

    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=kb_admin_menu())


@dp.message(F.text == "📈 Tashriflar")
async def admin_visits(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    async with get_pool().acquire() as conn:
        today = await conn.fetchval(
            "SELECT COUNT(*) FROM visits WHERE visited_at >= CURRENT_DATE"
        )
        week = await conn.fetchval(
            "SELECT COUNT(*) FROM visits WHERE visited_at >= CURRENT_DATE - INTERVAL '7 days'"
        )
        month = await conn.fetchval(
            "SELECT COUNT(*) FROM visits WHERE visited_at >= CURRENT_DATE - INTERVAL '30 days'"
        )
        unique_today = await conn.fetchval(
            "SELECT COUNT(DISTINCT uid) FROM visits WHERE visited_at >= CURRENT_DATE"
        )
        unique_week = await conn.fetchval(
            "SELECT COUNT(DISTINCT uid) FROM visits WHERE visited_at >= CURRENT_DATE - INTERVAL '7 days'"
        )
        unique_month = await conn.fetchval(
            "SELECT COUNT(DISTINCT uid) FROM visits WHERE visited_at >= CURRENT_DATE - INTERVAL '30 days'"
        )
        # Kunlik tashriflar oxirgi 7 kun
        daily = await conn.fetch("""
            SELECT
                TO_CHAR(visited_at, 'DD.MM') as day,
                COUNT(*) as total,
                COUNT(DISTINCT uid) as unique_cnt
            FROM visits
            WHERE visited_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY TO_CHAR(visited_at, 'DD.MM'), DATE(visited_at)
            ORDER BY DATE(visited_at) DESC
        """)

    lines = [
        "📈 <b>Tashriflar Statistikasi</b>\n",
        f"<b>Bugun:</b> {today} ta tashrif ({unique_today} unique)",
        f"<b>Hafta:</b> {week} ta tashrif ({unique_week} unique)",
        f"<b>Oy:</b> {month} ta tashrif ({unique_month} unique)",
        "\n<b>Kunlik (oxirgi 7 kun):</b>",
    ]
    for r in daily:
        lines.append(f"  {r['day']}: {r['total']} tashrif ({r['unique_cnt']} unique)")

    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=kb_admin_menu())


# ─────── E'lon joylash ───────

@dp.message(F.text == "📝 Qo'lda e'lon qo'shish")
async def admin_post_ad_start(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminPostAd.choosing_type)
    await message.answer(
        "📝 <b>E'lon turini tanlang:</b>",
        parse_mode="HTML",
        reply_markup=kb_ad_type()
    )


@dp.message(AdminPostAd.choosing_type, F.text.in_(["📦 Yuk e'loni", "🚚 Mashina e'loni"]))
async def admin_choose_ad_type(message: types.Message, state: FSMContext):
    ad_type = "cargo" if "Yuk" in message.text else "truck"
    await state.update_data(ad_type=ad_type)
    await state.set_state(AdminPostAd.waiting_text)

    example = (
        "Yuk bor\nToshkent - Moskva\n20 tonna\nKartoshka\nTel: +998 90 123 45 67"
        if ad_type == "cargo"
        else "Mashina bor\nFura tent\nToshkent - Buxoro\nTel: +998 90 123 45 67"
    )
    await message.answer(
        f"✍️ <b>E'lon matnini yozing:</b>\n\n<i>Misol:</i>\n<code>{example}</code>",
        parse_mode="HTML",
        reply_markup=kb_cancel()
    )


@dp.message(AdminPostAd.choosing_type, F.text == "❌ Bekor qilish")
async def admin_post_back(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("🛠 <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin_menu())


@dp.message(AdminPostAd.waiting_text, F.text)
async def admin_ad_text_entered(message: types.Message, state: FSMContext):
    if message.text == "❌ Bekor qilish":
        await state.clear()
        await message.answer("🛠 <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin_menu())
        return

    text = message.text.strip()
    if len(text) < 10:
        await message.answer("E'lon matni kamida 10 ta belgi bo'lishi kerak.")
        return

    await state.update_data(ad_text=text)
    await state.set_state(AdminPostAd.confirm)

    data = await state.get_data()
    ad_type_label = "📦 Yuk" if data["ad_type"] == "cargo" else "🚚 Mashina"
    await message.answer(
        f"📋 <b>E'lon tekshiruvi:</b>\n\n"
        f"<b>Turi:</b> {ad_type_label}\n"
        f"<b>Matn:</b>\n{escape_html(text)}\n\n"
        f"Joylashni tasdiqlaysizmi?",
        parse_mode="HTML",
        reply_markup=kb_confirm_ad()
    )


@dp.message(AdminPostAd.confirm, F.text == "✅ Tasdiqlash")
async def admin_confirm_ad(message: types.Message, state: FSMContext):
    data = await state.get_data()
    ad_type = data["ad_type"]
    ad_text = data["ad_text"]
    uid = message.from_user.id

    msg_date = now_utc()
    fake_msg_id = int(uuid.uuid4().int % 2_000_000_000)

    # Ma'lumotlar bazasiga saqlash
    if ad_type == "cargo":
        ok, status = await save_cargo_ad(
            chat_id=uid, message_id=fake_msg_id,
            chat_title="Admin Panel",
            chat_username=None,
            text=ad_text,
            msg_date_utc=msg_date
        )
    else:
        ok, status = await save_truck_ad(
            chat_id=uid, message_id=fake_msg_id,
            chat_title="Admin Panel",
            chat_username=None,
            text=ad_text,
            msg_date_utc=msg_date
        )

    # Manual ads jadvaliga ham yozish
    manual_expires = now_utc() + timedelta(days=AD_TTL_DAYS)
    async with get_pool().acquire() as conn:
        await conn.execute(
            "INSERT INTO manual_ads(ad_type, text_full, posted_by, expires_at) VALUES($1, $2, $3, $4)",
            ad_type, ad_text, uid, manual_expires
        )

    await state.clear()

    if ok:
        await message.answer(
            "✅ <b>E'lon muvaffaqiyatli joylashtirildi!</b>",
            parse_mode="HTML",
            reply_markup=kb_admin_menu()
        )
    else:
        await message.answer(
            f"⚠️ E'lon saqlandi (manual_ads), lekin asosiy bazaga yozilmadi.\n"
            f"Sabab: {status}\n\n"
            f"(E'lon matni filterga mos kelmagan bo'lishi mumkin. Manual_ads da saqlangan.)",
            reply_markup=kb_admin_menu()
        )


@dp.message(AdminPostAd.confirm, F.text == "❌ Bekor qilish")
async def admin_cancel_ad(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Bekor qilindi.", reply_markup=kb_admin_menu())


@dp.message(F.text == "🗑 E'lonlarni boshqarish")
async def admin_manage_ads(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    async with get_pool().acquire() as conn:
        ads = await conn.fetch(
            "SELECT id, ad_type, text_full, created_at, active FROM manual_ads "
            "ORDER BY created_at DESC LIMIT 10"
        )

    if not ads:
        await message.answer("📝 Hozircha admin e'lonlari yo'q.", reply_markup=kb_admin_menu())
        return

    lines = ["🗑 <b>Admin e'lonlari</b> (oxirgi 10 ta)\n"]
    buttons = []
    for r in ads:
        status = "✅" if r["active"] else "❌"
        ad_type_icon = "📦" if r["ad_type"] == "cargo" else "🚚"
        dt = r["created_at"].strftime("%d.%m %H:%M") if r["created_at"] else "—"
        short = shorten_text(r["text_full"], 60)
        lines.append(f"{status} {ad_type_icon} [{r['id']}] {dt}\n   {escape_html(short)}\n")
        if r["active"]:
            buttons.append([InlineKeyboardButton(
                text=f"🗑 O'chirish #{r['id']}",
                callback_data=f"admin_del_ad:{r['id']}"
            )])

    kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=kb)


@dp.callback_query(F.data.startswith("admin_del_ad:"))
async def admin_delete_ad(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔️ Ruxsat yo'q", show_alert=True)
        return

    try:
        ad_id = int(callback.data.split(":")[1])
    except Exception:
        await callback.answer("Xato", show_alert=True)
        return

    async with get_pool().acquire() as conn:
        await conn.execute("UPDATE manual_ads SET active=false WHERE id=$1", ad_id)

    await callback.answer("✅ E'lon o'chirildi")
    # Xabarni yangilash
    try:
        await callback.message.edit_text(
            callback.message.text + f"\n\n✅ #{ad_id} o'chirildi",
            parse_mode="HTML"
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════
#  TELETHON
# ═══════════════════════════════════════════════════════════════════════

async def get_chat_info_cached(chat_id: int) -> Tuple[str, Optional[str]]:
    now_ts = time.time()
    cached = CHAT_CACHE.get(chat_id)
    if cached and cached[2] > now_ts:
        return cached[0], cached[1]
    try:
        chat = await telethon_client.get_entity(chat_id)
        title = getattr(chat, "title", None) or getattr(chat, "username", None) or str(chat_id)
        username = getattr(chat, "username", None)
    except Exception:
        title, username = str(chat_id), None
    CHAT_CACHE[chat_id] = (title, username, now_ts + CHAT_CACHE_TTL)
    return title, username

@telethon_client.on(events.NewMessage(incoming=True))
async def on_new_message(event: events.NewMessage.Event):
    try:
        # Faqat guruh / kanal / supergroup — shaxsiy chatlarni o'tkazib yuboramiz
        if not (event.is_group or event.is_channel):
            return

        msg = event.message
        if not msg or not msg.message:
            return
        text = msg.message
        if not event.chat_id:
            return

        truck = is_truck_offer_message(text)
        cargo = is_cargo_ad(text)

        if not truck and not cargo:
            return

        msg_date = msg.date if isinstance(msg.date, datetime) else now_utc()
        if msg_date.tzinfo is None:
            msg_date = msg_date.replace(tzinfo=timezone.utc)

        INGEST_QUEUE.put_nowait({
            "chat_id": event.chat_id,
            "message_id": msg.id,
            "text": text,
            "msg_date": msg_date,
            "is_cargo": cargo,
            "is_truck": truck,
        })
        log.debug(
            "INGEST queued chat=%s msg=%s cargo=%s truck=%s",
            event.chat_id, msg.id, cargo, truck
        )
    except asyncio.QueueFull:
        log.warning("INGEST_QUEUE full")
    except Exception:
        log.exception("TELETHON NewMessage error")

@telethon_client.on(events.MessageDeleted)
async def on_deleted(event: events.MessageDeleted.Event):
    try:
        if not getattr(event, "chat_id", None):
            return
        changed = 0
        for mid in list(event.deleted_ids):
            try:
                changed += await deactivate_by_source(event.chat_id, mid)
            except Exception:
                log.exception("deactivate error")
        if changed:
            log.info("deleted updated=%s", changed)
    except Exception:
        log.exception("MessageDeleted error")


# ═══════════════════════════════════════════════════════════════════════
#  INGEST WORKER
# ═══════════════════════════════════════════════════════════════════════

async def ingest_worker(worker_id: int):
    buffer: List[Dict[str, Any]] = []
    last_flush = time.time()

    async def flush(buf):
        if not buf:
            return
        cargo_n = truck_n = 0
        skip_n = 0
        for it in buf:
            saved_as_cargo = False
            try:
                ok, status = await save_cargo_ad(
                    it["chat_id"], it["message_id"], it["chat_title"],
                    it["chat_username"], it["text"], it["msg_date"]
                )
                if ok:
                    cargo_n += 1
                    saved_as_cargo = True
                elif status not in ("filtered",):
                    log.info(
                        "cargo skip chat=%s msg=%s status=%s",
                        it["chat_id"], it["message_id"], status
                    )
                    skip_n += 1
            except Exception:
                log.exception("save_cargo_ad error")

            if not saved_as_cargo:
                try:
                    ok2, status2 = await save_truck_ad(
                        it["chat_id"], it["message_id"], it["chat_title"],
                        it["chat_username"], it["text"], it["msg_date"]
                    )
                    if ok2:
                        truck_n += 1
                    elif status2 not in ("filtered",):
                        log.info(
                            "truck skip chat=%s msg=%s status=%s username=%s",
                            it["chat_id"], it["message_id"], status2, it.get("chat_username")
                        )
                        skip_n += 1
                except Exception:
                    log.exception("save_truck_ad error")

        log.info(
            "worker[%s] flushed=%s cargo=%s truck=%s skip=%s queue=%s",
            worker_id, len(buf), cargo_n, truck_n, skip_n, INGEST_QUEUE.qsize()
        )

    while True:
        try:
            item = await INGEST_QUEUE.get()
            t, u = await get_chat_info_cached(item["chat_id"])
            item["chat_title"] = t
            item["chat_username"] = u
            buffer.append(item)
            INGEST_QUEUE.task_done()

            now_ts = time.time()
            if len(buffer) >= WORKER_BATCH_MAX or (now_ts - last_flush) >= WORKER_BATCH_WINDOW_SEC:
                await flush(buffer)
                buffer.clear()
                last_flush = now_ts

            while len(buffer) < WORKER_BATCH_MAX:
                try:
                    it2 = INGEST_QUEUE.get_nowait()
                except asyncio.QueueEmpty:
                    break
                t2, u2 = await get_chat_info_cached(it2["chat_id"])
                it2["chat_title"] = t2
                it2["chat_username"] = u2
                buffer.append(it2)
                INGEST_QUEUE.task_done()

        except Exception:
            log.exception("ingest_worker error")
            await asyncio.sleep(1)


# ═══════════════════════════════════════════════════════════════════════
#  BACKGROUND TASKS
# ═══════════════════════════════════════════════════════════════════════

async def expire_sweeper():
    while True:
        try:
            async with get_pool().acquire() as conn:
                c1 = await conn.fetchval(
                    "with t as (delete from cargo_ads "
                    "where expires_at <= now() returning 1) select count(*) from t"
                )
                c2 = await conn.fetchval(
                    "with t as (delete from truck_ads "
                    "where expires_at <= now() returning 1) select count(*) from t"
                )
                c3 = await conn.fetchval(
                    "with t as (delete from dedup_hashes where expires_at<=now() returning 1) select count(*) from t"
                )
                c4 = await conn.fetchval(
                    "with t as (delete from manual_ads "
                    "where expires_at is not null and expires_at <= now() returning 1) select count(*) from t"
                )
            if c1 or c2 or c3 or c4:
                log.info("cleanup cargo=%s truck=%s dedup=%s manual=%s", c1, c2, c3, c4)
        except Exception:
            log.exception("expire_sweeper error")
        await asyncio.sleep(EXPIRE_SWEEP_EVERY)

async def gc_task():
    while True:
        try:
            now_ts = time.time()
            for sid in list(SEARCH_SESSIONS):
                if now_ts - SEARCH_SESSIONS[sid].get("created", now_ts) > 86400:
                    SEARCH_SESSIONS.pop(sid, None)

            for sid in list(TRUCK_UI_SESSIONS):
                if now_ts - TRUCK_UI_SESSIONS[sid].get("created", now_ts) > 86400:
                    TRUCK_UI_SESSIONS.pop(sid, None)

            for k in list(CACHE):
                if now_ts - CACHE[k][0] > CACHE_TTL:
                    CACHE.pop(k, None)

            if len(RAM_DEDUP) > 20000:
                for k in list(RAM_DEDUP)[:6000]:
                    if RAM_DEDUP.get(k, 0) < now_ts:
                        RAM_DEDUP.pop(k, None)
        except Exception:
            log.exception("gc_task error")
        await asyncio.sleep(GC_EVERY)

async def run_bot():
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            me = await bot.get_me()
            log.info("Bot started @%s", me.username)
            await dp.start_polling(bot)
        except Exception:
            log.exception("run_bot error")
            await asyncio.sleep(5)

async def run_telethon_forever():
    while True:
        try:
            log.info("Telethon starting...")
            await telethon_client.connect()
            if not await telethon_client.is_user_authorized():
                raise RuntimeError("TG_SESSION noto'g'ri.")
            me = await telethon_client.get_me()
            is_bot_session = bool(getattr(me, "bot", False))
            group_n = None
            if not is_bot_session:
                try:
                    dialogs = await telethon_client.get_dialogs(limit=50)
                    group_n = sum(1 for d in dialogs if d.is_group or d.is_channel)
                except Exception:
                    log.exception("get_dialogs failed (listeners still active)")
            else:
                log.error(
                    "TG_SESSION bot akkaunti! Guruhlardan e'lon olish uchun USER session kerak."
                )
            log.info(
                "Telethon connected @%s id=%s bot=%s recent_groups_channels=%s",
                getattr(me, "username", None), getattr(me, "id", None),
                is_bot_session, group_n,
            )
            await telethon_client.run_until_disconnected()
        except FloodWaitError as e:
            await asyncio.sleep(int(getattr(e, "seconds", 60)) + 5)
        except SessionPasswordNeededError:
            log.error("2FA yoqilgan.")
            await asyncio.sleep(60)
        except Exception:
            log.exception("run_telethon_forever error")
            await asyncio.sleep(15)

async def main():
    await init_db()
    await setup_bot_ui()

    tasks = [
        asyncio.create_task(run_telethon_forever()),
        asyncio.create_task(run_bot()),
        asyncio.create_task(expire_sweeper()),
        asyncio.create_task(gc_task()),
    ]
    for i in range(1, 5):
        tasks.append(asyncio.create_task(ingest_worker(i)))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())