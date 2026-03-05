import asyncio
import os
import re
import time
import uuid
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv

# --- Aiogram
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    KeyboardButton, ReplyKeyboardMarkup,
    InlineKeyboardMarkup, InlineKeyboardButton
)

# --- Telethon
from telethon import TelegramClient, events

# --- Firebase
import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import AlreadyExists

# =============================
# Logging
# =============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("fast-ads-bot")

# =============================
# Env
# =============================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
FIREBASE_CRED = os.getenv("FIREBASE_CRED")

TG_API_ID = os.getenv("TG_API_ID")
TG_API_HASH = os.getenv("TG_API_HASH")
TG_PHONE = os.getenv("TG_PHONE")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN topilmadi (.env)")
if not FIREBASE_CRED or not os.path.exists(FIREBASE_CRED):
    raise RuntimeError("FIREBASE_CRED topilmadi yoki yo‘l noto‘g‘ri (.env)")
if not TG_API_ID or not TG_API_HASH or not TG_PHONE:
    raise RuntimeError("TG_API_ID / TG_API_HASH / TG_PHONE topilmadi (.env)")

TG_API_ID = int(TG_API_ID)

# =============================
# Firebase init
# =============================
cred = credentials.Certificate(FIREBASE_CRED)
firebase_admin.initialize_app(cred)
db = firestore.client()

COL_USERS = "users"
COL_ADS_CARGO = "ads_cargo"          # full doc (backup/source)
COL_DEDUP = "dedup"                  # dedup locks
COL_CITY_INDEX = "city_index"        # denormalized index for fast queries

PAGE_SIZE = 5
AD_TTL_DAYS = 5
DEDUP_TTL_HOURS = 2

CACHE_TTL = 60
EXPIRE_SWEEP_EVERY = 600  # 10 min
GC_EVERY = 30

# =============================
# PERF: Queue + Workers + RAM dedup + Chat cache
# =============================
INGEST_QUEUE: asyncio.Queue = asyncio.Queue(maxsize=5000)

# Firestore python client sync (blocking) -> threadpoolga chiqaramiz
FS_EXECUTOR = ThreadPoolExecutor(max_workers=8)

# RAM dedup (chat_id + hash) => expires_ts
RAM_DEDUP: Dict[str, float] = {}
RAM_DEDUP_TTL = DEDUP_TTL_HOURS * 3600

# Chat info cache (chat_id => (title, username, expires_ts))
CHAT_CACHE: Dict[int, Tuple[str, Optional[str], float]] = {}
CHAT_CACHE_TTL = 3600  # 1 soat

# City index write amplificationni kamaytirish
MAX_CITIES_TO_INDEX = 3  # 2-3 tavsiya

# Worker batch window
WORKER_BATCH_MAX = 120
WORKER_BATCH_WINDOW_SEC = 0.20

# =============================
# Utils / Parsing
# =============================
def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def normalize_city_text(s: str) -> str:
    s = normalize_text(s).lower()
    s = re.sub(r"[^\w\sʻ’`'ʼ-]", " ", s)
    s = re.sub(r"\b(sh|shahar|tumani|tuman)\b\.?", "", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def escape_html(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def looks_like_cargo_ad(text: str) -> bool:
    t = (text or "").lower()
    keywords = [
        "yuk bor", "yuk", "tonna", "t.", "kg",
        "mashina kerak", "fura", "gazel", "isuzu", "man", "kamaz",
        "sprinter", "ref", "refrij", "sovutkich", "muzlatkich",
        "yuk kerak", "yuk tashish"
    ]
    return any(k in t for k in keywords)

CITY_ALIASES = {
    "toshkent": ["toshkent", "toshkent sh", "toshkent shahri"],
    "buxoro": ["buxoro", "buhoro"],
    "samarqand": ["samarqand", "samarkand"],
    "andijon": ["andijon", "andijan"],
    "namangan": ["namangan"],
    "fargona": ["fargona", "farg‘ona", "fergana"],
    "xorazm": ["xorazm", "xorezm", "urganch", "urgench"],
    "navoiy": ["navoiy", "navoi"],
    "qashqadaryo": ["qashqadaryo", "qashqadar", "qarshi"],
    "surxondaryo": ["surxondaryo", "termiz"],
    "jizzax": ["jizzax", "jizzakh"],
    "sirdaryo": ["sirdaryo", "guliston"],
    "qoraqalpogiston": ["qoraqalpogiston", "nukus", "qaraqalpoq"],
}
ALL_CITY_TOKENS = [(canon, a.lower()) for canon, aliases in CITY_ALIASES.items() for a in aliases]

def canonicalize_city(input_city: str) -> str:
    x = normalize_city_text(input_city)
    for canon, token in ALL_CITY_TOKENS:
        if x == token or token in x:
            return canon
    return x

def extract_cities_by_dictionary(text: str) -> List[str]:
    t = normalize_city_text(text)
    found = set()
    for canon, token in ALL_CITY_TOKENS:
        if token in t:
            found.add(canon)
    return sorted(found)

VEHICLE_KEYWORDS = [
    ("Gazel", ["gazel", "gazelle"]),
    ("Isuzu", ["isuzu"]),
    ("Fura", ["fura", "tirkama", "shalan"]),
    ("MAN", [" man ", "man "]),
    ("Kamaz", ["kamaz", "kamas"]),
    ("Sprinter", ["sprinter"]),
    ("Ref", ["ref", "refrij", "muzlatkich", "sovutkich"]),
]

def parse_weight(text: str) -> Optional[str]:
    t = (text or "").lower()
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(tonna|t|kg)\b", t)
    if not m:
        return None
    val = m.group(1).replace(",", ".")
    unit = m.group(2)
    if unit == "t":
        unit = "tonna"
    return f"{val} {unit}"

def parse_vehicle(text: str) -> Optional[str]:
    t = f" {normalize_text(text).lower()} "
    for name, keys in VEHICLE_KEYWORDS:
        if any(k in t for k in keys):
            return name
    return None

def parse_route_from_to(text: str) -> Tuple[Optional[str], Optional[str]]:
    t = normalize_city_text(text)

    # buxorodan-toshkentga / buxorodan toshkentga
    m = re.search(r"([a-zа-яʻ’`'ʼ\- ]+?)dan\s*[-–—]?\s*([a-zа-яʻ’`'ʼ\- ]+?)ga\b", t)
    if m:
        return canonicalize_city(m.group(1)), canonicalize_city(m.group(2))

    # buxoro - toshkent
    m2 = re.search(r"\b([a-zа-яʻ’`'ʼ]{3,})\s*[-–—]\s*([a-zа-яʻ’`'ʼ]{3,})\b", t)
    if m2:
        return canonicalize_city(m2.group(1)), canonicalize_city(m2.group(2))

    return None, None

def infer_route_by_text_order(text: str, cities: List[str]) -> Tuple[Optional[str], Optional[str]]:
    if len(cities) < 2:
        return None, None

    t = normalize_city_text(text)

    def first_pos_for_city(canon: str) -> int:
        best = 10**9
        for c, token in ALL_CITY_TOKENS:
            if c != canon:
                continue
            p = t.find(token)
            if p != -1 and p < best:
                best = p
        return best

    positions = []
    for c in cities:
        p = first_pos_for_city(c)
        if p != 10**9:
            positions.append((p, c))

    if len(positions) < 2:
        return None, None

    positions.sort(key=lambda x: x[0])
    return positions[0][1], positions[1][1]

def make_message_link(chat_id: int, message_id: int, chat_username: Optional[str]) -> Optional[str]:
    if chat_username:
        return f"https://t.me/{chat_username}/{message_id}"
    s = str(chat_id)
    if s.startswith("-100"):
        internal_id = s[4:]
        return f"https://t.me/c/{internal_id}/{message_id}"
    return None

def text_hash_norm(text: str) -> str:
    t = normalize_text(text).lower()
    t = re.sub(r"[^\w\sʻ’`'ʼ-]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return hashlib.sha1(t.encode("utf-8")).hexdigest()

# =============================
# Firestore helpers
# =============================
def users_col():
    return db.collection(COL_USERS)

def ads_col():
    return db.collection(COL_ADS_CARGO)

def dedup_col():
    return db.collection(COL_DEDUP)

def city_index_col(city: str):
    return db.collection(COL_CITY_INDEX).document(city).collection("ads")

def user_doc(uid: int):
    return users_col().document(str(uid))

def get_user(uid: int) -> Optional[dict]:
    snap = user_doc(uid).get()
    return snap.to_dict() if snap.exists else None

def save_user(uid: int, data: dict):
    user_doc(uid).set(data, merge=True)

def is_registered(uid: int) -> bool:
    u = get_user(uid)
    return bool(u and u.get("registered") is True)

def ad_id(chat_id: int, message_id: int) -> str:
    return f"{chat_id}_{message_id}"

def dedup_id(chat_id: int, thash: str) -> str:
    return f"{chat_id}_{thash}"

def qualifies_as_ad(cities: List[str], vehicle: Optional[str], weight: Optional[str]) -> bool:
    # QATTIQ FILTER:
    #  - kamida 2 shahar
    #  - yoki 1 shahar + (vazn yoki mashina)
    if len(cities) >= 2:
        return True
    if len(cities) >= 1 and (vehicle or weight):
        return True
    return False

def try_dedup_lock(chat_id: int, text: str) -> bool:
    """
    True => yangi
    False => duplicate
    1) RAM TTL dedup (tez)
    2) Firestore dedup lock (backup)
    """
    th = text_hash_norm(text)
    did = dedup_id(chat_id, th)
    now = time.time()

    # 1) RAM dedup
    exp = RAM_DEDUP.get(did)
    if exp and exp > now:
        return False
    RAM_DEDUP[did] = now + RAM_DEDUP_TTL

    # 2) Firestore dedup (backup)
    expires = datetime.now(timezone.utc) + timedelta(hours=DEDUP_TTL_HOURS)
    ref = dedup_col().document(did)
    try:
        ref.create({
            "chat_id": chat_id,
            "hash": th,
            "expiresAt": expires,
            "createdAt": datetime.now(timezone.utc),
        })
        return True
    except AlreadyExists:
        return False
    except Exception:
        log.exception("⚠️ dedup firestore error (RAM ishlayapti)")
        return True

def save_ad_and_index(
    chat_id: int,
    message_id: int,
    chat_title: str,
    chat_username: Optional[str],
    text: str,
    msg_date_utc: datetime
):
    created_at = msg_date_utc
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    expires_at = created_at + timedelta(days=AD_TTL_DAYS)

    cities = extract_cities_by_dictionary(text)
    from_city, to_city = parse_route_from_to(text)
    vehicle = parse_vehicle(text)
    weight = parse_weight(text)

    # Qattiq filter
    if not qualifies_as_ad(cities, vehicle, weight):
        return False, "filtered"

    # Agar aniq route topilmasa, taxmin qilamiz
    if (from_city is None or to_city is None) and len(cities) >= 2:
        inf_from, inf_to = infer_route_by_text_order(text, cities)
        if from_city is None:
            from_city = inf_from
        if to_city is None:
            to_city = inf_to

    # --- City listni kamaytiramiz (write amplification pasayadi)
    if from_city and to_city:
        cities = [from_city, to_city] if from_city != to_city else [from_city]
    else:
        cities = cities[:MAX_CITIES_TO_INDEX]

    # Dedup
    if not try_dedup_lock(chat_id, text):
        return False, "dedup"

    link = make_message_link(chat_id=chat_id, message_id=message_id, chat_username=chat_username)
    aid = ad_id(chat_id, message_id)

    # city_index doc ham cities maydonini saqlaydi (2-shahar AND filter uchun!)
    idx_payload = {
        "adId": aid,
        "cities": cities,
        "sourceChatId": chat_id,
        "sourceMessageId": message_id,
        "sourceTitle": chat_title,
        "sourceUsername": chat_username,
        "text": text,
        "fromCity": from_city,
        "toCity": to_city,
        "vehicle": vehicle,
        "weight": weight,
        "link": link,
        "createdAt": created_at,
        "expiresAt": expires_at,
        "active": True,
        "updatedAt": datetime.now(timezone.utc),
    }

    full_payload = dict(idx_payload)
    full_payload["source"] = "telethon"

    batch = db.batch()
    batch.set(ads_col().document(aid), full_payload, merge=True)
    for c in cities:
        batch.set(city_index_col(c).document(aid), idx_payload, merge=True)
    batch.commit()

    log.info("📩 SAVED | %s | msg=%s | cities=%s | route=%s->%s | %s | %s",
             chat_title, message_id, cities, from_city, to_city, vehicle, weight)
    return True, "saved"

def mark_inactive_everywhere(chat_id: int, message_id: int) -> bool:
    aid = ad_id(chat_id, message_id)
    ref = ads_col().document(aid)
    snap = ref.get()
    if not snap.exists:
        return False

    data = snap.to_dict() or {}
    cities = data.get("cities") or []

    batch = db.batch()
    batch.set(ref, {"active": False, "updatedAt": datetime.now(timezone.utc)}, merge=True)
    for c in cities:
        batch.set(city_index_col(c).document(aid), {"active": False, "updatedAt": datetime.now(timezone.utc)}, merge=True)
    batch.commit()
    return True

# =============================
# FAST pagination via city_index + cursor + optional 2-city AND
# =============================
SEARCH_SESSIONS: Dict[str, Dict[str, Any]] = {}

# CACHE key => (ts, items, next_cursor, has_next)
CACHE: Dict[Tuple[str, str, str, str], Tuple[float, List[dict], Optional[Tuple[datetime, str]], bool]] = {}

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

def make_session_id() -> str:
    return uuid.uuid4().hex[:10]

def query_city_index_page(city1: str, city2: Optional[str], cursor: Optional[Tuple[datetime, str]]):
    """
    city_index/{city1}/ads dan:
    - active==true
    - agar city2 bo'lsa: cities array_contains city2 (AND)
    - orderBy createdAt desc + __name__ desc
    - startAfter cursor
    """
    col = city_index_col(city1)

    q = (
        col.where("active", "==", True)
           .order_by("createdAt", direction=firestore.Query.DESCENDING)
           .order_by("__name__", direction=firestore.Query.DESCENDING)
    )

    if city2:
        q = q.where("cities", "array_contains", city2)

    if cursor:
        created_at, doc_id = cursor
        # start_after order_by ketma-ketligiga mos values:
        # createdAt, __name__ (DocumentReference)
        q = q.start_after([created_at, col.document(doc_id)])

    docs = list(q.limit(PAGE_SIZE + 1).stream())
    has_next = len(docs) > PAGE_SIZE
    docs = docs[:PAGE_SIZE]

    items = []
    next_cursor = None
    for d in docs:
        data = d.to_dict()
        data["_id"] = d.id
        items.append(data)

    if docs:
        last = docs[-1]
        last_data = last.to_dict() or {}
        last_created = last_data.get("createdAt")
        if isinstance(last_created, datetime):
            next_cursor = (last_created, last.id)

    return items, next_cursor, has_next

# =============================
# Bot UI
# =============================
class Register(StatesGroup):
    waiting_contact = State()
    waiting_fullname = State()

class CargoSearch(StatesGroup):
    waiting_city = State()

def kb_request_contact() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Kontakt ulashish", request_contact=True)],
            [KeyboardButton(text="❌ Bekor qilish")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def kb_main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📦 Yuk e'lonlari")],
            [KeyboardButton(text="🚚 Yuk mashinalar e'lonlari")],
        ],
        resize_keyboard=True
    )

def kb_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Bekor qilish")]],
        resize_keyboard=True
    )

def format_ad_card(ad: dict) -> str:
    created = ""
    created_dt = ad.get("createdAt")
    if isinstance(created_dt, datetime):
        created = created_dt.strftime("%Y-%m-%d %H:%M")

    from_city = ad.get("fromCity")
    to_city = ad.get("toCity")
    vehicle = ad.get("vehicle") or "—"
    weight = ad.get("weight") or "—"
    src = ad.get("sourceTitle") or "Guruh"

    route = "—"
    if from_city and to_city:
        route = f"{from_city} → {to_city}"
    else:
        cities = ad.get("cities") or []
        if len(cities) >= 2:
            route = f"{cities[0]} → {cities[1]}"
        elif len(cities) == 1:
            route = cities[0]

    card = (
        f"┏━━━━━━━━━━━━━━━━━━━┓\n"
        f"📦 <b>YUK E’LONI</b>\n"
        f"┗━━━━━━━━━━━━━━━━━━━┛\n"
        f"🕒 <b>Vaqti:</b> {escape_html(created)}\n"
        f"📍 <b>Yo‘nalish:</b> {escape_html(route)}\n"
        f"🚚 <b>Mashina:</b> {escape_html(vehicle)}\n"
        f"⚖️ <b>Vazn:</b> {escape_html(weight)}\n"
        f"🏷 <b>Manba:</b> {escape_html(src)}\n\n"
        f"<b>Matn:</b>\n{escape_html(ad.get('text',''))}"
    )
    return card

def build_card_keyboard(ad_link: Optional[str], session_id: str, page: int, has_prev: bool, has_next: bool, is_last_card: bool):
    rows = []
    if ad_link:
        rows.append([InlineKeyboardButton(text="🔎 Batafsil", url=ad_link)])

    if is_last_card:
        nav = []
        if has_prev:
            nav.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=f"cargo_nav:{session_id}:{page-1}"))
        if has_next:
            nav.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=f"cargo_nav:{session_id}:{page+1}"))
        if nav:
            rows.append(nav)

    if not rows:
        return None
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def send_page(bot: Bot, chat_id: int, session_id: str, page: int):
    sess = SEARCH_SESSIONS.get(session_id)
    if not sess:
        await bot.send_message(chat_id, "Sessiya tugagan. Qayta qidiring: 📦 Yuk e'lonlari", reply_markup=kb_main_menu())
        return

    city1 = sess["city1"]
    city2 = sess.get("city2")
    cursors = sess["cursors"]
    while len(cursors) <= page:
        cursors.append(None)

    cursor = cursors[page]
    cursor_key = ("none", "none")
    if cursor:
        cursor_key = (cursor[0].isoformat(), cursor[1])

    cache_key = (city1, city2 or "-", cursor_key[0], cursor_key[1])
    cached = cache_get(cache_key)
    if cached:
        items, next_cursor, has_next = cached
    else:
        items, next_cursor, has_next = query_city_index_page(city1, city2, cursor)
        cache_set(cache_key, items, next_cursor, has_next)

    if not items:
        if city2:
            await bot.send_message(chat_id, f"❗️<b>{escape_html(city1)} + {escape_html(city2)}</b> bo‘yicha e’lon topilmadi.", parse_mode="HTML")
        else:
            await bot.send_message(chat_id, f"❗️<b>{escape_html(city1)}</b> bo‘yicha e’lon topilmadi.", parse_mode="HTML")
        return

    if next_cursor:
        if len(cursors) <= page + 1:
            cursors.append(next_cursor)
        else:
            cursors[page + 1] = next_cursor
        sess["cursors"] = cursors
        SEARCH_SESSIONS[session_id] = sess

    for i, ad in enumerate(items):
        is_last = (i == len(items) - 1)
        kb = build_card_keyboard(
            ad_link=ad.get("link"),
            session_id=session_id,
            page=page,
            has_prev=(page > 0),
            has_next=has_next,
            is_last_card=is_last
        )
        await bot.send_message(chat_id, format_ad_card(ad), parse_mode="HTML", reply_markup=kb)

# =============================
# Bot + Telethon init
# =============================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
telethon_client = TelegramClient("telethon_session", TG_API_ID, TG_API_HASH)

# =============================
# Error handler (bot)
# =============================
@dp.error()
async def on_error(event: types.ErrorEvent):
    log.exception("❌ BOT ERROR: %s", event.exception)
    try:
        msg = getattr(getattr(event, "update", None), "message", None)
        if msg and msg.chat.type == "private":
            await msg.answer(f"❌ Xatolik: {escape_html(str(event.exception))}", parse_mode="HTML")
    except Exception:
        pass
    return True

# =============================
# Registration + menu
# =============================
@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    log.info("BOT /start uid=%s chat_type=%s", uid, message.chat.type)

    if message.chat.type != "private":
        await message.answer("Botdan foydalanish uchun menga private yozing: /start")
        return

    u = get_user(uid)
    if u and u.get("registered") is True:
        await state.clear()
        await message.answer("✅ Menyu:", reply_markup=kb_main_menu())
        return

    await state.set_state(Register.waiting_contact)
    await message.answer(
        "Assalomu alaykum!\n\n1) Davom etish uchun 📱 kontaktingizni ulashing.",
        reply_markup=kb_request_contact()
    )

@dp.message(F.text == "❌ Bekor qilish")
async def cancel(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Bekor qilindi. /start ni bosing.", reply_markup=types.ReplyKeyboardRemove())

@dp.message(Register.waiting_contact, F.contact)
async def got_contact(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if message.contact.user_id != uid:
        await message.answer("❗Iltimos, aynan o‘zingizning kontaktingizni ulashing.")
        return

    phone = message.contact.phone_number
    save_user(uid, {
        "phone": phone,
        "tgUsername": message.from_user.username,
        "registered": False,
        "updatedAt": datetime.now(timezone.utc),
    })
    log.info("BOT contact saved uid=%s phone=%s", uid, phone)

    await state.set_state(Register.waiting_fullname)
    await message.answer(
        "2) Endi Ism Familiyangizni kiriting.\nMasalan: <b>Ali Valiyev</b>",
        parse_mode="HTML",
        reply_markup=types.ReplyKeyboardRemove()
    )

@dp.message(Register.waiting_contact)
async def contact_required(message: types.Message):
    await message.answer("📱 'Kontakt ulashish' tugmasini bosing.", reply_markup=kb_request_contact())

@dp.message(Register.waiting_fullname, F.text)
async def got_fullname(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    fullname = normalize_text(message.text)

    if len(fullname) < 3 or " " not in fullname:
        await message.answer("❗Iltimos, ism va familiyani to‘liq kiriting. Masalan: Ali Valiyev")
        return

    save_user(uid, {
        "fullname": fullname,
        "registered": True,
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": datetime.now(timezone.utc),
    })
    log.info("BOT user registered uid=%s fullname=%s", uid, fullname)

    await state.clear()
    await message.answer("✅ Ro‘yxatdan o‘tdingiz!\nMenyu:", reply_markup=kb_main_menu())

@dp.message(F.text == "📦 Yuk e'lonlari")
async def cargo_menu(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    if message.chat.type != "private":
        return
    if not is_registered(uid):
        await message.answer("Avval /start orqali ro‘yxatdan o‘ting.")
        return

    await state.set_state(CargoSearch.waiting_city)
    await message.answer(
        "🏙 <b>Shahar(lar) nomini kiriting</b>\n\n"
        "✅ 1 shahar: <i>Buxoro</i>\n"
        "✅ 2 shahar: <i>Toshkent Buxoro</i> (ikkalasi qatnashgan postlar chiqadi)",
        parse_mode="HTML",
        reply_markup=kb_cancel()
    )

@dp.message(CargoSearch.waiting_city, F.text)
async def cargo_city_entered(message: types.Message, state: FSMContext):
    if message.text == "❌ Bekor qilish":
        await state.clear()
        await message.answer("Menyu:", reply_markup=kb_main_menu())
        return

    if message.chat.type != "private":
        return

    raw = normalize_text(message.text)
    if len(raw) < 3:
        await message.answer("❗ Shaharni to‘liq kiriting. Masalan: Buxoro yoki Toshkent Buxoro")
        return

    entered = extract_cities_by_dictionary(raw)

    city1 = entered[0] if len(entered) >= 1 else canonicalize_city(raw)
    city2 = entered[1] if len(entered) >= 2 else None

    await state.clear()
    await message.answer("🔎 Qidiryapman...", reply_markup=kb_main_menu())

    sid = make_session_id()
    SEARCH_SESSIONS[sid] = {
        "uid": message.from_user.id,
        "city1": city1,
        "city2": city2,
        "cursors": [None],
        "created": time.time()
    }

    await send_page(bot, message.chat.id, sid, 0)

@dp.callback_query(F.data.startswith("cargo_nav:"))
async def cargo_nav(callback: types.CallbackQuery):
    try:
        _, sid, page_str = callback.data.split(":", 2)
        page = int(page_str)
    except Exception:
        await callback.answer("Xato", show_alert=True)
        return

    sess = SEARCH_SESSIONS.get(sid)
    if not sess:
        await callback.answer("Sessiya tugagan", show_alert=True)
        return

    if callback.from_user.id != sess.get("uid"):
        await callback.answer("Bu sizniki emas", show_alert=True)
        return

    await callback.answer()
    await send_page(bot, callback.message.chat.id, sid, page)

@dp.message(F.text == "🚚 Yuk mashinalar e'lonlari")
async def trucks_menu(message: types.Message):
    if message.chat.type != "private":
        return
    if not is_registered(message.from_user.id):
        await message.answer("Avval /start orqali ro‘yxatdan o‘ting.")
        return
    await message.answer("🚚 Bu bo‘limni keyingi bosqichda ulaymiz.")

# =============================
# Telethon: chat info cache
# =============================
async def get_chat_info_cached(chat_id: int) -> Tuple[str, Optional[str]]:
    now = time.time()
    cached = CHAT_CACHE.get(chat_id)
    if cached:
        title, username, exp = cached
        if exp > now:
            return title, username

    try:
        chat = await telethon_client.get_entity(chat_id)
        title = getattr(chat, "title", None) or getattr(chat, "username", None) or str(chat_id)
        username = getattr(chat, "username", None)
    except Exception:
        title = str(chat_id)
        username = None

    CHAT_CACHE[chat_id] = (title, username, now + CHAT_CACHE_TTL)
    return title, username

# =============================
# Telethon listeners (FAST: queue only)
# =============================
@telethon_client.on(events.NewMessage)
async def on_new_message(event: events.NewMessage.Event):
    try:
        msg = event.message
        if not msg or not msg.message:
            return

        text = msg.message
        if not looks_like_cargo_ad(text):
            return

        chat_id = event.chat_id
        if not chat_id:
            return

        msg_date = msg.date
        if not isinstance(msg_date, datetime):
            msg_date = datetime.now(timezone.utc)
        if msg_date.tzinfo is None:
            msg_date = msg_date.replace(tzinfo=timezone.utc)

        payload = {
            "chat_id": chat_id,
            "message_id": msg.id,
            "text": text,
            "msg_date": msg_date,
        }

        try:
            INGEST_QUEUE.put_nowait(payload)
        except asyncio.QueueFull:
            log.warning("⚠️ INGEST_QUEUE full, dropping message chat=%s msg=%s", chat_id, msg.id)

    except Exception as e:
        log.exception("❌ TELETHON NewMessage error: %s", e)

@telethon_client.on(events.MessageDeleted)
async def on_deleted(event: events.MessageDeleted.Event):
    try:
        chat_id = getattr(event, "chat_id", None)
        if not chat_id:
            return

        loop = asyncio.get_running_loop()

        def _do_delete(mids):
            changed_any = 0
            for mid in mids:
                try:
                    changed = mark_inactive_everywhere(chat_id, mid)
                    if changed:
                        changed_any += 1
                except Exception:
                    log.exception("❌ mark_inactive_everywhere error chat=%s msg=%s", chat_id, mid)
            return changed_any

        changed_any = await loop.run_in_executor(FS_EXECUTOR, _do_delete, list(event.deleted_ids))
        if changed_any:
            log.info("🗑️ DELETED (tracked) chat_id=%s count=%s", chat_id, changed_any)

    except Exception as e:
        log.exception("❌ TELETHON MessageDeleted error: %s", e)

# =============================
# Ingest worker (batch + threadpool)
# =============================
async def ingest_worker(worker_id: int):
    buffer: List[Dict[str, Any]] = []
    last_flush = time.time()

    async def flush(buf: List[Dict[str, Any]]):
        if not buf:
            return

        def _do_writes(items: List[Dict[str, Any]]):
            ok_cnt = 0
            for it in items:
                try:
                    ok, _ = save_ad_and_index(
                        chat_id=it["chat_id"],
                        message_id=it["message_id"],
                        chat_title=it["chat_title"],
                        chat_username=it["chat_username"],
                        text=it["text"],
                        msg_date_utc=it["msg_date"],
                    )
                    if ok:
                        ok_cnt += 1
                except Exception:
                    log.exception("❌ save_ad_and_index error chat=%s msg=%s", it.get("chat_id"), it.get("message_id"))
            return ok_cnt

        loop = asyncio.get_running_loop()
        ok_cnt = await loop.run_in_executor(FS_EXECUTOR, _do_writes, list(buf))
        log.info("⚡ ingest_worker[%s] flushed=%s saved=%s queue=%s",
                 worker_id, len(buf), ok_cnt, INGEST_QUEUE.qsize())

    while True:
        try:
            item = await INGEST_QUEUE.get()

            title, username = await get_chat_info_cached(item["chat_id"])
            item["chat_title"] = title
            item["chat_username"] = username
            buffer.append(item)
            INGEST_QUEUE.task_done()

            now = time.time()
            if len(buffer) >= WORKER_BATCH_MAX or (now - last_flush) >= WORKER_BATCH_WINDOW_SEC:
                await flush(buffer)
                buffer.clear()
                last_flush = now

            # drain: tez-tez kelsa ham bir urinishda ko'proq yig'amiz
            while len(buffer) < WORKER_BATCH_MAX:
                try:
                    it2 = INGEST_QUEUE.get_nowait()
                except asyncio.QueueEmpty:
                    break
                title2, username2 = await get_chat_info_cached(it2["chat_id"])
                it2["chat_title"] = title2
                it2["chat_username"] = username2
                buffer.append(it2)
                INGEST_QUEUE.task_done()

        except Exception:
            log.exception("❌ ingest_worker error")
            await asyncio.sleep(0.2)

# =============================
# Background tasks: expire sweep + GC
# =============================
async def expire_sweeper():
    """
    expiresAt <= now bo'lganlarni active=false.
    ads_cargo va city_index ichida ham.
    """
    while True:
        try:
            now_dt = datetime.now(timezone.utc)

            def _expire_once():
                # ads expire
                q = (
                    ads_col()
                    .where("active", "==", True)
                    .where("expiresAt", "<=", now_dt)
                    .order_by("expiresAt")
                    .limit(200)
                )
                docs = list(q.stream())
                if docs:
                    batch = db.batch()
                    for d in docs:
                        data = d.to_dict() or {}
                        cities = data.get("cities") or []
                        batch.set(d.reference, {"active": False, "updatedAt": now_dt}, merge=True)
                        for c in cities:
                            batch.set(city_index_col(c).document(d.id), {"active": False, "updatedAt": now_dt}, merge=True)
                    batch.commit()
                return len(docs)

            def _dedup_cleanup_once():
                dq = (
                    dedup_col()
                    .where("expiresAt", "<=", now_dt)
                    .order_by("expiresAt")
                    .limit(300)
                )
                dd = list(dq.stream())
                if dd:
                    batch2 = db.batch()
                    for d in dd:
                        batch2.delete(d.reference)
                    batch2.commit()
                return len(dd)

            loop = asyncio.get_running_loop()
            expired_cnt = await loop.run_in_executor(FS_EXECUTOR, _expire_once)
            if expired_cnt:
                log.info("🧹 EXPIRE sweep: %s docs inactive", expired_cnt)

            dd_cnt = await loop.run_in_executor(FS_EXECUTOR, _dedup_cleanup_once)
            if dd_cnt:
                log.info("🧹 DEDUP cleanup: %s docs deleted", dd_cnt)

        except Exception as e:
            log.exception("❌ EXPIRE sweep error: %s", e)

        await asyncio.sleep(EXPIRE_SWEEP_EVERY)

async def gc_task():
    while True:
        try:
            now = time.time()

            # sessions gc
            to_del = [sid for sid, s in SEARCH_SESSIONS.items() if now - s.get("created", now) > 1200]
            for sid in to_del:
                SEARCH_SESSIONS.pop(sid, None)

            # cache gc
            for k in list(CACHE.keys()):
                v = CACHE.get(k)
                if not v:
                    continue
                ts = v[0]
                if now - ts > CACHE_TTL:
                    CACHE.pop(k, None)

            # ram dedup gc (ixtiyoriy, yengil)
            if len(RAM_DEDUP) > 20000:
                for k in list(RAM_DEDUP.keys())[:5000]:
                    if RAM_DEDUP.get(k, 0) < now:
                        RAM_DEDUP.pop(k, None)

            if to_del:
                log.info("🧽 GC: sessions_removed=%s cache_size=%s queue=%s",
                         len(to_del), len(CACHE), INGEST_QUEUE.qsize())
        except Exception as e:
            log.exception("❌ GC error: %s", e)

        await asyncio.sleep(GC_EVERY)

# =============================
# Run both
# =============================
async def run_bot():
    await bot.delete_webhook(drop_pending_updates=True)
    me = await bot.get_me()
    log.info("✅ Aiogram bot ishga tushdi: @%s (id=%s)", me.username, me.id)
    await dp.start_polling(bot)

async def run_telethon():
    log.info("✅ Telethon ishga tushyapti... (1-marta code so‘rashi mumkin)")
    await telethon_client.start(phone=TG_PHONE)
    me = await telethon_client.get_me()
    log.info("✅ Telethon ulandi: @%s (id=%s)", getattr(me, "username", None), me.id)
    await telethon_client.run_until_disconnected()

async def main():
    # 4 ta worker (serveringiz kuchiga qarab 2-8)
    workers = [ingest_worker(i) for i in range(1, 5)]

    await asyncio.gather(
        run_telethon(),
        run_bot(),
        expire_sweeper(),
        gc_task(),
        *workers,
    )

if __name__ == "__main__":
    asyncio.run(main())