import asyncio
import os
import re
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    KeyboardButton, ReplyKeyboardMarkup,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, firestore

# =============================
# Logging
# =============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("ads-bot")

# =============================
# Env
# =============================
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
FIREBASE_CRED = os.getenv("FIREBASE_CRED")

if not TOKEN:
    raise RuntimeError("BOT_TOKEN topilmadi. .env faylni tekshiring!")
if not FIREBASE_CRED or not os.path.exists(FIREBASE_CRED):
    raise RuntimeError("FIREBASE_CRED topilmadi yoki fayl yo‘li noto‘g‘ri. .env va JSON faylni tekshiring!")

# =============================
# Firebase init
# =============================
cred = credentials.Certificate(FIREBASE_CRED)
firebase_admin.initialize_app(cred)
db = firestore.client()

COL_USERS = "users"
COL_ADS_CARGO = "ads_cargo"

PAGE_SIZE = 5
AD_TTL_DAYS = 5

# =============================
# Bot init
# =============================
bot = Bot(token=TOKEN)
dp = Dispatcher()

# =============================
# States
# =============================
class Register(StatesGroup):
    waiting_contact = State()
    waiting_fullname = State()

class CargoSearch(StatesGroup):
    waiting_city = State()

# =============================
# Keyboards
# =============================
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

# =============================
# Utils
# =============================
def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def normalize_city(s: str) -> str:
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
        "sprinter", "ref", "refrij", "sovutkich"
    ]
    return any(k in t for k in keywords)

# =============================
# City dictionary (MVP)
# =============================
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
    x = normalize_city(input_city)
    for canon, token in ALL_CITY_TOKENS:
        if x == token or token in x:
            return canon
    return x

def extract_cities_by_dictionary(text: str) -> List[str]:
    t = normalize_city(text)
    found = set()
    for canon, token in ALL_CITY_TOKENS:
        if token in t:
            found.add(canon)
    return sorted(found)

# =============================
# Parser: route, weight, vehicle
# =============================
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
    t = normalize_city(text)

    # buxorodan-toshkentga / buxorodan toshkentga
    m = re.search(r"([a-zа-яʻ’`'ʼ\- ]+?)dan\s*[-–—]?\s*([a-zа-яʻ’`'ʼ\- ]+?)ga\b", t)
    if m:
        return canonicalize_city(m.group(1)), canonicalize_city(m.group(2))

    # buxoro - toshkent
    m2 = re.search(r"\b([a-zа-яʻ’`'ʼ]{3,})\s*[-–—]\s*([a-zа-яʻ’`'ʼ]{3,})\b", t)
    if m2:
        return canonicalize_city(m2.group(1)), canonicalize_city(m2.group(2))

    return None, None

# =============================
# Link builder (Batafsil)
# =============================
def make_message_link(chat_id: int, message_id: int, chat_username: Optional[str]) -> Optional[str]:
    # Public group/channel bo'lsa
    if chat_username:
        return f"https://t.me/{chat_username}/{message_id}"

    # Private supergroup bo'lsa: -100xxxxxxxxxx
    s = str(chat_id)
    if s.startswith("-100"):
        internal_id = s[4:]
        return f"https://t.me/c/{internal_id}/{message_id}"

    return None

# =============================
# Firestore: users
# =============================
def user_doc(uid: int):
    return db.collection(COL_USERS).document(str(uid))

def get_user(uid: int) -> Optional[dict]:
    snap = user_doc(uid).get()
    return snap.to_dict() if snap.exists else None

def save_user(uid: int, data: dict):
    user_doc(uid).set(data, merge=True)

def is_registered(uid: int) -> bool:
    u = get_user(uid)
    return bool(u and u.get("registered") is True)

# =============================
# Firestore: ads
# =============================
def ads_col():
    return db.collection(COL_ADS_CARGO)

def save_cargo_ad(
    source_chat_id: int,
    source_msg_id: int,
    source_title: str,
    source_username: Optional[str],
    text: str,
    cities: List[str],
    from_city: Optional[str],
    to_city: Optional[str],
    vehicle: Optional[str],
    weight: Optional[str],
    link: Optional[str],
):
    now = datetime.utcnow()
    expires = now + timedelta(days=AD_TTL_DAYS)
    doc_id = f"{source_chat_id}_{source_msg_id}"

    ads_col().document(doc_id).set({
        "sourceChatId": source_chat_id,
        "sourceMessageId": source_msg_id,
        "sourceTitle": source_title,
        "sourceUsername": source_username,
        "text": text,
        "cities": cities,
        "fromCity": from_city,
        "toCity": to_city,
        "vehicle": vehicle,
        "weight": weight,
        "link": link,
        "createdAt": now,
        "expiresAt": expires,
        "active": True,
    }, merge=True)

def query_ads_by_city(city: str, page: int):
    canon_city = canonicalize_city(city)
    now = datetime.utcnow()

    base = (
        ads_col()
        .where("active", "==", True)
        .where("expiresAt", ">", now)
        .where("cities", "array_contains", canon_city)
        .order_by("expiresAt")
    )

    offset = PAGE_SIZE * page
    docs = base.offset(offset).limit(PAGE_SIZE).stream()
    items = [d.to_dict() | {"_id": d.id} for d in docs]

    next_docs = list(base.offset(offset + PAGE_SIZE).limit(1).stream())
    has_next = len(next_docs) > 0

    return canon_city, items, has_next

# =============================
# Card UI
# =============================
def format_ad_card(ad: dict) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    created = ad["createdAt"].strftime("%Y-%m-%d %H:%M") if ad.get("createdAt") else ""
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
        if cities:
            route = " / ".join(cities)

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

    link = ad.get("link")
    if link:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔎 Batafsil (guruhdagi e’lon)", url=link)]
        ])
        return card, kb

    # link bo'lmasa ham ko'rsatamiz
    return card + "\n\n⚠️ <i>Batafsil link yo‘q (guruh public emas yoki supergroup emas).</i>", None

def kb_page_nav(canon_city: str, page: int, has_next: bool) -> Optional[InlineKeyboardMarkup]:
    btns = []
    if page > 0:
        btns.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=f"cargo_page:{canon_city}:{page-1}"))
    if has_next:
        btns.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=f"cargo_page:{canon_city}:{page+1}"))
    if not btns:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[btns])

async def send_ads_page(chat_id: int, city: str, page: int):
    canon_city, items, has_next = query_ads_by_city(city, page)

    if not items:
        await bot.send_message(chat_id, f"❗️<b>{escape_html(city)}</b> bo‘yicha e’lon topilmadi.", parse_mode="HTML")
        return

    await bot.send_message(
        chat_id,
        f"📦 <b>{escape_html(canon_city)}</b> bo‘yicha e’lonlar\nSahifa: <b>{page+1}</b>",
        parse_mode="HTML"
    )

    for ad in items:
        card_text, kb = format_ad_card(ad)
        await bot.send_message(chat_id, card_text, parse_mode="HTML", reply_markup=kb)

    nav = kb_page_nav(canon_city, page, has_next)
    if nav:
        await bot.send_message(chat_id, "Sahifalash:", reply_markup=nav)

# =============================
# Error handler
# =============================
@dp.error()
async def on_error(event: types.ErrorEvent):
    log.exception("❌ ERROR: %s", event.exception)
    try:
        if event.update and getattr(event.update, "message", None):
            msg = event.update.message
            if msg.chat.type == "private":
                await msg.answer(f"❌ Xatolik: {type(event.exception).__name__}\n{escape_html(str(event.exception))}", parse_mode="HTML")
    except Exception:
        pass
    return True

# =============================
# Startup
# =============================
async def on_startup():
    me = await bot.get_me()
    log.info("✅ Bot ishga tushdi: @%s (id=%s)", me.username, me.id)

# =============================
# /start & registration
# =============================
@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    log.info("START uid=%s chat_type=%s", uid, message.chat.type)

    if message.chat.type != "private":
        await message.answer("Botdan foydalanish uchun menga private yozing: /start")
        return

    u = get_user(uid)
    if u and u.get("registered") is True:
        await state.clear()
        await message.answer("✅ Menyu:", reply_markup=kb_main_menu())
        return

    if not u:
        log.info("🆕 New user uid=%s username=%s", uid, message.from_user.username)

    await state.set_state(Register.waiting_contact)
    await message.answer(
        "Assalomu alaykum!\n\n1) Davom etish uchun 📱 kontaktingizni ulashing.",
        reply_markup=kb_request_contact()
    )

@dp.message(F.text == "❌ Bekor qilish")
async def cancel(message: types.Message, state: FSMContext):
    log.info("CANCEL uid=%s", message.from_user.id)
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
        "updatedAt": datetime.utcnow(),
    })
    log.info("✅ Contact saved uid=%s phone=%s", uid, phone)

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
        "updatedAt": datetime.utcnow(),
    })
    log.info("✅ User registered uid=%s fullname=%s", uid, fullname)

    await state.clear()
    await message.answer("✅ Ro‘yxatdan o‘tdingiz!\nMenyu:", reply_markup=kb_main_menu())

# =============================
# Menu: Cargo Search
# =============================
@dp.message(F.text == "📦 Yuk e'lonlari")
async def cargo_menu(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    log.info("MENU Cargo pressed uid=%s chat_type=%s", uid, message.chat.type)

    if message.chat.type != "private":
        return
    if not is_registered(uid):
        await message.answer("Avval /start orqali ro‘yxatdan o‘ting.")
        return

    await state.set_state(CargoSearch.waiting_city)
    await message.answer(
        "🏙 <b>Shahar nomini kiriting</b>\n\nMasalan: <i>Buxoro</i> yoki <i>Toshkent</i>",
        parse_mode="HTML",
        reply_markup=kb_cancel()
    )

@dp.message(CargoSearch.waiting_city, F.text)
async def cargo_city_entered(message: types.Message, state: FSMContext):
    if message.text == "❌ Bekor qilish":
        await state.clear()
        await message.answer("Menyu:", reply_markup=kb_main_menu())
        return

    city = normalize_text(message.text)
    log.info("CITY entered city=%s", city)

    await state.clear()
    await message.answer("🔎 Qidiryapman...", reply_markup=kb_main_menu())
    await send_ads_page(chat_id=message.chat.id, city=city, page=0)

@dp.callback_query(F.data.startswith("cargo_page:"))
async def cargo_pagination(callback: types.CallbackQuery):
    try:
        _, city, page_str = callback.data.split(":", 2)
        page = int(page_str)
    except Exception:
        await callback.answer("Xato pagination", show_alert=True)
        return

    log.info("PAGINATION city=%s page=%s", city, page)
    await callback.answer()
    await send_ads_page(chat_id=callback.message.chat.id, city=city, page=page)

@dp.message(F.text == "🚚 Yuk mashinalar e'lonlari")
async def trucks_menu(message: types.Message):
    uid = message.from_user.id
    log.info("MENU Trucks pressed uid=%s", uid)

    if message.chat.type != "private":
        return
    if not is_registered(uid):
        await message.answer("Avval /start orqali ro‘yxatdan o‘ting.")
        return

    await message.answer("🚚 Bu bo‘limni keyingi bosqichda ulaymiz.")

# =============================
# Group scanner: save ads
# =============================
@dp.message()
async def group_scanner(message: types.Message):
    if message.chat.type not in ("group", "supergroup"):
        return

    text = message.text or ""
    if not text:
        return

    if not looks_like_cargo_ad(text):
        return

    cities = extract_cities_by_dictionary(text)
    from_city, to_city = parse_route_from_to(text)
    vehicle = parse_vehicle(text)
    weight = parse_weight(text)

    link = make_message_link(
        chat_id=message.chat.id,
        message_id=message.message_id,
        chat_username=getattr(message.chat, "username", None)
    )

    save_cargo_ad(
        source_chat_id=message.chat.id,
        source_msg_id=message.message_id,
        source_title=message.chat.title or "Guruh",
        source_username=getattr(message.chat, "username", None),
        text=text,
        cities=cities,
        from_city=from_city,
        to_city=to_city,
        vehicle=vehicle,
        weight=weight,
        link=link,
    )

    log.info("📩 SAVED AD | chat=%s msg=%s cities=%s from=%s to=%s vehicle=%s weight=%s link=%s",
             message.chat.title, message.message_id, cities, from_city, to_city, vehicle, weight, bool(link))

# =============================
# Main
# =============================
async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())