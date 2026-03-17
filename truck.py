import asyncio
import re
import time
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

from aiogram import F, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

PAGE_SIZE = 5
TRUCK_CACHE_TTL = 300

TRUCK_UI_SESSIONS: Dict[str, Dict[str, Any]] = {}
TRUCK_CACHE: Dict[
    Tuple[str, str, str, str, str],
    Tuple[float, List[dict], Optional[Tuple[datetime, str]], bool]
] = {}

# ─────────────────────────────────────────────
#  FILTER CONSTANTS
# ─────────────────────────────────────────────

_TRUCK_OFFER_PHRASES = [
    r"\byuk\s+kerak\b",
    r"\byuk\s+k[ke]+r[ae][kq]?\b",
    r"\bюк\s+керак\b",
    r"\bюк\s+нужн",
    r"\bюк\s+нужен\b",
    r"\bюк\s+нужна\b",
    r"\bюк\s+нужно\b",
    r"\byuk\s+kk\b",
    r"\byuk\s+kere\b",

    r"\bmashina\s+bor\b",
    r"\bмашина\s+есть\b",
    r"\bмашина\s+бор\b",
    r"\bmashina\s+kerak\b",
    r"\bмашина\s+керак\b",
    r"\bмашина\s+нужн",

    r"\bfura\s+bor\b",
    r"\bfura\s+kerak\b",
    r"\bfura\s+kk\b",
    r"\bфура\s+есть\b",
    r"\bфура\s+бор\b",
    r"\bфура\s+керак\b",
    r"\bфура\s+нужн",

    r"\btent\s+bor\b",
    r"\btent\s+kerak\b",
    r"\btent\s+kk\b",
    r"\bтент\s+есть\b",
    r"\bтент\s+бор\b",
    r"\bтент\s+керак\b",
    r"\bтент\s+нужн",

    r"\bref\s+bor\b",
    r"\bref\s+kerak\b",
    r"\bref\s+kk\b",
    r"\bреф\s+есть\b",
    r"\bреф\s+бор\b",
    r"\bреф\s+керак\b",
    r"\bреф\s+нужн",
    r"\brefrijerator\s+bor\b",
    r"\brefrijerator\s+kerak\b",
    r"\brefrijerator\s+kk\b",
    r"\bрефрижератор\s+есть\b",
    r"\bрефрижератор\s+бор\b",
    r"\bрефрижератор\s+керак\b",
    r"\bрефрижератор\s+нужн",
    r"\brefka\s+bor\b",
    r"\brefka\s+kerak\b",
    r"\bрефка\s+есть\b",
    r"\bрефка\s+бор\b",

    r"\blabo\s+bor\b",
    r"\blabo\s+kerak\b",
    r"\blabo\s+kk\b",
    r"\bлабо\s+есть\b",
    r"\bлабо\s+бор\b",
    r"\bлабо\s+керак\b",

    r"\bisuzu\s+bor\b",
    r"\bisuzu\s+kerak\b",
    r"\bisuzu\s+kk\b",

    r"\bkamaz\s+bor\b",
    r"\bkamaz\s+kerak\b",
    r"\bkamaz\s+kk\b",
    r"\bкамаз\s+есть\b",
    r"\bкамаз\s+бор\b",
    r"\bкамаз\s+керак\b",
    r"\bкамаз\s+нужн",

    r"\bhowo\s+bor\b",
    r"\bhowo\s+kerak\b",
    r"\bhowo\s+kk\b",
    r"\bhovo\s+bor\b",
    r"\bhovo\s+kerak\b",
    r"\bhovo\s+kk\b",
    r"\bхово\s+есть\b",
    r"\bхово\s+бор\b",
    r"\bхово\s+керак\b",

    r"\bgazel\s+bor\b",
    r"\bgazel\s+kerak\b",
    r"\bgazel\s+kk\b",
    r"\bгазель\s+есть\b",
    r"\bгазель\s+бор\b",
    r"\bгазель\s+керак\b",

    r"\bsprinter\s+bor\b",
    r"\bsprinter\s+kerak\b",
    r"\bsprinter\s+kk\b",
    r"\bспринтер\s+есть\b",
    r"\bспринтер\s+бор\b",
    r"\bспринтер\s+керак\b",

    r"\bman\s+bor\b",
    r"\bman\s+kerak\b",
    r"\bman\s+kk\b",

    r"\bbosh\s+mashina\b",
    r"\bbo[''`]?sh\s+mashina\b",
    r"\bбуш\s+машина\b",
    r"\bпустой?\s+машин",
    r"\bпустая\s+машин",
    r"\byuksiz\b",
    r"\bбез\s+груза\b",
]

_TRUCK_OFFER_RE = [re.compile(p, re.IGNORECASE) for p in _TRUCK_OFFER_PHRASES]

_TRUCK_NEGATIVE_PHRASES = [
    r"\bsotiladi\b",
    r"\bsotamiz\b",
    r"\bnarxi\b",
    r"\bnarx\b",
    r"\bцена\b",
    r"\bпродается\b",
    r"\bпродаём\b",
    r"\bпродаем\b",
    r"\bskidka\b",
    r"\bакция\b",
    r"\bчисто\s+груз\b",
    r"\bюк\s+bor\b",
    r"\bюк\s+есть\b",
]
_TRUCK_NEGATIVE_RE = [re.compile(p, re.IGNORECASE) for p in _TRUCK_NEGATIVE_PHRASES]


# ─────────────────────────────────────────────
#  VEHICLE TYPE PARSER
# ─────────────────────────────────────────────

_VEHICLE_RULES: List[Tuple[str, List[str]]] = [
    ("Tent", [
        r"\btent\b", r"\bтент\b",
    ]),
    ("Ref", [
        r"\bref\b", r"\brefrij\b", r"\brefka\b",
        r"\brefrijerator\b", r"\brefrizherator\b", r"\brefrigerator\b",
        r"\bреф\b", r"\bрефриж", r"\bрефрижератор\b", r"\bрефка\b",
        r"\bmuzlat", r"\bsovut",
    ]),
    ("Fura", [r"\bfura\b", r"\bфура\b"]),
    ("Kamaz", [r"\bkamaz\b", r"\bкамаз\b", r"\bkamas\b"]),
    ("Hovo", [r"\bhowo\b", r"\bhovo\b", r"\bхово\b", r"\bхова\b"]),
    ("Isuzu", [r"\bisuzu\b"]),
    ("Gazel", [r"\bgazel\b", r"\bgazelle\b", r"\bгазел", r"\bгазель"]),
    ("MAN", [r"\bman\b"]),
    ("Sprinter", [r"\bsprinter\b", r"\bспринтер\b"]),
    ("Labo", [r"\blabo\b", r"\bлабо\b"]),
]
_VEHICLE_RE: List[Tuple[str, List[re.Pattern]]] = [
    (name, [re.compile(p, re.IGNORECASE) for p in patterns])
    for name, patterns in _VEHICLE_RULES
]


# ─────────────────────────────────────────────
#  COUNTRY / REGION DETECTION
# ─────────────────────────────────────────────

_UZ_REGIONS = {
    "toshkent":    {"label": "Toshkent",           "aliases": ["toshkent", "ташкент", "tashkent"]},
    "samarqand":   {"label": "Samarqand",          "aliases": ["samarqand", "самарканд", "samarkand"]},
    "buxoro":      {"label": "Buxoro",             "aliases": ["buxoro", "bukhara", "бухоро", "бухара"]},
    "navoiy":      {"label": "Navoiy",             "aliases": ["navoiy", "навои"]},
    "qashqadaryo": {"label": "Qashqadaryo",        "aliases": ["qashqadaryo", "qarshi", "карши", "кашкадарё"]},
    "surxondaryo": {"label": "Surxondaryo",        "aliases": ["surxondaryo", "termiz", "термиз", "сурхондарё"]},
    "andijon":     {"label": "Andijon",            "aliases": ["andijon", "андижан"]},
    "fargona":     {"label": "Farg'ona",           "aliases": ["fargona", "farg'ona", "фаргона", "fergana"]},
    "namangan":    {"label": "Namangan",           "aliases": ["namangan", "наманган"]},
    "xorazm":      {"label": "Xorazm",             "aliases": ["xorazm", "urgench", "ургенч", "хорезм"]},
    "jizzax":      {"label": "Jizzax",             "aliases": ["jizzax", "джизак"]},
    "sirdaryo":    {"label": "Sirdaryo",           "aliases": ["sirdaryo", "сирдарё"]},
    "qoraqalpoq":  {"label": "Qoraqalpog'iston",   "aliases": ["nukus", "нукус", "qoraqalpoq", "каракалпак"]},
}

_COUNTRY_MAP = {
    "uzbekistan": {
        "label": "O'zbekiston",
        "aliases": [
            "uzbekistan", "o'zbekiston", "ozbekiston",
            "узбекистан", "узб", "uzb",
            "toshkent", "ташкент", "tashkent",
            "samarqand", "самарканд",
            "buxoro", "бухоро",
            "navoiy", "навои",
            "qashqadaryo", "qarshi", "карши",
            "surxondaryo", "termiz", "термиз",
            "andijon", "андижан",
            "fargona", "фаргона",
            "namangan", "наманган",
            "xorazm", "urgench", "ургенч",
            "jizzax", "джизак",
            "sirdaryo", "сирдарё",
            "nukus", "нукус",
        ],
    },
    "russia": {
        "label": "Rossiya",
        "aliases": [
            "russia", "rossiya", "россия", "рф",
            "moskva", "москва", "moscow",
            "piter", "питер", "санкт-петербург", "spb",
            "екатеринбург", "новосибирск", "краснодар",
            "тюмень", "челябинск", "уфа", "казань",
        ],
    },
    "kazakhstan": {
        "label": "Qozog'iston",
        "aliases": [
            "kazakhstan", "qozogiston", "қозоғистон",
            "казахстан", "казахи",
            "almaty", "алматы", "алмата",
            "astana", "астана", "нур-султан",
            "shymkent", "шымкент",
        ],
    },
    "kyrgyzstan": {
        "label": "Qirg'iziston",
        "aliases": [
            "kyrgyzstan", "qirgiziston", "киргизия", "кыргызстан",
            "bishkek", "бишкек",
            "osh", "ош",
        ],
    },
    "tajikistan": {
        "label": "Tojikiston",
        "aliases": [
            "tajikistan", "tojikiston", "таджикистан",
            "dushanbe", "душанбе",
            "хужанд", "худжанд",
        ],
    },
    "turkey": {
        "label": "Turkiya",
        "aliases": [
            "turkey", "turkiya", "турция",
            "istanbul", "стамбул", "istanbul",
            "ankara", "анкара",
            "izmir", "измир",
        ],
    },
    "china": {
        "label": "Xitoy",
        "aliases": [
            "china", "xitoy", "китай",
            "beijing", "пекин", "shanghai", "шанхай",
            "guangzhou", "гуанчжоу", "urumqi", "урумчи",
        ],
    },
    "other": {
        "label": "Boshqa",
        "aliases": [],
    },
}


def _normalize_for_detect(text: str) -> str:
    t = re.sub(r"\s+", " ", (text or "")).strip().lower()
    t = t.replace("ё", "е")
    return t


def detect_truck_country_region(text: str):
    t = _normalize_for_detect(text)

    for region_id, info in _UZ_REGIONS.items():
        for alias in info["aliases"]:
            if alias in t:
                return (
                    "uzbekistan",
                    _COUNTRY_MAP["uzbekistan"]["label"],
                    region_id,
                    info["label"],
                )

    for country_id, info in _COUNTRY_MAP.items():
        if country_id in ("uzbekistan", "other"):
            continue
        for alias in info["aliases"]:
            if alias in t:
                return country_id, info["label"], None, None

    return "uzbekistan", _COUNTRY_MAP["uzbekistan"]["label"], None, None


# ─────────────────────────────────────────────
#  MAIN FILTER FUNCTION
# ─────────────────────────────────────────────

def is_truck_offer_message(text: str) -> bool:
    if not text or len(text.strip()) < 5:
        return False

    t = text.lower()

    for neg_re in _TRUCK_NEGATIVE_RE:
        if neg_re.search(t):
            return False

    for offer_re in _TRUCK_OFFER_RE:
        if offer_re.search(t):
            return True

    return False


def parse_truck_vehicle(text: str) -> Optional[str]:
    t = text.lower()
    found = []
    for name, patterns in _VEHICLE_RE:
        if any(p.search(t) for p in patterns):
            found.append(name)
    return ", ".join(dict.fromkeys(found)) if found else None


def parse_truck_phone(text: str) -> Optional[str]:
    _PHONE_PATTERNS = [
        r"(\+998[\s\-]?\(?\d{2}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2})",
        r"(\b998[\s\-]?\(?\d{2}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}\b)",
        r"(\b\d{2}[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}\b)",
    ]
    t = re.sub(r"\s+", " ", text).strip()
    for p in _PHONE_PATTERNS:
        m = re.search(p, t, re.IGNORECASE)
        if m:
            phone = re.sub(r"[^\d+]", " ", m.group(1))
            return re.sub(r"\s+", " ", phone).strip()
    return None


def parse_truck_weight(text: str) -> Optional[str]:
    m = re.search(
        r"(\d+(?:[.,]\d+)?)\s*(tonna|t|kg|тонна|кг)\b",
        text.lower(),
        re.IGNORECASE,
    )
    if not m:
        return None
    val = m.group(1).replace(",", ".")
    unit = m.group(2).lower()
    if unit in ("t", "тонна"):
        unit = "tonna"
    elif unit == "кг":
        unit = "kg"
    return f"{val} {unit}"


def shorten_truck_preview(text: str, max_len: int = 160) -> str:
    t = re.sub(r"\s+", " ", (text or "")).strip()
    if len(t) <= max_len:
        return t
    cut = t[:max_len].rsplit(" ", 1)[0].strip()
    return (cut or t[:max_len]).strip() + "..."


def register_truck_module(dp, bot, deps: dict):
    get_db_pool     = deps["get_db_pool"]
    is_registered   = deps["is_registered"]
    kb_main_menu    = deps["kb_main_menu"]
    escape_html     = deps["escape_html"]
    make_session_id = deps["make_session_id"]

    def cache_get(key):
        v = TRUCK_CACHE.get(key)
        if not v:
            return None
        ts, items, next_cursor, has_next = v
        if time.time() - ts > TRUCK_CACHE_TTL:
            TRUCK_CACHE.pop(key, None)
            return None
        return items, next_cursor, has_next

    def cache_set(key, items, next_cursor, has_next):
        TRUCK_CACHE[key] = (time.time(), items, next_cursor, has_next)

    async def get_total_count() -> int:
        pool = get_db_pool()
        async with pool.acquire() as conn:
            total = await conn.fetchval(
                "select count(*) from truck_ads where active = true"
            )
        return int(total or 0)

    async def get_country_counts() -> List[dict]:
        pool = get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select
                    country as id,
                    max(country_label) as label,
                    count(*) as count
                from truck_ads
                where active = true
                group by country
                having count(*) > 0
                order by count(*) desc, max(country_label) asc
                limit 30
                """
            )
        return [
            {
                "id": r["id"],
                "label": r["label"] or str(r["id"]).title(),
                "count": int(r["count"] or 0),
            }
            for r in rows
        ]

    async def get_region_counts(country: str) -> List[dict]:
        pool = get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select
                    region as id,
                    max(region_label) as label,
                    count(*) as count
                from truck_ads
                where active = true
                  and country = $1
                  and region is not null
                group by region
                having count(*) > 0
                order by count(*) desc, max(region_label) asc
                limit 50
                """,
                country,
            )
        return [
            {
                "id": r["id"],
                "label": r["label"] or str(r["id"]).title(),
                "count": int(r["count"] or 0),
            }
            for r in rows
        ]

    async def query_country_page(country: str, cursor: Optional[Tuple[datetime, str]]):
        pool = get_db_pool()
        params: List[Any] = [country]
        where_sql = "active = true and country = $1"

        if cursor:
            params.extend([cursor[0], cursor[1]])
            where_sql += " and (created_at, ad_id) < ($2, $3)"

        sql = f"""
            select
                ad_id, text_short as text, text_norm,
                vehicle, phone, weight,
                country, country_label, region, region_label,
                link, active, created_at, updated_at, expires_at
            from truck_ads
            where {where_sql}
            order by created_at desc, ad_id desc
            limit {PAGE_SIZE + 1}
        """
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        has_next = len(rows) > PAGE_SIZE
        rows = rows[:PAGE_SIZE]
        items = [dict(r) for r in rows]
        next_cursor = None
        if rows:
            last = rows[-1]
            next_cursor = (last["created_at"], last["ad_id"])
        return items, next_cursor, has_next

    async def query_region_page(country: str, region: str, cursor: Optional[Tuple[datetime, str]]):
        pool = get_db_pool()
        params: List[Any] = [country, region]
        where_sql = "active = true and country = $1 and region = $2"

        if cursor:
            params.extend([cursor[0], cursor[1]])
            where_sql += " and (created_at, ad_id) < ($3, $4)"

        sql = f"""
            select
                ad_id, text_short as text, text_norm,
                vehicle, phone, weight,
                country, country_label, region, region_label,
                link, active, created_at, updated_at, expires_at
            from truck_ads
            where {where_sql}
            order by created_at desc, ad_id desc
            limit {PAGE_SIZE + 1}
        """
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        has_next = len(rows) > PAGE_SIZE
        rows = rows[:PAGE_SIZE]
        items = [dict(r) for r in rows]
        next_cursor = None
        if rows:
            last = rows[-1]
            next_cursor = (last["created_at"], last["ad_id"])
        return items, next_cursor, has_next

    def build_rows(buttons: List[InlineKeyboardButton], row_size: int = 2):
        return [buttons[i:i + row_size] for i in range(0, len(buttons), row_size)]

    def format_truck_item(ad: dict, idx: int) -> str:
        country_label = ad.get("country_label") or "—"
        region_label  = ad.get("region_label")  or "—"
        vehicle = ad.get("vehicle") or "Ko'rsatilmagan"
        phone   = ad.get("phone")   or "Ko'rsatilmagan"
        weight  = ad.get("weight")  or "—"
        text    = ad.get("text")    or ""
        link    = ad.get("link")

        hudud = region_label if region_label != "—" else country_label
        valid_link = bool(link and re.match(r"^https://t\.me/[A-Za-z0-9_]{5,}/\d+$", link))
        details = f'<a href="{link}">Batafsil</a>' if valid_link else "Batafsil yo'q"

        return (
            f"<b>{idx}.</b> 📍 <b>Hudud:</b> {escape_html(hudud)}\n"
            f"🚚 <b>Mashina:</b> {escape_html(vehicle)}\n"
            f"⚖️ <b>Sig'im:</b> {escape_html(weight)}\n"
            f"📞 <b>Telefon:</b> {escape_html(phone)}\n"
            f"📝 <b>Qisqa:</b> {escape_html(text)}\n"
            f"🔗 {details}"
        )

    def build_page_text(items: List[dict], title: str, page: int) -> str:
        lines = [
            "🚚 <b>Yuk mashinalar e'lonlari</b>",
            f"🔎 <b>Bo'lim:</b> {escape_html(title)}",
            f"📄 <b>Sahifa:</b> {page + 1}",
            "",
        ]
        for i, ad in enumerate(items, start=1):
            lines.append(format_truck_item(ad, i))
            if i != len(items):
                lines.append("────────────")
        text = "\n".join(lines)

        if len(text) <= 3900:
            return text

        short = [
            "🚚 <b>Yuk mashinalar e'lonlari</b>",
            f"🔎 <b>Bo'lim:</b> {escape_html(title)}",
            f"📄 <b>Sahifa:</b> {page + 1}",
            "",
        ]
        for i, ad in enumerate(items, start=1):
            vehicle      = ad.get("vehicle") or "—"
            phone        = ad.get("phone")   or "—"
            region_label = ad.get("region_label") or ad.get("country_label") or "—"
            link         = ad.get("link")
            valid_link   = bool(link and re.match(r"^https://t\.me/[A-Za-z0-9_]{5,}/\d+$", link))
            details      = f'<a href="{link}">Batafsil</a>' if valid_link else "Batafsil yo'q"
            short.append(
                f"<b>{i}.</b> {escape_html(region_label)} | "
                f"{escape_html(vehicle)} | {escape_html(phone)} | {details}"
            )
        return "\n".join(short)

    async def delete_session_messages(chat_id: int, sid: str):
        sess = TRUCK_UI_SESSIONS.get(sid)
        if not sess:
            return
        mids = sess.get("sent_msg_ids") or []
        if not mids:
            return

        async def _del(mid: int):
            try:
                await bot.delete_message(chat_id, mid)
            except Exception:
                pass

        await asyncio.gather(*[_del(mid) for mid in mids], return_exceptions=True)
        sess["sent_msg_ids"] = []
        TRUCK_UI_SESSIONS[sid] = sess

    async def send_country_menu(chat_id: int, sid: str):
        await delete_session_messages(chat_id, sid)

        total     = await get_total_count()
        countries = await get_country_counts()

        if total <= 0 or not countries:
            m = await bot.send_message(
                chat_id,
                "🚚 Hozircha yuk mashina e'lonlari topilmadi.",
                reply_markup=kb_main_menu(),
            )
            sess = TRUCK_UI_SESSIONS.get(sid, {})
            sess["sent_msg_ids"] = [m.message_id]
            TRUCK_UI_SESSIONS[sid] = sess
            return

        text = [f"🚚 <b>Jami truck e'lonlar:</b> {total}", "", "🌍 <b>Davlatni tanlang:</b>"]

        buttons = [
            InlineKeyboardButton(
                text=f"{c['label']} ({c['count']})",
                callback_data=f"tr_country:{sid}:{c['id']}",
            )
            for c in countries
        ]

        kb = InlineKeyboardMarkup(inline_keyboard=build_rows(buttons, 2))
        m  = await bot.send_message(chat_id, "\n".join(text), parse_mode="HTML", reply_markup=kb)
        sess = TRUCK_UI_SESSIONS.get(sid, {})
        sess["sent_msg_ids"] = [m.message_id]
        TRUCK_UI_SESSIONS[sid] = sess

    async def send_region_menu(chat_id: int, sid: str, country: str):
        sess = TRUCK_UI_SESSIONS.get(sid)
        if not sess:
            return

        await delete_session_messages(chat_id, sid)

        regions        = await get_region_counts(country)
        country_counts = await get_country_counts()
        country_label  = next(
            (x["label"] for x in country_counts if x["id"] == country),
            country.title(),
        )

        if not regions:
            sess.update({
                "mode": "list_country",
                "country": country,
                "region": None,
                "title": country_label,
                "cursors": [None],
            })
            TRUCK_UI_SESSIONS[sid] = sess
            await send_list_page(chat_id, sid, 0)
            return

        total_in_country = sum(x["count"] for x in regions)

        text = [
            f"🌍 <b>{escape_html(country_label)}</b>",
            "",
            "📍 <b>Viloyat / hududni tanlang:</b>",
        ]

        buttons = [
            InlineKeyboardButton(
                text=f"📦 Barchasi ({total_in_country})",
                callback_data=f"tr_all_country:{sid}:{country}",
            )
        ]
        for r in regions:
            buttons.append(
                InlineKeyboardButton(
                    text=f"{r['label']} ({r['count']})",
                    callback_data=f"tr_region:{sid}:{country}:{r['id']}",
                )
            )
        buttons.append(
            InlineKeyboardButton(
                text="⬅️ Davlatlarga qaytish",
                callback_data=f"tr_back_countries:{sid}",
            )
        )

        kb = InlineKeyboardMarkup(inline_keyboard=build_rows(buttons, 2))
        m  = await bot.send_message(chat_id, "\n".join(text), parse_mode="HTML", reply_markup=kb)
        sess["sent_msg_ids"] = [m.message_id]
        TRUCK_UI_SESSIONS[sid] = sess

    async def send_list_page(chat_id: int, sid: str, page: int):
        sess = TRUCK_UI_SESSIONS.get(sid)
        if not sess:
            await bot.send_message(chat_id, "Sessiya tugagan.", reply_markup=kb_main_menu())
            return

        await delete_session_messages(chat_id, sid)

        mode    = sess.get("mode")
        country = sess.get("country")
        region  = sess.get("region")
        title   = sess.get("title") or "Truck"

        cursors = sess.setdefault("cursors", [None])
        while len(cursors) <= page:
            cursors.append(None)

        cursor     = cursors[page]
        cursor_key = (cursor[0].isoformat(), cursor[1]) if cursor else ("none", "none")

        if mode == "list_country":
            key    = ("country", country or "-", "-", cursor_key[0], cursor_key[1])
            cached = cache_get(key)
            if cached:
                items, next_cursor, has_next = cached
            else:
                items, next_cursor, has_next = await query_country_page(country, cursor)
                cache_set(key, items, next_cursor, has_next)
        else:
            key    = ("region", country or "-", region or "-", cursor_key[0], cursor_key[1])
            cached = cache_get(key)
            if cached:
                items, next_cursor, has_next = cached
            else:
                items, next_cursor, has_next = await query_region_page(country, region, cursor)
                cache_set(key, items, next_cursor, has_next)

        if not items:
            m = await bot.send_message(chat_id, "E'lon topilmadi.", reply_markup=kb_main_menu())
            sess["sent_msg_ids"] = [m.message_id]
            TRUCK_UI_SESSIONS[sid] = sess
            return

        if next_cursor:
            if len(cursors) <= page + 1:
                cursors.append(next_cursor)
            else:
                cursors[page + 1] = next_cursor

        rows = []
        nav  = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=f"tr_page:{sid}:{page-1}"))
        if has_next:
            nav.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=f"tr_page:{sid}:{page+1}"))
        if nav:
            rows.append(nav)

        if region:
            rows.append([InlineKeyboardButton(
                text="⬅️ Hududlarga qaytish",
                callback_data=f"tr_back_regions:{sid}:{country}",
            )])
        else:
            rows.append([InlineKeyboardButton(
                text="⬅️ Davlatlarga qaytish",
                callback_data=f"tr_back_countries:{sid}",
            )])

        kb = InlineKeyboardMarkup(inline_keyboard=rows)
        m  = await bot.send_message(
            chat_id,
            build_page_text(items, title, page),
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=kb,
        )
        sess["sent_msg_ids"] = [m.message_id]
        TRUCK_UI_SESSIONS[sid] = sess

    @dp.message(F.text == "🚚 Yuk mashinalar e'lonlari")
    async def trucks_root(message: types.Message):
        if message.chat.type != "private":
            return
        if not await is_registered(message.from_user.id):
            await message.answer("Avval /start orqali ro'yxatdan o'ting.")
            return

        sid = make_session_id()
        TRUCK_UI_SESSIONS[sid] = {
            "uid": message.from_user.id,
            "created": time.time(),
            "sent_msg_ids": [],
        }
        await send_country_menu(message.chat.id, sid)

    @dp.callback_query(F.data.startswith("tr_country:"))
    async def tr_country(callback: types.CallbackQuery):
        try:
            _, sid, country = callback.data.split(":", 2)
        except Exception:
            await callback.answer("Xato", show_alert=True)
            return
        sess = TRUCK_UI_SESSIONS.get(sid)
        if not sess or callback.from_user.id != sess.get("uid"):
            await callback.answer("Sessiya tugagan", show_alert=True)
            return
        await callback.answer()
        await send_region_menu(callback.message.chat.id, sid, country)

    @dp.callback_query(F.data.startswith("tr_all_country:"))
    async def tr_all_country(callback: types.CallbackQuery):
        try:
            _, sid, country = callback.data.split(":", 2)
        except Exception:
            await callback.answer("Xato", show_alert=True)
            return
        sess = TRUCK_UI_SESSIONS.get(sid)
        if not sess or callback.from_user.id != sess.get("uid"):
            await callback.answer("Sessiya tugagan", show_alert=True)
            return

        country_counts = await get_country_counts()
        country_label  = next(
            (x["label"] for x in country_counts if x["id"] == country),
            country.title(),
        )
        sess.update({
            "mode": "list_country",
            "country": country,
            "region": None,
            "title": country_label,
            "cursors": [None],
        })
        TRUCK_UI_SESSIONS[sid] = sess
        await callback.answer()
        await send_list_page(callback.message.chat.id, sid, 0)

    @dp.callback_query(F.data.startswith("tr_region:"))
    async def tr_region(callback: types.CallbackQuery):
        try:
            _, sid, country, region = callback.data.split(":", 3)
        except Exception:
            await callback.answer("Xato", show_alert=True)
            return
        sess = TRUCK_UI_SESSIONS.get(sid)
        if not sess or callback.from_user.id != sess.get("uid"):
            await callback.answer("Sessiya tugagan", show_alert=True)
            return

        region_counts = await get_region_counts(country)
        region_label  = next(
            (x["label"] for x in region_counts if x["id"] == region),
            region.title(),
        )
        sess.update({
            "mode": "list_region",
            "country": country,
            "region": region,
            "title": region_label,
            "cursors": [None],
        })
        TRUCK_UI_SESSIONS[sid] = sess
        await callback.answer()
        await send_list_page(callback.message.chat.id, sid, 0)

    @dp.callback_query(F.data.startswith("tr_page:"))
    async def tr_page(callback: types.CallbackQuery):
        try:
            _, sid, page_str = callback.data.split(":", 2)
            page = int(page_str)
        except Exception:
            await callback.answer("Xato", show_alert=True)
            return
        sess = TRUCK_UI_SESSIONS.get(sid)
        if not sess or callback.from_user.id != sess.get("uid"):
            await callback.answer("Sessiya tugagan", show_alert=True)
            return
        await callback.answer()
        await send_list_page(callback.message.chat.id, sid, page)

    @dp.callback_query(F.data.startswith("tr_back_countries:"))
    async def tr_back_countries(callback: types.CallbackQuery):
        try:
            _, sid = callback.data.split(":", 1)
        except Exception:
            await callback.answer("Xato", show_alert=True)
            return
        sess = TRUCK_UI_SESSIONS.get(sid)
        if not sess or callback.from_user.id != sess.get("uid"):
            await callback.answer("Sessiya tugagan", show_alert=True)
            return
        await callback.answer()
        await send_country_menu(callback.message.chat.id, sid)

    @dp.callback_query(F.data.startswith("tr_back_regions:"))
    async def tr_back_regions(callback: types.CallbackQuery):
        try:
            _, sid, country = callback.data.split(":", 2)
        except Exception:
            await callback.answer("Xato", show_alert=True)
            return
        sess = TRUCK_UI_SESSIONS.get(sid)
        if not sess or callback.from_user.id != sess.get("uid"):
            await callback.answer("Sessiya tugagan", show_alert=True)
            return
        await callback.answer()
        await send_region_menu(callback.message.chat.id, sid, country)