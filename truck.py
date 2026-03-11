import asyncio
import time
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

from aiogram import F, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from firebase_admin import firestore


PAGE_SIZE = 5
TRUCK_CACHE_TTL = 300

TRUCK_UI_SESSIONS: Dict[str, Dict[str, Any]] = {}
TRUCK_CACHE: Dict[Tuple[str, str, str, str, str], Tuple[float, List[dict], Optional[Tuple[datetime, str]], bool]] = {}


def register_truck_module(dp, bot, deps: dict):
    db = deps["db"]
    is_registered = deps["is_registered"]
    kb_main_menu = deps["kb_main_menu"]
    escape_html = deps["escape_html"]
    make_session_id = deps["make_session_id"]

    def truck_summary_ref():
        return db.collection("truck_stats").document("summary")

    def truck_country_stats_col():
        return db.collection("truck_stats_countries")

    def truck_region_stats_col(country: str):
        return db.collection("truck_stats_regions").document(country).collection("regions")

    def truck_country_index_col(country: str):
        return db.collection("truck_country_index").document(country).collection("ads")

    def truck_region_index_col(country: str, region: str):
        return (
            db.collection("truck_region_index")
            .document(country)
            .collection("regions")
            .document(region)
            .collection("ads")
        )

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

    def get_total_count() -> int:
        snap = truck_summary_ref().get()
        if not snap.exists:
            return 0
        return max(0, int((snap.to_dict() or {}).get("total", 0) or 0))

    def get_country_counts() -> List[dict]:
        docs = list(
            truck_country_stats_col()
            .order_by("count", direction=firestore.Query.DESCENDING)
            .limit(30)
            .stream()
        )
        out = []
        for d in docs:
            x = d.to_dict() or {}
            cnt = int(x.get("count", 0) or 0)
            if cnt <= 0:
                continue
            out.append({
                "id": d.id,
                "label": x.get("label") or d.id.title(),
                "count": cnt,
            })
        return out

    def get_region_counts(country: str) -> List[dict]:
        docs = list(
            truck_region_stats_col(country)
            .order_by("count", direction=firestore.Query.DESCENDING)
            .limit(50)
            .stream()
        )
        out = []
        for d in docs:
            x = d.to_dict() or {}
            cnt = int(x.get("count", 0) or 0)
            if cnt <= 0:
                continue
            out.append({
                "id": d.id,
                "label": x.get("label") or d.id.title(),
                "count": cnt,
            })
        return out

    def query_country_page(country: str, cursor: Optional[Tuple[datetime, str]]):
        col = truck_country_index_col(country)
        q = (
            col.where("active", "==", True)
            .order_by("createdAt", direction=firestore.Query.DESCENDING)
            .order_by("__name__", direction=firestore.Query.DESCENDING)
        )

        if cursor:
            q = q.start_after([cursor[0], col.document(cursor[1])])

        docs = list(q.limit(PAGE_SIZE + 1).stream())
        has_next = len(docs) > PAGE_SIZE
        docs = docs[:PAGE_SIZE]

        items, next_cursor = [], None
        for d in docs:
            x = d.to_dict() or {}
            x["_id"] = d.id
            items.append(x)

        if docs:
            last = docs[-1]
            ld = last.to_dict() or {}
            created_at = ld.get("createdAt")
            if isinstance(created_at, datetime):
                next_cursor = (created_at, last.id)

        return items, next_cursor, has_next

    def query_region_page(country: str, region: str, cursor: Optional[Tuple[datetime, str]]):
        col = truck_region_index_col(country, region)
        q = (
            col.where("active", "==", True)
            .order_by("createdAt", direction=firestore.Query.DESCENDING)
            .order_by("__name__", direction=firestore.Query.DESCENDING)
        )

        if cursor:
            q = q.start_after([cursor[0], col.document(cursor[1])])

        docs = list(q.limit(PAGE_SIZE + 1).stream())
        has_next = len(docs) > PAGE_SIZE
        docs = docs[:PAGE_SIZE]

        items, next_cursor = [], None
        for d in docs:
            x = d.to_dict() or {}
            x["_id"] = d.id
            items.append(x)

        if docs:
            last = docs[-1]
            ld = last.to_dict() or {}
            created_at = ld.get("createdAt")
            if isinstance(created_at, datetime):
                next_cursor = (created_at, last.id)

        return items, next_cursor, has_next

    def build_rows(buttons: List[InlineKeyboardButton], row_size: int = 2):
        return [buttons[i:i + row_size] for i in range(0, len(buttons), row_size)]

    def format_truck_item(ad: dict, idx: int) -> str:
        country_label = ad.get("countryLabel") or "—"
        region_label = ad.get("regionLabel") or "—"
        vehicle = ad.get("vehicle") or "Ko‘rsatilmagan"
        phone = ad.get("phone") or "Ko‘rsatilmagan"
        weight = ad.get("weight") or "—"
        text = ad.get("text") or ""
        link = ad.get("link")

        hudud = region_label if region_label and region_label != "—" else country_label
        details = f'<a href="{link}">Batafsil</a>' if link else "Batafsil yo‘q"

        return (
            f"<b>{idx}.</b> 📍 <b>Hudud:</b> {escape_html(hudud)}\n"
            f"🚚 <b>Mashina:</b> {escape_html(vehicle)}\n"
            f"⚖️ <b>Sig‘im:</b> {escape_html(weight)}\n"
            f"📞 <b>Telefon:</b> {escape_html(phone)}\n"
            f"📝 <b>Qisqa:</b> {escape_html(text)}\n"
            f"🔗 {details}"
        )

    def build_page_text(items: List[dict], title: str, page: int) -> str:
        lines = [
            "🚚 <b>Yuk mashinalar e’lonlari</b>",
            f"🔎 <b>Bo‘lim:</b> {escape_html(title)}",
            f"📄 <b>Sahifa:</b> {page + 1}",
            ""
        ]
        for i, ad in enumerate(items, start=1):
            lines.append(format_truck_item(ad, i))
            if i != len(items):
                lines.append("────────────")
        text = "\n".join(lines)

        if len(text) <= 3900:
            return text

        short = [
            "🚚 <b>Yuk mashinalar e’lonlari</b>",
            f"🔎 <b>Bo‘lim:</b> {escape_html(title)}",
            f"📄 <b>Sahifa:</b> {page + 1}",
            ""
        ]
        for i, ad in enumerate(items, start=1):
            vehicle = ad.get("vehicle") or "—"
            phone = ad.get("phone") or "—"
            region_label = ad.get("regionLabel") or ad.get("countryLabel") or "—"
            link = ad.get("link")
            details = f'<a href="{link}">Batafsil</a>' if link else "Batafsil yo‘q"
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

        total = get_total_count()
        countries = get_country_counts()

        if total <= 0 or not countries:
            m = await bot.send_message(
                chat_id,
                "🚚 Hozircha yuk mashina e’lonlari topilmadi.",
                reply_markup=kb_main_menu()
            )
            sess = TRUCK_UI_SESSIONS.get(sid, {})
            sess["sent_msg_ids"] = [m.message_id]
            TRUCK_UI_SESSIONS[sid] = sess
            return

        text = [f"🚚 <b>Jami truck e’lonlar:</b> {total}", "", "🌍 <b>Davlatni tanlang:</b>"]

        buttons = []
        for c in countries:
            buttons.append(
                InlineKeyboardButton(
                    text=f"{c['label']} ({c['count']})",
                    callback_data=f"tr_country:{sid}:{c['id']}"
                )
            )

        kb = InlineKeyboardMarkup(inline_keyboard=build_rows(buttons, 2))
        m = await bot.send_message(chat_id, "\n".join(text), parse_mode="HTML", reply_markup=kb)
        sess = TRUCK_UI_SESSIONS.get(sid, {})
        sess["sent_msg_ids"] = [m.message_id]
        TRUCK_UI_SESSIONS[sid] = sess

    async def send_region_menu(chat_id: int, sid: str, country: str):
        sess = TRUCK_UI_SESSIONS.get(sid)
        if not sess:
            return

        await delete_session_messages(chat_id, sid)

        regions = get_region_counts(country)
        country_counts = get_country_counts()
        country_label = next((x["label"] for x in country_counts if x["id"] == country), country.title())

        if not regions:
            sess["mode"] = "list_country"
            sess["country"] = country
            sess["region"] = None
            sess["title"] = country_label
            sess["cursors"] = [None]
            TRUCK_UI_SESSIONS[sid] = sess
            await send_list_page(chat_id, sid, 0)
            return

        total_in_country = sum(x["count"] for x in regions)

        text = [
            f"🌍 <b>{escape_html(country_label)}</b>",
            "",
            "📍 <b>Viloyat / hududni tanlang:</b>"
        ]

        buttons = [
            InlineKeyboardButton(
                text=f"📦 Barchasi ({total_in_country})",
                callback_data=f"tr_all_country:{sid}:{country}"
            )
        ]

        for r in regions:
            buttons.append(
                InlineKeyboardButton(
                    text=f"{r['label']} ({r['count']})",
                    callback_data=f"tr_region:{sid}:{country}:{r['id']}"
                )
            )

        buttons.append(
            InlineKeyboardButton(text="⬅️ Davlatlarga qaytish", callback_data=f"tr_back_countries:{sid}")
        )

        kb = InlineKeyboardMarkup(inline_keyboard=build_rows(buttons, 2))
        m = await bot.send_message(chat_id, "\n".join(text), parse_mode="HTML", reply_markup=kb)

        sess["sent_msg_ids"] = [m.message_id]
        TRUCK_UI_SESSIONS[sid] = sess

    async def send_list_page(chat_id: int, sid: str, page: int):
        sess = TRUCK_UI_SESSIONS.get(sid)
        if not sess:
            await bot.send_message(chat_id, "Sessiya tugagan.", reply_markup=kb_main_menu())
            return

        await delete_session_messages(chat_id, sid)

        mode = sess.get("mode")
        country = sess.get("country")
        region = sess.get("region")
        title = sess.get("title") or "Truck"

        cursors = sess.setdefault("cursors", [None])
        while len(cursors) <= page:
            cursors.append(None)

        cursor = cursors[page]
        cursor_key = ("none", "none")
        if cursor:
            cursor_key = (cursor[0].isoformat(), cursor[1])

        if mode == "list_country":
            key = ("country", country or "-", "-", cursor_key[0], cursor_key[1])
            cached = cache_get(key)
            if cached:
                items, next_cursor, has_next = cached
            else:
                items, next_cursor, has_next = query_country_page(country, cursor)
                cache_set(key, items, next_cursor, has_next)
        else:
            key = ("region", country or "-", region or "-", cursor_key[0], cursor_key[1])
            cached = cache_get(key)
            if cached:
                items, next_cursor, has_next = cached
            else:
                items, next_cursor, has_next = query_region_page(country, region, cursor)
                cache_set(key, items, next_cursor, has_next)

        if not items:
            m = await bot.send_message(chat_id, "E’lon topilmadi.", reply_markup=kb_main_menu())
            sess["sent_msg_ids"] = [m.message_id]
            TRUCK_UI_SESSIONS[sid] = sess
            return

        if next_cursor:
            if len(cursors) <= page + 1:
                cursors.append(next_cursor)
            else:
                cursors[page + 1] = next_cursor

        rows = []
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=f"tr_page:{sid}:{page-1}"))
        if has_next:
            nav.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=f"tr_page:{sid}:{page+1}"))
        if nav:
            rows.append(nav)

        if region:
            rows.append([InlineKeyboardButton(text="⬅️ Hududlarga qaytish", callback_data=f"tr_back_regions:{sid}:{country}")])
        else:
            rows.append([InlineKeyboardButton(text="⬅️ Davlatlarga qaytish", callback_data=f"tr_back_countries:{sid}")])

        kb = InlineKeyboardMarkup(inline_keyboard=rows)

        m = await bot.send_message(
            chat_id,
            build_page_text(items, title, page),
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=kb
        )
        sess["sent_msg_ids"] = [m.message_id]
        TRUCK_UI_SESSIONS[sid] = sess

    @dp.message(F.text == "🚚 Yuk mashinalar e'lonlari")
    async def trucks_root(message: types.Message):
        if message.chat.type != "private":
            return
        if not is_registered(message.from_user.id):
            await message.answer("Avval /start orqali ro‘yxatdan o‘ting.")
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

        country_counts = get_country_counts()
        country_label = next((x["label"] for x in country_counts if x["id"] == country), country.title())

        sess["mode"] = "list_country"
        sess["country"] = country
        sess["region"] = None
        sess["title"] = country_label
        sess["cursors"] = [None]
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

        region_counts = get_region_counts(country)
        region_label = next((x["label"] for x in region_counts if x["id"] == region), region.title())

        sess["mode"] = "list_region"
        sess["country"] = country
        sess["region"] = region
        sess["title"] = region_label
        sess["cursors"] = [None]
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