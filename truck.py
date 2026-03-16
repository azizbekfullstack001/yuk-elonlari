import asyncio
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


def register_truck_module(dp, bot, deps: dict):
    get_db_pool = deps["get_db_pool"]
    is_registered = deps["is_registered"]
    kb_main_menu = deps["kb_main_menu"]
    escape_html = deps["escape_html"]
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
                """
                select count(*)
                from truck_ads
                where active = true
                """
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
                ad_id,
                text_short as text,
                text_norm,
                vehicle,
                phone,
                weight,
                country,
                country_label,
                region,
                region_label,
                link,
                active,
                created_at,
                updated_at,
                expires_at
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
                ad_id,
                text_short as text,
                text_norm,
                vehicle,
                phone,
                weight,
                country,
                country_label,
                region,
                region_label,
                link,
                active,
                created_at,
                updated_at,
                expires_at
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
        region_label = ad.get("region_label") or "—"
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
            region_label = ad.get("region_label") or ad.get("country_label") or "—"
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

        total = await get_total_count()
        countries = await get_country_counts()

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

        regions = await get_region_counts(country)
        country_counts = await get_country_counts()
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
                items, next_cursor, has_next = await query_country_page(country, cursor)
                cache_set(key, items, next_cursor, has_next)
        else:
            key = ("region", country or "-", region or "-", cursor_key[0], cursor_key[1])
            cached = cache_get(key)
            if cached:
                items, next_cursor, has_next = cached
            else:
                items, next_cursor, has_next = await query_region_page(country, region, cursor)
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
        if not await is_registered(message.from_user.id):
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

        country_counts = await get_country_counts()
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

        region_counts = await get_region_counts(country)
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