"""
USER StringSession yaratish.
Ishlatish:
  .\venv\Scripts\python.exe generate_session.py +998901234567
  # keyin Telegramdagi kodni kiriting
  # 2FA bo'lsa — parolni kiriting
"""
import asyncio
import os
import sys

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession

load_dotenv()

API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")


async def main():
    phone = (sys.argv[1] if len(sys.argv) > 1 else input("Telefon (+998...): ")).strip()
    if not phone:
        print("Telefon raqami kerak")
        return

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    await client.send_code_request(phone)
    code = input("Telegramdagi kodni kiriting: ").strip()

    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        password = input("2FA parol: ").strip()
        await client.sign_in(password=password)

    me = await client.get_me()
    if getattr(me, "bot", False):
        print("XATO: bu bot akkaunti. USER raqam bilan kiring.")
        await client.disconnect()
        return

    session = client.session.save()
    print("\n=== TG_SESSION ===")
    print(session)
    print("==================\n")
    print(f"OK @{getattr(me, 'username', None)} id={me.id} bot={me.bot}")

    with open("_new_session.txt", "w", encoding="utf-8") as f:
        f.write(session)
    print("Saqlandi: _new_session.txt")
    print("Shu qiymatni .env dagi TG_SESSION ga yozing, keyin botni qayta ishga tushiring.")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
