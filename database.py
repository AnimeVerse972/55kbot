import asyncpg
import os
from dotenv import load_dotenv
from datetime import date
import asyncio

load_dotenv()

db_pool = None

# === Databasega ulanish ===
async def init_db(retries: int = 5, delay: int = 2):
    """
    Ulanishni yaratadi, agar muvaffaqiyatsiz bo‘lsa qayta urinadi.
    """
    global db_pool
    for attempt in range(1, retries + 1):
        try:
            db_pool = await asyncpg.create_pool(
                dsn=os.getenv("DATABASE_URL"),
                ssl="require",
                statement_cache_size=0
            )
            print("✅ Database pool yaratildi")
            break
        except Exception as e:
            print(f"❌ Ulanishda xatolik ({attempt}/{retries}): {e}")
            if attempt == retries:
                raise
            await asyncio.sleep(delay)

    # Jadval yaratish
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS anime_codes (
                code TEXT PRIMARY KEY,
                title TEXT,
                poster_file_id TEXT,
                parts_file_ids TEXT[]
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                code TEXT PRIMARY KEY,
                searched INTEGER DEFAULT 0,
                viewed INTEGER DEFAULT 0
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY
            );
        """)

        default_admins = [6486825926]
        for admin_id in default_admins:
            await conn.execute(
                "INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
                admin_id
            )

# === Poolni tekshirish va qayta ulanish ===
async def get_conn():
    global db_pool
    if db_pool is None:
        await init_db()
    else:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("SELECT 1")  # test query
        except Exception:
            print("⚠️ Pool ishlamayapti, qayta ulanmoqda...")
            await init_db()
    return db_pool

# === Foydalanuvchilar bilan ishlash ===
async def add_user(user_id):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
        )

async def get_user_count():
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) FROM users")
        return row[0]

async def get_today_users():
    pool = await get_conn()
    async with pool.acquire() as conn:
        today = date.today()
        row = await conn.fetchrow(
            "SELECT COUNT(*) FROM users WHERE DATE(created_at) = $1", today
        )
        return row[0] if row else 0

# === Anime bilan ishlash ===
async def add_anime(code: str, title: str, poster_file_id: str, parts_file_ids: list[str]):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO anime_codes (code, title, poster_file_id, parts_file_ids)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (code) DO UPDATE SET
                title = EXCLUDED.title,
                poster_file_id = EXCLUDED.poster_file_id,
                parts_file_ids = EXCLUDED.parts_file_ids;
        """, code, title, poster_file_id, parts_file_ids)

async def get_anime(code: str):
    pool = await get_conn()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT code, title, poster_file_id, parts_file_ids
            FROM anime_codes
            WHERE code = $1
        """, code)
        return dict(row) if row else None

async def get_all_animes():
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT code, title, poster_file_id, parts_file_ids FROM anime_codes
        """)
        return [dict(r) for r in rows]

async def delete_anime(code: str):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM stats WHERE code = $1", code)
        result = await conn.execute("DELETE FROM anime_codes WHERE code = $1", code)
        return result.endswith("1")

# === Statistika bilan ishlash ===
async def increment_stat(code, field):
    if field not in ("searched", "viewed", "init"):
        return
    pool = await get_conn()
    async with pool.acquire() as conn:
        if field == "init":
            await conn.execute("""
                INSERT INTO stats (code, searched, viewed) VALUES ($1, 0, 0)
                ON CONFLICT DO NOTHING
            """, code)
        else:
            await conn.execute(f"""
                UPDATE stats SET {field} = {field} + 1 WHERE code = $1
            """, code)

async def get_code_stat(code):
    pool = await get_conn()
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT searched, viewed FROM stats WHERE code = $1", code)

# === Kodni yangilash ===
async def update_anime_code(old_code, new_code, new_title):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE anime_codes SET code = $1, title = $2 WHERE code = $3
        """, new_code, new_title, old_code)

# === Adminlar bilan ishlash ===
async def get_all_admins():
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM admins")
        return {row["user_id"] for row in rows}

async def add_admin(user_id: int):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )

async def remove_admin(user_id: int):
    pool = await get_conn()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM admins WHERE user_id = $1", user_id)

# === Barcha foydalanuvchilarni olish ===
async def get_all_user_ids():
    pool = await get_conn()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        return [row["user_id"] for row in rows]
