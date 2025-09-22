import asyncpg
import os
import json
from dotenv import load_dotenv
from datetime import date

load_dotenv()

db_pool = None

# === Databasega ulanish ===
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        dsn=os.getenv("DATABASE_URL"),
        ssl="require",
        statement_cache_size=0
    )

    async with db_pool.acquire() as conn:
        # === Foydalanuvchilar ===
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # === Anime kodlari ===
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS kino_codes (
                code TEXT PRIMARY KEY,
                title TEXT,
                channel TEXT,
                message_id INTEGER,
                post_count INTEGER,
                poster_file_id TEXT,
                caption TEXT,
                parts_file_ids TEXT
            );
        """)

        # === Statistika ===
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                code TEXT PRIMARY KEY,
                searched INTEGER DEFAULT 0,
                viewed INTEGER DEFAULT 0
            );
        """)

        # === Adminlar ===
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY
            );
        """)

        # Dastlabki adminlar (o‘zingning ID’laringni yoz)
        default_admins = [6486825926]
        for admin_id in default_admins:
            await conn.execute(
                "INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
                admin_id
            )


# === Foydalanuvchilar bilan ishlash ===
async def add_user(user_id):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
        )

async def get_conn():
    global db_pool
    if db_pool is None:
        await init_db()
    return db_pool
    
async def get_user_count():
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) FROM users")
        return row[0]

async def get_today_users():
    async with db_pool.acquire() as conn:
        today = date.today()
        row = await conn.fetchrow("""
            SELECT COUNT(*) FROM users WHERE DATE(created_at) = $1
        """, today)
        return row[0] if row else 0


# === Anime qo‘shish va kodlar bilan ishlash ===
async def add_anime(code, title, poster_file_id, parts_file_ids, caption=""):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO kino_codes (code, title, poster_file_id, caption, parts_file_ids, post_count)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (code) DO UPDATE SET
                title = EXCLUDED.title,
                poster_file_id = EXCLUDED.poster_file_id,
                caption = EXCLUDED.caption,
                parts_file_ids = EXCLUDED.parts_file_ids;
        """, code, title, poster_file_id, caption, json.dumps(parts_file_ids), len(parts_file_ids))
        await conn.execute("""
            INSERT INTO stats (code) VALUES ($1)
            ON CONFLICT DO NOTHING
        """, code)


async def get_kino_by_code(code):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT code, title, poster_file_id, caption, parts_file_ids, post_count, channel, message_id
            FROM kino_codes
            WHERE code = $1
        """, code)
        if row:
            data = dict(row)
            data["parts_file_ids"] = json.loads(data["parts_file_ids"]) if data.get("parts_file_ids") else []
            return data
        return None


async def get_all_codes():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT code, title, poster_file_id, caption, parts_file_ids, post_count, channel, message_id
            FROM kino_codes
        """)
        result = []
        for row in rows:
            item = dict(row)
            item["parts_file_ids"] = json.loads(item["parts_file_ids"]) if item.get("parts_file_ids") else []
            result.append(item)
        return result


async def delete_kino_code(code):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM stats WHERE code = $1", code)
        result = await conn.execute("DELETE FROM kino_codes WHERE code = $1", code)
        return result.endswith("1")


# === Statistika bilan ishlash ===
async def increment_stat(code, field):
    if field not in ("searched", "viewed", "init"):
        return
    async with db_pool.acquire() as conn:
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
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT searched, viewed FROM stats WHERE code = $1", code)


# === Kodni yangilash ===
async def update_anime_code(old_code, new_code, new_title):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE kino_codes SET code = $1, title = $2 WHERE code = $3
        """, new_code, new_title, old_code)


# === Adminlar bilan ishlash ===
async def get_all_admins():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM admins")
        return {row["user_id"] for row in rows}


async def add_admin(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )


async def remove_admin(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM admins WHERE user_id = $1", user_id)


# === Barcha foydalanuvchilarni olish ===
async def get_all_user_ids():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        return [row["user_id"] for row in rows]

# === Qism qo‘shish ===
async def add_part_to_anime(code: str, file_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT parts_file_ids FROM kino_codes WHERE code=$1", code)
        parts = json.loads(row["parts_file_ids"]) if row["parts_file_ids"] else []
        parts.append(file_id)
        await conn.execute(
            "UPDATE kino_codes SET parts_file_ids=$1 WHERE code=$2",
            json.dumps(parts),
            code
        )

async def delete_part_from_anime(code: str, index: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT parts_file_ids FROM kino_codes WHERE code=$1", code)
        parts = json.loads(row["parts_file_ids"]) if row["parts_file_ids"] else []
        if index < 0 or index >= len(parts):
            return False
        parts.pop(index)
        await conn.execute(
            "UPDATE kino_codes SET parts_file_ids=$1 WHERE code=$2",
            json.dumps(parts),
            code
        )
        return True
