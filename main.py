# === IMPORTLAR ===
import io
import os
import asyncio
import time
from datetime import datetime, date
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import BoundFilter
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from aiogram.utils import executor
from keep_alive import keep_alive
from database import (
    init_db,
    add_user,
    get_user_count,
    get_kino_by_code,
    get_all_codes,
    delete_kino_code,
    get_code_stat,
    increment_stat,
    get_all_user_ids,
    update_anime_code,
    get_today_users,
    add_anime,
    add_part_to_anime,
    delete_part_from_anime, get_all_admins, add_admin, remove_admin
)

# === YUKLAMALAR ===
load_dotenv()
keep_alive()

API_TOKEN = os.getenv("API_TOKEN")
CHANNELS = []
LINKS = []
MAIN_CHANNELS = []
MAIN_LINKS = []
BOT_USERNAME = os.getenv("BOT_USERNAME")

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# === KEYBOARDS ===
def edit_menu_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        "✏️ Nomi tahrirlash",
        "➕ Qism qo‘shish"
    )
    kb.add(
        "🗑️ Qismni o‘chirish",
        "⬅️ Ortga"
    )
    return kb

def admin_menu_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("➕ Admin qo‘shish")
    kb.add("➖ Admin o‘chirish")
    kb.add("👥 Adminlar ro‘yxati")
    kb.add("⬅️ Ortga")
    return kb

def admin_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("📡 Kanal boshqaruvi")
    kb.row("❌ Kodni o‘chirish", "➕ Anime qo‘shish", "✏️ Kodni tahrirlash")
    kb.row("📄 Kodlar ro‘yxati", "📈 Kod statistikasi", "📊 Statistika")
    kb.row("👥 Adminlar")
    kb.row("📢 Habar yuborish")
    kb.row("📤 Post qilish", "📘 Qo‘llanma")
    return kb

def control_keyboard():
    """Faol jarayonlarda foydalaniladigan 'Boshqarish' tugmasi"""
    return ReplyKeyboardMarkup(resize_keyboard=True).add("📡 Boshqarish")

async def send_admin_panel(message: types.Message):
    await message.answer("👮‍♂️ Admin panel:", reply_markup=admin_keyboard())

class AdminStates(StatesGroup):
    waiting_for_kino_data = State()
    waiting_for_delete_code = State()
    waiting_for_stat_code = State()
    waiting_for_broadcast_data = State()
    waiting_for_admin_id = State()
    waiting_for_remove_id = State()   # ✅ qo‘shish kerak

class AdminReplyStates(StatesGroup):
    waiting_for_reply_message = State()

class EditAnimeStates(StatesGroup):
    waiting_for_code = State()
    menu = State()
    waiting_for_new_title = State()
    waiting_for_new_part = State()
    waiting_for_part_delete = State()

class UserStates(StatesGroup):
    waiting_for_admin_message = State()

class SearchStates(StatesGroup):
    waiting_for_anime_name = State()

class PostStates(StatesGroup):
    waiting_for_image = State()
    waiting_for_title = State()
    waiting_for_link = State()
    waiting_for_button_text = State() 
    waiting_for_code = State()
    
class KanalStates(StatesGroup):
    waiting_for_channel_id = State()
    waiting_for_channel_link = State()
# === HOLATLAR ===
class AddAnimeStates(StatesGroup):
    waiting_for_code = State()
    waiting_for_title = State()
    waiting_for_poster = State()
    waiting_for_parts = State()

class IsAdmin(BoundFilter):
    key = "is_admin"

    def __init__(self, is_admin: bool):
        self.is_admin = is_admin

    async def check(self, obj):
        from database import get_all_admins
        admins = await get_all_admins()

        user_id = None
        if isinstance(obj, types.Message):
            user_id = obj.from_user.id
        elif isinstance(obj, types.CallbackQuery):
            user_id = obj.from_user.id

        if user_id is None:
            return False

        if self.is_admin:
            return user_id in admins
        else:
            return user_id not in admins
            
dp.filters_factory.bind(IsAdmin)

# === OBUNA TEKSHIRISH ===
async def get_unsubscribed_channels(user_id):
    unsubscribed = []
    for idx, channel_id in enumerate(CHANNELS):
        try:
            member = await bot.get_chat_member(channel_id, user_id)
            if member.status not in ["member", "administrator", "creator"]:
                unsubscribed.append((channel_id, LINKS[idx]))
        except Exception as e:
            print(f"❗ Obuna tekshirishda xatolik: {channel_id} -> {e}")
            unsubscribed.append((channel_id, LINKS[idx]))
    return unsubscribed


# === OBUNA BO‘LMAGANLAR MARKUP ===
async def make_unsubscribed_markup(user_id, code):
    unsubscribed = await get_unsubscribed_channels(user_id)
    markup = InlineKeyboardMarkup(row_width=1)

    for channel_id, channel_link in unsubscribed:
        try:
            chat = await bot.get_chat(channel_id)
            markup.add(
                InlineKeyboardButton(f"➕ {chat.title}", url=channel_link)
            )
        except Exception as e:
            print(f"❗ Kanal tugmasini yaratishda xatolik: {channel_id} -> {e}")

    # Tekshirish tugmasi
    markup.add(InlineKeyboardButton("✅ Tekshirish", callback_data=f"checksub:{code}"))
    return markup


# === /start HANDLER ===
@dp.message_handler(commands=['start'])
async def start_handler(message: types.Message):
    await add_user(message.from_user.id)
    args = message.get_args()

    if args and args.isdigit():
        code = args
        await increment_stat(code, "init")
        await increment_stat(code, "searched")

        unsubscribed = await get_unsubscribed_channels(message.from_user.id)
        if unsubscribed:
            markup = await make_unsubscribed_markup(message.from_user.id, code)
            await message.answer(
                "❗ Animeni olishdan oldin quyidagi kanal(lar)ga obuna bo‘ling:",
                reply_markup=markup
            )
        else:
            await send_reklama_post(message.from_user.id, code)
            await increment_stat(code, "searched")
        return

    if message.from_user.id in ADMINS:
        await send_admin_panel(message)
    else:
        kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        kb.add(KeyboardButton("🎞 Barcha animelar"), KeyboardButton("✉️ Admin bilan bog‘lanish"))
        await message.answer("✨", reply_markup=kb)


# === TEKSHIRUV CALLBACK ===
@dp.callback_query_handler(lambda c: c.data.startswith("checksub:"))
async def check_subscription_callback(call: CallbackQuery):
    code = call.data.split(":")[1]
    unsubscribed = await get_unsubscribed_channels(call.from_user.id)

    if unsubscribed:
        markup = InlineKeyboardMarkup(row_width=1)
        for channel_id, channel_link in unsubscribed:
            try:
                chat = await bot.get_chat(channel_id)
                markup.add(
                    InlineKeyboardButton(f"➕ {chat.title}", url=channel_link)
                )
            except Exception as e:
                print(f"❗ Kanal tugmasini qayta yaratishda xatolik: {channel_id} -> {e}")

        markup.add(InlineKeyboardButton("✅ Yana tekshirish", callback_data=f"checksub:{code}"))
        await call.message.edit_text("❗ Hali ham obuna bo‘lmagan kanal(lar):", reply_markup=markup)
    else:
        await call.message.delete()
        await send_reklama_post(call.from_user.id, code)
        await increment_stat(code, "searched")


# === Barcha animelar ===
@dp.message_handler(lambda m: m.text == "🎞 Barcha animelar")
async def show_all_animes(message: types.Message):
    kodlar = await get_all_codes()
    if not kodlar:
        await message.answer("⛔️ Hozircha animelar yoʻq.")
        return

    kodlar = sorted(kodlar, key=lambda x: int(x["code"]))

    # 100 tadan bo‘lib yuborish
    chunk_size = 100
    for i in range(0, len(kodlar), chunk_size):
        chunk = kodlar[i:i + chunk_size]

        text = "📄 *Barcha animelar:*\n\n"
        for row in chunk:
            text += f"`{row['code']}` – *{row['title']}*\n"

        await message.answer(text, parse_mode="Markdown")


# === Admin bilan bog‘lanish (foydalanuvchi qismi) ===
def cancel_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("❌ Bekor qilish"))
    return kb


# === Admin bilan bog‘lanish (foydalanuvchi qismi) ===
@dp.message_handler(lambda m: m.text == "✉️ Admin bilan bog‘lanish")
async def contact_admin(message: types.Message):
    await UserStates.waiting_for_admin_message.set()
    await message.answer(
        "✍️ Adminlarga yubormoqchi bo‘lgan xabaringizni yozing.\n\n❌ Bekor qilish tugmasini bosing agar ortga qaytmoqchi bo‘lsangiz.",
        reply_markup=cancel_keyboard()
    )


@dp.message_handler(state=UserStates.waiting_for_admin_message)
async def forward_to_admins(message: types.Message, state: FSMContext):
    # Bekor qilish tugmasi bosilganda
    if message.text == "❌ Bekor qilish":
        await state.finish()
        kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        kb.add(KeyboardButton("🎞 Barcha animelar"), KeyboardButton("✉️ Admin bilan bog‘lanish"))
        await message.answer("🏠 Asosiy menyuga qaytdingiz.", reply_markup=kb)
        return

    await state.finish()
    user = message.from_user

    for admin_id in ADMINS:
        try:
            keyboard = InlineKeyboardMarkup().add(
                InlineKeyboardButton("✉️ Javob yozish", callback_data=f"reply_user:{user.id}")
            )

            await bot.send_message(
                admin_id,
                f"📩 <b>Yangi xabar:</b>\n\n"
                f"<b>👤 Foydalanuvchi:</b> {user.full_name} | <code>{user.id}</code>\n"
                f"<b>💬 Xabar:</b> {message.text}",
                parse_mode="HTML",
                reply_markup=keyboard
            )
        except Exception as e:
            print(f"Adminga yuborishda xatolik: {e}")

    await message.answer(
        "✅ Xabaringiz yuborildi. Tez orada admin siz bilan bog‘lanadi.",
        reply_markup=ReplyKeyboardMarkup(resize_keyboard=True, row_width=2).add(
            KeyboardButton("🎞 Barcha animelar"), KeyboardButton("✉️ Admin bilan bog‘lanish")
        )
    )

@dp.callback_query_handler(lambda c: c.data.startswith("reply_user:"), is_admin=True)
async def start_admin_reply(callback: CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split(":")[1])
    await state.update_data(reply_user_id=user_id)
    await AdminReplyStates.waiting_for_reply_message.set()
    await callback.message.answer("✍️ Endi foydalanuvchiga yubormoqchi bo‘lgan xabaringizni yozing.")
    await callback.answer()

@dp.message_handler(state=AdminReplyStates.waiting_for_reply_message, is_admin=True)
async def send_admin_reply(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user_id = data.get("reply_user_id")

    try:
        await bot.send_message(user_id, f"✉️ Admindan javob:\n\n{message.text}")
        await message.answer("✅ Javob foydalanuvchiga yuborildi.")
    except Exception as e:
        await message.answer(f"❌ Xatolik: {e}")
    finally:
        await state.finish()
    
# === Kanal boshqaruvi menyusi ===
@dp.message_handler(lambda m: m.text == "📡 Kanal boshqaruvi", is_admin=True)
async def kanal_boshqaruvi(message: types.Message):
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("🔗 Majburiy obuna", callback_data="channel_type:sub"),
        InlineKeyboardButton("📌 Asosiy kanallar", callback_data="channel_type:main")
    )
    await message.answer("📡 Qaysi kanal turini boshqarasiz?", reply_markup=kb)


# === Kanal turi tanlanadi ===
@dp.callback_query_handler(lambda c: c.data.startswith("channel_type:"), is_admin=True)
async def select_channel_type(callback: types.CallbackQuery, state: FSMContext):
    ctype = callback.data.split(":")[1]
    await state.update_data(channel_type=ctype)

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("➕ Kanal qo‘shish", callback_data="action:add"),
        InlineKeyboardButton("📋 Kanal ro‘yxati", callback_data="action:list")
    )
    kb.add(
        InlineKeyboardButton("❌ Kanal o‘chirish", callback_data="action:delete"),
        InlineKeyboardButton("⬅️ Orqaga", callback_data="action:back")
    )

    text = "📡 Majburiy obuna kanallari menyusi:" if ctype == "sub" else "📌 Asosiy kanallar menyusi:"
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()


# === Actionlarni boshqarish ===
@dp.callback_query_handler(lambda c: c.data.startswith("action:"), is_admin=True)
async def channel_actions(callback: types.CallbackQuery, state: FSMContext):
    action = callback.data.split(":")[1]
    data = await state.get_data()
    ctype = data.get("channel_type")

    if not ctype:
        await callback.answer("❗ Avval kanal turini tanlang.")
        return

    # ➕ Kanal qo‘shish
    if action == "add":
        await KanalStates.waiting_for_channel_id.set()
        await callback.message.answer("🆔 Kanal ID yuboring (masalan: -1001234567890):")

    # 📋 Kanal ro‘yxati
    elif action == "list":
        if ctype == "sub":
            channels = list(zip(CHANNELS, LINKS))
            title = "📋 Majburiy obuna kanallari:\n\n"
        else:
            channels = list(zip(MAIN_CHANNELS, MAIN_LINKS))
            title = "📌 Asosiy kanallar:\n\n"

        if not channels:
            await callback.message.answer("📭 Hali kanal yo‘q.")
        else:
            text = title + "\n".join(
                f"{i}. 🆔 {cid}\n   🔗 {link}" for i, (cid, link) in enumerate(channels, 1)
            )
            await callback.message.answer(text)

    # ❌ Kanal o‘chirish
    elif action == "delete":
        if ctype == "sub":
            channels = list(zip(CHANNELS, LINKS))
            prefix = "del_sub"
        else:
            channels = list(zip(MAIN_CHANNELS, MAIN_LINKS))
            prefix = "del_main"

        if not channels:
            await callback.message.answer("📭 Hali kanal yo‘q.")
            return

        kb = InlineKeyboardMarkup()
        for cid, link in channels:
            kb.add(InlineKeyboardButton(f"O‘chirish: {cid}", callback_data=f"{prefix}:{cid}"))
        await callback.message.answer("❌ Qaysi kanalni o‘chirmoqchisiz?", reply_markup=kb)

    # ⬅️ Orqaga
    elif action == "back":
        await kanal_boshqaruvi(callback.message)

    await callback.answer()


# === 1. Kanal ID qabul qilish ===
@dp.message_handler(state=KanalStates.waiting_for_channel_id, is_admin=True)
async def add_channel_id(message: types.Message, state: FSMContext):
    try:
        channel_id = int(message.text.strip())
        await state.update_data(channel_id=channel_id)
        await KanalStates.waiting_for_channel_link.set()
        await message.answer("🔗 Endi kanal linkini yuboring (masalan: https://t.me/+invitehash):")
    except ValueError:
        await message.answer("❗ Faqat sonlardan iborat ID yuboring (masalan: -1001234567890).")


# === 2. Kanal linkini qabul qilish va saqlash ===
@dp.message_handler(state=KanalStates.waiting_for_channel_link, is_admin=True)
async def add_channel_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    ctype = data.get("channel_type")
    channel_id = data.get("channel_id")
    channel_link = message.text.strip()

    if not channel_link.startswith("http"):
        await message.answer("❗ To‘liq link yuboring (masalan: https://t.me/...)")
        return

    if ctype == "sub":
        if channel_id in CHANNELS:
            await message.answer("ℹ️ Bu kanal allaqachon qo‘shilgan.")
        else:
            CHANNELS.append(channel_id)
            LINKS.append(channel_link)
            await message.answer(f"✅ Kanal qo‘shildi!\n🆔 {channel_id}\n🔗 {channel_link}")
    else:
        if channel_id in MAIN_CHANNELS:
            await message.answer("ℹ️ Bu kanal allaqachon qo‘shilgan.")
        else:
            MAIN_CHANNELS.append(channel_id)
            MAIN_LINKS.append(channel_link)
            await message.answer(f"✅ Asosiy kanal qo‘shildi!\n🆔 {channel_id}\n🔗 {channel_link}")

    await state.finish()


# === Kanalni o‘chirish ===
@dp.callback_query_handler(lambda c: c.data.startswith("del_"), is_admin=True)
async def delete_channel(callback: types.CallbackQuery):
    action, cid = callback.data.split(":")
    cid = int(cid)

    if action == "del_sub":
        if cid in CHANNELS:
            idx = CHANNELS.index(cid)
            CHANNELS.pop(idx)
            LINKS.pop(idx)
            await callback.message.answer(f"❌ Kanal o‘chirildi!\n🆔 {cid}")
    elif action == "del_main":
        if cid in MAIN_CHANNELS:
            idx = MAIN_CHANNELS.index(cid)
            MAIN_CHANNELS.pop(idx)
            MAIN_LINKS.pop(idx)
            await callback.message.answer(f"❌ Asosiy kanal o‘chirildi!\n🆔 {cid}")

    await callback.answer("O‘chirildi ✅")

# === 👥 Adminlar bo‘limi ===
@dp.message_handler(lambda m: m.text == "👥 Adminlar", is_admin=True)
async def show_admin_menu(message: types.Message):
    await message.answer("👥 Adminlar bo‘limi:", reply_markup=admin_menu_keyboard())


# === ➕ Admin qo‘shish ===
@dp.message_handler(lambda m: m.text == "➕ Admin qo‘shish", is_admin=True)
async def add_admin_start(message: types.Message):
    await AdminStates.waiting_for_admin_id.set()
    await message.answer("🆔 Yangi adminning Telegram ID raqamini yuboring.", 
                         reply_markup=control_keyboard())


@dp.message_handler(state=AdminStates.waiting_for_admin_id, is_admin=True)
async def add_admin_process(message: types.Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.finish()
        await send_admin_panel(message)
        return

    text = message.text.strip()
    if not text.isdigit():
        await message.answer("❗ Faqat raqam yuboring (Telegram user ID).",
                             reply_markup=control_keyboard())
        return

    new_admin_id = int(text)
    if await is_admin_db(new_admin_id):
        await message.answer("ℹ️ Bu foydalanuvchi allaqachon admin.", 
                             reply_markup=control_keyboard())
    else:
        await add_admin(new_admin_id)   # ✅ DB ga qo‘shish
        await message.answer(
            f"✅ <code>{new_admin_id}</code> admin sifatida qo‘shildi.",
            parse_mode="HTML", reply_markup=control_keyboard()
        )
        try:
            await bot.send_message(new_admin_id, "✅ Siz botga admin sifatida qo‘shildingiz.")
        except:
            pass
    await state.finish()

# === 👥 Adminlar ro‘yxati ===
@dp.message_handler(lambda m: m.text == "👥 Adminlar ro‘yxati", is_admin=True)
async def show_admins(message: types.Message):
    current_admins = await get_all_admins()  # ✅ To‘liq ro‘yxatni DB dan olamiz
    if not current_admins:
        await message.answer("ℹ️ Hozircha adminlar ro‘yxati bo‘sh.",
                             reply_markup=admin_menu_keyboard())
        return

    admins_list = "\n".join([f"• <code>{a}</code>" for a in current_admins])
    await message.answer(
        f"👥 Hozirgi adminlar:\n\n{admins_list}",
        parse_mode="HTML",
        reply_markup=admin_menu_keyboard()
    )

# === ➖ Admin o‘chirish ===
@dp.message_handler(lambda m: m.text == "➖ Admin o‘chirish", is_admin=True)
async def remove_admin_start(message: types.Message):
    await AdminStates.waiting_for_remove_id.set()
    await message.answer("🗑 O‘chirish uchun adminning ID raqamini yuboring.", 
                         reply_markup=control_keyboard())
    
@dp.message_handler(state=AdminStates.waiting_for_remove_id, is_admin=True)
async def remove_admin_process(message: types.Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.finish()
        await send_admin_panel(message)
        return
    text = message.text.strip()
    if not text.isdigit():
        await message.answer("❗ Faqat raqam yuboring (Telegram user ID).",
                             reply_markup=control_keyboard())
        return
    remove_id = int(text)
    if not await is_admin_db(remove_id):
        await message.answer("ℹ️ Bu ID ro‘yxatda yo‘q.",
                             reply_markup=control_keyboard())
    else:
        await remove_admin(remove_id)   # ✅ DB dan o‘chirish
        await message.answer(
            f"✅ <code>{remove_id}</code> admin ro‘yxatidan o‘chirildi.",
            parse_mode="HTML", reply_markup=control_keyboard()
        )
    await state.finish()
    
# === ⬅️ Ortga tugmasi ===
@dp.message_handler(lambda m: m.text == "⬅️ Ortga", is_admin=True)
async def back_to_main(message: types.Message):
    await message.answer("📋 Boshqaruv paneli:", reply_markup=admin_keyboard())

# === Kod statistikasi ===
@dp.message_handler(lambda m: m.text == "📈 Kod statistikasi" and m.from_user.id in ADMINS)
async def ask_stat_code(message: types.Message):
    await AdminStates.waiting_for_stat_code.set()
    await message.answer("📥 Kod raqamini yuboring:", reply_markup=control_keyboard())


@dp.message_handler(state=AdminStates.waiting_for_stat_code)
async def show_code_stat(message: types.Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.finish()
        await send_admin_panel(message)
        return

    code = message.text.strip()
    if not code:
        await message.answer("❗ Kod yuboring.", reply_markup=control_keyboard())
        return

    stat = await get_code_stat(code)
    await state.finish()  # faqat oxirida tugatamiz

    if not stat:
        await message.answer("❗ Bunday kod statistikasi topilmadi.", reply_markup=control_keyboard())
        await send_admin_panel(message)
        return

    await message.answer(
        f"📊 <b>{code} statistikasi:</b>\n"
        f"🔍 Qidirilgan: <b>{stat['searched']}</b>\n",
        parse_mode="HTML"
    )

    # Statistikani ko‘rsatib bo‘lgach, admin panel qaytadi
    await send_admin_panel(message)

# === Kodni tahrirlash (ANIME) ===
@dp.message_handler(lambda m: m.text == "✏️ Kodni tahrirlash", is_admin=True)
async def edit_anime_start(message: types.Message):
    await EditAnimeStates.waiting_for_code.set()
    await message.answer(
        "📝 Qaysi anime KODini tahrirlamoqchisiz?",
        reply_markup=control_keyboard()
    )

@dp.message_handler(state=EditAnimeStates.waiting_for_code, is_admin=True)
async def edit_anime_code(message: types.Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.finish()
        await send_admin_panel(message)
        return
    code = message.text.strip()
    anime = await get_kino_by_code(code)
    if not anime:
        await message.answer("❌ Bunday kod topilmadi.", reply_markup=control_keyboard())
        return

    await state.update_data(code=code)
    await EditAnimeStates.menu.set()
    await message.answer(
        f"🔎 Kod: {code}\n📌 Nomi: {anime['title']}\n\nTahrirlash turini tanlang:",
        reply_markup=edit_menu_keyboard()
    )

# === 1️⃣ Nomi tahrirlash ===
@dp.message_handler(lambda m: m.text.startswith("1️⃣"), state=EditAnimeStates.menu, is_admin=True)
async def edit_title_start(message: types.Message, state: FSMContext):
    await EditAnimeStates.waiting_for_new_title.set()
    await message.answer("📝 Yangi nomni kiriting:")

@dp.message_handler(state=EditAnimeStates.waiting_for_new_title, is_admin=True)
async def edit_title_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    await update_anime_title(data["code"], message.text.strip())
    await message.answer("✅ Nomi yangilandi.", reply_markup=admin_keyboard())
    await state.finish()

# === 2️⃣ Qism qo‘shish ===
@dp.message_handler(lambda m: m.text.startswith("2️⃣"), state=EditAnimeStates.menu, is_admin=True)
async def add_part_start(message: types.Message, state: FSMContext):
    await EditAnimeStates.waiting_for_new_part.set()
    await message.answer("🎞 Yangi qismni (video/document) yuboring:")

@dp.message_handler(content_types=["video","document"],
                    state=EditAnimeStates.waiting_for_new_part,
                    is_admin=True)
async def add_part_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    file_id = message.video.file_id if message.video else message.document.file_id
    await add_part_to_anime(data["code"], file_id)
    await message.answer("✅ Qism qo‘shildi.", reply_markup=admin_keyboard())
    await state.finish()

# === 3️⃣ Qismni o‘chirish ===
@dp.message_handler(lambda m: m.text.startswith("3️⃣"), state=EditAnimeStates.menu, is_admin=True)
async def delete_part_start(message: types.Message, state: FSMContext):
    await EditAnimeStates.waiting_for_part_delete.set()
    await message.answer("❌ O‘chirmoqchi bo‘lgan qism raqamini kiriting:")

@dp.message_handler(state=EditAnimeStates.waiting_for_part_delete, is_admin=True)
async def delete_part_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    part_number = int(message.text.strip())
    await delete_part_from_anime(data["code"], part_number)
    await message.answer("✅ Qism o‘chirildi.", reply_markup=admin_keyboard())
    await state.finish()

# === 4️⃣ Ortga ===
@dp.message_handler(lambda m: m.text.startswith("4️⃣"), state=EditAnimeStates.menu, is_admin=True)
async def go_back(message: types.Message, state: FSMContext):
    await state.finish()
    await send_admin_panel(message)

# === ❌ Kodni o‘chirish ===
@dp.message_handler(lambda m: m.text == "❌ Kodni o‘chirish", is_admin=True)
async def ask_delete_code(message: types.Message):
    await AdminStates.waiting_for_delete_code.set()
    await message.answer("🗑 Qaysi kodni o‘chirmoqchisiz? Kodni yuboring.",
                         reply_markup=control_keyboard())

@dp.message_handler(state=AdminStates.waiting_for_delete_code, is_admin=True)
async def delete_code_handler(message: types.Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.finish()
        await send_admin_panel(message)
        return
    await state.finish()
    code = message.text.strip()
    if not code.isdigit():
        await message.answer("❗ Noto‘g‘ri format. Kod raqamini yuboring.",
                             reply_markup=control_keyboard())
        return
    deleted = await delete_kino_code(code)
    if deleted:
        await message.answer(f"✅ Kod {code} o‘chirildi.", reply_markup=admin_keyboard())
    else:
        await message.answer("❌ Kod topilmadi yoki o‘chirib bo‘lmadi.",
                             reply_markup=admin_keyboard())

# === ➕ Anime qo‘shish ===
@dp.message_handler(lambda m: m.text == "➕ Anime qo‘shish")
async def start_add_anime(message: types.Message, state: FSMContext):
    # ✅ adminlikni bazadan tekshirish
    if message.from_user.id not in await get_admin_ids():
        return
    await message.answer("📝 Kodni kiriting:")
    await AddAnimeStates.waiting_for_code.set()

@dp.message_handler(state=AddAnimeStates.waiting_for_code)
async def anime_code_handler(message: types.Message, state: FSMContext):
    await state.update_data(code=message.text.strip())
    await message.answer("📝 Anime nomini kiriting:")
    await AddAnimeStates.waiting_for_title.set()

@dp.message_handler(state=AddAnimeStates.waiting_for_title)
async def anime_title_handler(message: types.Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await message.answer("📸 Reklama postini yuboring (rasm/video/file, caption bilan bo‘lishi mumkin):")
    await AddAnimeStates.waiting_for_poster.set()

@dp.message_handler(content_types=["photo", "video", "document"], state=AddAnimeStates.waiting_for_poster)
async def anime_poster_handler(message: types.Message, state: FSMContext):
    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.video:
        file_id = message.video.file_id
    elif message.document:
        file_id = message.document.file_id

    caption = message.caption or ""
    await state.update_data(poster_file_id=file_id, caption=caption, parts_file_ids=[])

    await message.answer("📥 Endi qismlarni yuboring (video/file). Oxirida /done yuboring.")
    await AddAnimeStates.waiting_for_parts.set()

@dp.message_handler(content_types=["video", "document"], state=AddAnimeStates.waiting_for_parts)
async def anime_parts_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    parts = data.get("parts_file_ids", [])

    file_id = message.video.file_id if message.video else message.document.file_id
    parts.append(file_id)

    await state.update_data(parts_file_ids=parts)
    await message.answer(f"✅ Qism qo‘shildi. Hozircha {len(parts)} ta qism saqlandi.")

@dp.message_handler(lambda m: m.text.lower() == "/done", state=AddAnimeStates.waiting_for_parts)
async def anime_done_handler(message: types.Message, state: FSMContext):
    # ✅ adminlikni bazadan tekshirish
    if message.from_user.id not in await get_admin_ids():
        return

    data = await state.get_data()
    await add_anime(
        data["code"],
        data["title"],
        data["poster_file_id"],
        data["parts_file_ids"],
        data["caption"]
    )
    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(
        KeyboardButton("🏠 Bosh menyu")
    )
    await message.answer(
        f"✅ Anime saqlandi!\n\n"
        f"📌 Kod: <b>{data['code']}</b>\n"
        f"📖 Nomi: <b>{data['title']}</b>\n"
        f"📸 Reklama post caption: {data['caption']}\n"
        f"🎞 Qismlar soni: {len(data['parts_file_ids'])}",
        reply_markup=kb
    )
    await state.finish()

# === ➤ Post qilish (Adminlar bazadan) ===
@dp.message_handler(lambda m: m.text == "📤 Post qilish")
async def start_post_process(message: types.Message):
    # ✅ Faqat bazadagi adminlarga ruxsat
    if message.from_user.id not in await get_admin_ids():
        return
    await PostStates.waiting_for_code.set()
    await message.answer(
        "🔢 Qaysi anime KODini kanalga yubormoqchisiz?\nMasalan: `147`",
        reply_markup=control_keyboard()
    )

@dp.message_handler(state=PostStates.waiting_for_code)
async def send_post_by_code(message: types.Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.finish()
        await send_admin_panel(message)
        return

    code = message.text.strip()
    if not code.isdigit():
        await message.answer("❌ Kod faqat raqamlardan iborat bo‘lishi kerak.", reply_markup=control_keyboard())
        return

    # ✅ Bazadan anime ma'lumotini olish
    kino = await get_kino_by_code(code)
    if not kino:
        await message.answer("❌ Bunday kod topilmadi.", reply_markup=control_keyboard())
        return

    # 🔘 Yuklab olish tugmasi
    download_btn = InlineKeyboardMarkup().add(
        InlineKeyboardButton(
            "✨ Yuklab olish ✨",
            url=f"https://t.me/{BOT_USERNAME}?start={code}"
        )
    )
    successful, failed = 0, 0
    for ch in MAIN_CHANNELS:
        try:
            if kino["poster_file_id"]:
                caption = kino["caption"] or ""
                await bot.send_photo(ch, kino["poster_file_id"], caption=caption, reply_markup=download_btn)
            successful += 1
        except Exception as e:
            print(f"Xato: {e}")
            failed += 1
    await message.answer(
        f"✅ Post yuborildi.\n\n✅ Muvaffaqiyatli: {successful}\n❌ Xatolik: {failed}",
        reply_markup=admin_keyboard()
    )
    await state.finish()

# === Kodlar ro'yxati ===
@dp.message_handler(lambda m: m.text == "📄 Kodlar ro‘yxati")
async def show_all_animes(message: types.Message):
    kodlar = await get_all_codes()
    if not kodlar:
        await message.answer("Ba'zada hech qanday kodlar yo'q!")
        return
    kodlar = sorted(kodlar, key=lambda x: int(x["code"]))
    chunk_size = 100
    for i in range(0, len(kodlar), chunk_size):
        chunk = kodlar[i:i + chunk_size]
        text = "📄 *Barcha animelar:*\n\n"
        for row in chunk:
            text += f"`{row['code']}` – *{row['title']}*\n"
        await message.answer(text, parse_mode="Markdown")


# === Statistika ===
@dp.message_handler(lambda m: m.text == "📊 Statistika")
async def stats(message: types.Message):
    from database import db_pool
    async with db_pool.acquire() as conn:
        start = time.perf_counter()
        await conn.fetch("SELECT 1;")
        ping = (time.perf_counter() - start) * 1000
    kodlar = await get_all_codes()
    foydalanuvchilar = await get_user_count()
    today_users = await get_today_users()
    text = (
        f"💡 O'rtacha yuklanish: {ping:.2f} ms\n\n"
        f"👥 Foydalanuvchilar: {foydalanuvchilar} ta\n\n"
        f"📂 Barcha yuklangan animelar: {len(kodlar)} ta\n\n"
        f"📅 Bugun qo'shilgan foydalanuvchilar: {today_users} ta"
    )
    await message.answer(text, reply_markup=admin_keyboard())


# === Orqaga tugmasi ===
@dp.message_handler(lambda m: m.text == "⬅️ Orqaga", is_admin=True)
async def back_to_admin_menu(message: types.Message):
    await send_admin_panel(message)

# === Qo'llanma ===
@dp.message_handler(lambda m: m.text == "📘 Qo‘llanma")
async def qollanma(message: types.Message):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("📥 1. Anime qo‘shish", callback_data="help_add"),
        InlineKeyboardButton("📡 2. Kanal yaratish", callback_data="help_channel"),
        InlineKeyboardButton("🆔 3. Reklama ID olish", callback_data="help_id"),
        InlineKeyboardButton("🔁 4. Kod ishlashi", callback_data="help_code"),
        InlineKeyboardButton("❓ 5. Savol-javob", callback_data="help_faq")
    )
    await message.answer("📘 Qanday yordam kerak?", reply_markup=kb)


# === Qo'llanma sahifalari ===
HELP_TEXTS = {
    "help_add": ("📥 *Anime qo‘shish*\n\n`KOD @kanal REKLAMA_ID POST_SONI ANIME_NOMI`\n\nMisol: `91 @MyKino 4 12 Naruto`\n\n• *Kod* – foydalanuvchi yozadigan raqam\n• *@kanal* – server kanal username\n• *REKLAMA_ID* – post ID raqami (raqam)\n• *POST_SONI* – nechta qism borligi\n• *ANIME_NOMI* – ko‘rsatiladigan sarlavha\n\n📩 Endi formatda xabar yuboring:"),
    "help_channel": ("📡 *Kanal yaratish*\n\n1. 2 ta kanal yarating:\n   • *Server kanal* – post saqlanadi\n   • *Reklama kanal* – bot ulashadi\n\n2. Har ikkasiga botni admin qiling\n\n3. Kanalni public (@username) qiling"),
    "help_id": ("🆔 *Reklama ID olish*\n\n1. Server kanalga post joylang\n\n2. Post ustiga bosing → *Share* → *Copy link*\n\n3. Link oxiridagi sonni oling\n\nMisol: `t.me/MyKino/4` → ID = `4`"),
    "help_code": ("🔁 *Kod ishlashi*\n\n1. Foydalanuvchi kod yozadi (masalan: `91`)\n\n2. Obuna tekshiriladi → reklama post yuboriladi\n\n3. Tugmalar orqali qismlarni ochadi"),
    "help_faq": ("❓ *Tez-tez so‘raladigan savollar*\n\n• *Kodni qanday ulashaman?*\n  `https://t.me/{BOT_USERNAME}?start=91`\n\n• *Har safar yangi kanal kerakmi?*\n  – Yo‘q, bitta server kanal yetarli\n\n• *Kodni tahrirlash/o‘chirish mumkinmi?*\n  – Ha, admin menyuda ✏️ / ❌ tugmalari bor")
}

@dp.callback_query_handler(lambda c: c.data.startswith("help_"))
async def show_help_page(callback: types.CallbackQuery):
    key = callback.data
    text = HELP_TEXTS.get(key, "❌ Ma'lumot topilmadi.")
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("⬅️ Ortga", callback_data="back_help"))
    try:
        await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    except:
        await callback.message.answer(text, parse_mode="Markdown", reply_markup=kb)
        await callback.message.delete()
    finally:
        await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "back_help")
async def back_to_qollanma(callback: types.CallbackQuery):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("📥 1. Anime qo‘shish", callback_data="help_add"),
        InlineKeyboardButton("📡 2. Kanal yaratish", callback_data="help_channel"),
        InlineKeyboardButton("🆔 3. Reklama ID olish", callback_data="help_id"),
        InlineKeyboardButton("🔁 4. Kod ishlashi", callback_data="help_code"),
        InlineKeyboardButton("❓ 5. Savol-javob", callback_data="help_faq")
    )
    try:
        await callback.message.edit_text("📘 Qanday yordam kerak?", reply_markup=kb)
    except:
        await callback.message.answer("📘 Qanday yordam kerak?", reply_markup=kb)
        await callback.message.delete()
    finally:
        await callback.answer()


# === Habar yuborish ===
@dp.message_handler(lambda m: m.text == "📢 Habar yuborish", is_admin=True)
async def ask_broadcast_info(message: types.Message):
    await AdminStates.waiting_for_broadcast_data.set()
    await message.answer(
        "📨 Habar yuborish uchun format:\n`@kanal xabar_id`",
        parse_mode="Markdown",
        reply_markup=control_keyboard()
    )


@dp.message_handler(is_admin=True, state=AdminStates.waiting_for_broadcast_data)
async def send_forward_only(message: types.Message, state: FSMContext):
    if message.text == "📡 Boshqarish":
        await state.finish()
        await send_admin_panel(message)
        return

    parts = message.text.strip().split()
    if len(parts) != 2:
        await message.answer("❗ Format noto‘g‘ri. Masalan: `@kanalim 123`", reply_markup=control_keyboard())
        return

    channel_username, msg_id = parts
    if not msg_id.isdigit():
        await message.answer("❗ Xabar ID raqam bo‘lishi kerak.", reply_markup=control_keyboard())
        return

    msg_id = int(msg_id)
    users = await get_all_user_ids()
    success = 0
    fail = 0

    for index, user_id in enumerate(users, start=1):
        try:
            await bot.forward_message(user_id, channel_username, msg_id)
            success += 1
        except Exception as e:
            print(f"Xatolik {user_id} uchun: {e}")
            fail += 1

        # Har 20 ta yuborilganda 1 sekund kutish
        if index % 20 == 0:
            await asyncio.sleep(1.5)

    # Shu yerda state tugatiladi
    await state.finish()

    await message.answer(
        f"✅ Yuborildi: {success} ta\n❌ Xatolik: {fail} ta",
        reply_markup=admin_keyboard()
    )

# === Kodni qidirish (raqam) ===
@dp.message_handler(lambda message: message.text.isdigit())
async def handle_code_message(message: types.Message):
    code = message.text
    unsubscribed = await get_unsubscribed_channels(message.from_user.id)
    if unsubscribed:
        markup = await make_unsubscribed_markup(message.from_user.id, code)
        await message.answer(
            "❗ Anime olishdan oldin quyidagi kanal(lar)ga obuna bo‘ling:",
            reply_markup=markup
        )
        return

    await increment_stat(code, "init")
    await increment_stat(code, "searched")
    await send_reklama_post(message.from_user.id, code)
    await increment_stat(code, "viewed")


# === Reklama post yuborish ===
async def send_reklama_post(user_id, code):
    data = await get_kino_by_code(code)
    if not data:
        await bot.send_message(user_id, "❌ Kod topilmadi.")
        return

    poster_file_id = data["poster_file_id"]
    caption = data.get("caption", "")
    
    # Inline tugma
    keyboard = InlineKeyboardMarkup().add(
        InlineKeyboardButton("✨Tomosha qilish✨", callback_data=f"download:{code}")
    )

    try:
        if poster_file_id:
            await bot.send_photo(user_id, poster_file_id, caption=caption, reply_markup=keyboard)
        else:
            # Agar poster yo'q bo'lsa, oddiy matn
            await bot.send_message(user_id, caption or "Anime tayyor!", reply_markup=keyboard)
    except:
        await bot.send_message(user_id, "❌ Reklama postni yuborib bo‘lmadi.")


# === Yuklab olish tugmasi bosilganda ===
@dp.callback_query_handler(lambda c: c.data.startswith("download:"))
async def download_all(callback: types.CallbackQuery):
    code = callback.data.split(":")[1]
    result = await get_kino_by_code(code)
    if not result:
        await callback.message.answer("❌ Kod topilmadi.")
        return

    parts_file_ids = result.get("parts_file_ids", [])
    if not parts_file_ids:
        await callback.message.answer("❌ Hech qanday qism topilmadi.")
        return

    await callback.answer("⏳ Yuklanmoqda, biroz kuting...")

    # Hamma qismlarni ketma-ket yuborish
    for file_id in parts_file_ids:
        try:
            if file_id.startswith("BQAD") or file_id.startswith("AgAD"):  # photo/video/document check (file_id formatiga qarab)
                await bot.send_document(callback.from_user.id, file_id)
            else:
                await bot.send_document(callback.from_user.id, file_id)
            await asyncio.sleep(0.1)  # flood control uchun sekin yuborish
        except:
            pass

# === START ===
async def on_startup(dp):
    await init_db()
    print("✅ PostgreSQL bazaga ulandi!")

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=False, on_startup=on_startup)
