# filters.py
from aiogram.dispatcher.filters import BoundFilter
from aiogram import types
from database import get_all_admins   # sening bazadan olish funksiyang

class IsAdmin(BoundFilter):
    key = "is_admin"

    def __init__(self, is_admin: bool = True):
        self.is_admin = is_admin

    async def check(self, message: types.Message) -> bool:
        admins = await get_all_admins()
        return message.from_user.id in admins
