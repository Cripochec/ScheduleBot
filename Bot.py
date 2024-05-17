from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
import parsing


from config import TOKEN

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)


@dp.message_handler(commands="start")
async def cmd_start(msg: types.Message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ["today", "tomorrow", "all_schedule"]
    keyboard.add(*buttons)
    await msg.answer("Расписание ", reply_markup=keyboard)
    # await msg.answer(msg.from_user.id)
    await msg.delete()


@dp.message_handler(lambda message: message.text == "today")
async def schedule_for_today(msg: types.Message):
    await msg.answer(parsing.get_schedule_today())
    await msg.delete()


@dp.message_handler(lambda message: message.text == "tomorrow")
async def schedule_for_tomorrow(msg: types.Message):
    await msg.answer(parsing.get_schedule_tomorrow())
    await msg.delete()


@dp.message_handler(lambda message: message.text == "all_schedule")
async def schedule_for_week(msg: types.Message):
    await msg.answer(parsing.get_schedule_week())
    await msg.delete()


@dp.message_handler(commands=['help'])
async def process_help_command(msg: types.Message):
    await msg.reply("Этот бот выводит расисание на сегодня и завтра!", reply_markup=types.ReplyKeyboardRemove())
    await msg.delete()

if __name__ == '__main__':
    executor.start_polling(dp)
