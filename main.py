from AggregationTime import AggregationTime
import os
import asyncio
from aiogram import Bot, Dispatcher, types, executor
from aiogram.utils import executor
import json

dp = Dispatcher(Bot(token=os.environ["tg_token"]))  # Создаем переменную бота


@dp.message_handler(commands=["start"])  # Обработка команды start
async def send_welcome(message: types.Message):
    return await message.answer(
        f"Hi {message.from_user.mention}!", reply_markup=types.ForceReply(True)
    )


@dp.message_handler()
async def echo_message(message: types.Message):  # Обработка входящих сообщений
    try:
        return await message.answer(
            json.dumps(AggregationTime().main(json.loads(message.text)))
        )
    except Exception as e:
        return await message.answer(f"Ошибка: {e}")


executor.start_polling(dp)  # Запускаем бота
