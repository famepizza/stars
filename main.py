import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, Any, List

import aiosqlite
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiohttp import web

# ---------- НАСТРОЙКИ ----------
BOT_TOKEN = "8733133714:AAFG5g1MFA_q6gAvSFtj2vbhOosxcfrJ2SI"
ADMIN_IDS = [8356674232]  # Ваш ID
ADMIN_USERNAME = "@Gabarovv"  # Ваш юзернейм

STAR_RATE_RUB = Decimal("1.4")
MIN_STARS = 50

MANUAL_PAYMENT_PHONE = "+79026674703"
MANUAL_PAYMENT_NAME = "кирилл"
MANUAL_PAYMENT_BANK = "Озон Банк"

CRYPTO_WALLET_ADDRESS = "UQDL9Eo8eKPkBpsNY1KoUjYXu23dGmsfDTJnFG_1GgqcetDf"
CRYPTO_WALLET_NETWORK = "TON"

DATABASE_PATH = "bot.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# -------------------- База данных --------------------
async def init_db():
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                stars_balance INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                package_stars INTEGER NOT NULL,
                amount_rub INTEGER NOT NULL,
                payment_method TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                admin_message_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
            """
        )
        await db.commit()

async def get_all_users() -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id FROM users") as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

async def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

async def create_user(user_id: int, username: str = None, full_name: str = None):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (user_id, username, full_name) VALUES (?, ?, ?)",
            (user_id, username, full_name),
        )
        await db.commit()

async def update_user_balance(user_id: int, stars_add: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE users SET stars_balance = stars_balance + ? WHERE user_id = ?",
            (stars_add, user_id),
        )
        await db.commit()

async def create_order(
    user_id: int,
    username: str,
    package_stars: int,
    amount_rub: int,
    payment_method: str,
) -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            """
            INSERT INTO orders (user_id, username, package_stars, amount_rub, payment_method, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, username, package_stars, amount_rub, payment_method, "pending"),
        )
        await db.commit()
        return cursor.lastrowid

async def update_order_status(order_id: int, status: str, admin_message_id: int = None):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        if admin_message_id:
            await db.execute(
                "UPDATE orders SET status = ?, admin_message_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, admin_message_id, order_id),
            )
        else:
            await db.execute(
                "UPDATE orders SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, order_id),
            )
        await db.commit()

async def get_order(order_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

async def get_pending_orders() -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at DESC"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

async def update_order_admin_message(order_id: int, message_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE orders SET admin_message_id = ? WHERE id = ?",
            (message_id, order_id),
        )
        await db.commit()

# -------------------- Клавиатуры --------------------
def main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🛒 Купить звёзды", callback_data="buy_stars")
    builder.button(text="👤 Мой профиль", callback_data="profile")
    builder.button(text="ℹ️ Помощь", callback_data="help")
    builder.adjust(1)
    return builder.as_markup()

def buy_options_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📦 Выбрать из пакетов", callback_data="select_package")
    builder.button(text="✏️ Ввести своё количество", callback_data="custom_amount")
    builder.button(text="🔙 Назад", callback_data="back_to_main")
    builder.adjust(1)
    return builder.as_markup()

def packages_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    packages = [
        {"stars": 50, "price": round(50 * STAR_RATE_RUB)},
        {"stars": 100, "price": round(100 * STAR_RATE_RUB)},
        {"stars": 250, "price": round(250 * STAR_RATE_RUB)},
        {"stars": 500, "price": round(500 * STAR_RATE_RUB)},
        {"stars": 1000, "price": round(1000 * STAR_RATE_RUB)},
    ]
    for pkg in packages:
        builder.button(
            text=f"{pkg['stars']} ⭐ за {pkg['price']} ₽",
            callback_data=f"package_{pkg['stars']}",
        )
    builder.button(text="🔙 Назад", callback_data="back_to_buy_options")
    builder.adjust(2)
    return builder.as_markup()

def payment_method_keyboard(stars: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💎 Криптокошелёк TON", callback_data=f"pay_crypto_{stars}")
    builder.button(text="💳 Перевод на карту", callback_data=f"pay_manual_{stars}")
    builder.button(text="🔙 Назад", callback_data="back_to_packages")
    builder.adjust(1)
    return builder.as_markup()

def admin_menu_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text="📋 Заказы")
    builder.button(text="⏳ Ожидают подтверждения")
    builder.button(text="📢 Рассылка")
    builder.button(text="📊 Статистика")
    builder.button(text="❌ Закрыть админ-панель")
    builder.adjust(2, 2, 1)
    return builder.as_markup(resize_keyboard=True)

def cancel_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text="❌ Отмена")
    return builder.as_markup(resize_keyboard=True)

def admin_order_keyboard(order_id: int, user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить оплату", callback_data=f"confirm_{order_id}")
    builder.button(text="👤 Написать покупателю", url=f"tg://user?id={user_id}")
    builder.adjust(1)
    return builder.as_markup()

# -------------------- Состояния FSM --------------------
class BuyStars(StatesGroup):
    choosing_option = State()
    choosing_package = State()
    entering_custom_amount = State()
    choosing_payment = State()

class BroadcastState(StatesGroup):
    waiting_for_message = State()

# -------------------- Обработчики команд --------------------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await create_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name
    )
    await message.answer(
        f"🌟 Добро пожаловать в магазин Telegram Stars!\n\n"
        f"Курс: 1 ⭐ = {STAR_RATE_RUB} ₽ (округляется до целых)\n"
        f"Минимальная покупка: {MIN_STARS} ⭐\n\n"
        f"Способы оплаты:\n"
        f"• Перевод на криптокошелёк (TON)\n"
        f"• Перевод на карту по номеру телефона\n\n"
        f"Выберите действие:",
        reply_markup=main_menu_keyboard(),
    )

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Главное меню:", reply_markup=main_menu_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "profile")
async def show_profile(callback: CallbackQuery):
    user = await get_user(callback.from_user.id)
    if user:
        text = (
            f"👤 Профиль\n"
            f"ID: {user['user_id']}\n"
            f"Баланс звёзд: {user['stars_balance']} ⭐"
        )
    else:
        text = "Профиль не найден. Нажмите /start."
    await callback.message.edit_text(text, reply_markup=main_menu_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "help")
async def show_help(callback: CallbackQuery):
    text = (
        "ℹ️ Инструкция:\n\n"
        "1. Выберите количество звёзд (минимум 50)\n"
        "2. Выберите способ оплаты\n"
        "3. Оплатите по реквизитам\n"
        "4. Отправьте чек/скриншот оплаты боту\n"
        "5. Администратор проверит оплату и начислит звёзды\n\n"
        f"По всем вопросам: {ADMIN_USERNAME}"
    )
    await callback.message.edit_text(text, reply_markup=main_menu_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "buy_stars")
async def buy_stars(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BuyStars.choosing_option)
    await callback.message.edit_text(
        "Выберите способ выбора количества звёзд:",
        reply_markup=buy_options_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_buy_options", BuyStars.choosing_package)
async def back_to_buy_options(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BuyStars.choosing_option)
    await callback.message.edit_text(
        "Выберите способ выбора количества звёзд:",
        reply_markup=buy_options_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "select_package", BuyStars.choosing_option)
async def select_package(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BuyStars.choosing_package)
    await callback.message.edit_text(
        "Выберите пакет звёзд:",
        reply_markup=packages_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "custom_amount", BuyStars.choosing_option)
async def custom_amount_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BuyStars.entering_custom_amount)
    await callback.message.edit_text(
        f"✏️ Введите количество звёзд (минимум {MIN_STARS}):\n\n"
        f"Курс: 1 ⭐ = {STAR_RATE_RUB} ₽\n"
        f"Сумма округляется до целых рублей.",
        reply_markup=cancel_keyboard()
    )
    await callback.answer()

@dp.message(BuyStars.entering_custom_amount, F.text.regexp(r'^\d+$'))
async def process_custom_amount(message: Message, state: FSMContext):
    stars = int(message.text)
    
    if stars < MIN_STARS:
        await message.answer(
            f"❌ Минимальное количество звёзд: {MIN_STARS}\n"
            f"Пожалуйста, введите число от {MIN_STARS} и больше:",
            reply_markup=cancel_keyboard()
        )
        return
    
    price = round(stars * STAR_RATE_RUB)
    
    await state.update_data(chosen_stars=stars)
    await state.set_state(BuyStars.choosing_payment)
    
    await message.answer(
        f"✅ Выбрано: {stars} ⭐\n"
        f"Стоимость: {price} ₽\n\n"
        f"Выберите способ оплаты:",
        reply_markup=payment_method_keyboard(stars)
    )

@dp.message(BuyStars.entering_custom_amount, F.text == "❌ Отмена")
async def cancel_custom_amount(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Операция отменена.", reply_markup=main_menu_keyboard())

@dp.message(BuyStars.entering_custom_amount)
async def invalid_custom_amount(message: Message):
    await message.answer(
        "❌ Пожалуйста, введите целое число.",
        reply_markup=cancel_keyboard()
    )

@dp.callback_query(F.data.startswith("package_"), BuyStars.choosing_package)
async def package_selected(callback: CallbackQuery, state: FSMContext):
    stars = int(callback.data.split("_")[1])
    await state.update_data(chosen_stars=stars)
    await state.set_state(BuyStars.choosing_payment)
    
    price = round(stars * STAR_RATE_RUB)
    
    await callback.message.edit_text(
        f"Пакет: {stars} ⭐\n"
        f"Стоимость: {price} ₽\n\n"
        f"Выберите способ оплаты:",
        reply_markup=payment_method_keyboard(stars)
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_packages", BuyStars.choosing_payment)
async def back_to_packages(callback: CallbackQuery, state: FSMContext):
    await state.set_state(BuyStars.choosing_package)
    await callback.message.edit_text(
        "Выберите пакет звёзд:",
        reply_markup=packages_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("pay_"), BuyStars.choosing_payment)
async def payment_selected(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    stars = data["chosen_stars"]
    price = round(stars * STAR_RATE_RUB)
    
    username = callback.from_user.username or f"user{callback.from_user.id}"
    user_mention = f"@{username}" if callback.from_user.username else f"[Пользователь](tg://user?id={callback.from_user.id})"
    
    if callback.data.startswith("pay_crypto_"):
        order_id = await create_order(
            user_id=callback.from_user.id,
            username=username,
            package_stars=stars,
            amount_rub=price,
            payment_method="crypto_manual",
        )
        
        text = (
            f"🧾 Заказ #{order_id}\n"
            f"Покупатель: {user_mention}\n"
            f"Количество: {stars} ⭐\n"
            f"Сумма: {price} ₽\n\n"
            f"💎 Реквизиты для оплаты криптовалютой:\n"
            f"Сеть: {CRYPTO_WALLET_NETWORK}\n"
            f"Адрес кошелька:\n<code>{CRYPTO_WALLET_ADDRESS}</code>\n\n"
            f"⚠️ После оплаты отправьте СКРИНШОТ/ЧЕК сюда же в бота, указав номер заказа #{order_id}"
        )
        
    else:
        order_id = await create_order(
            user_id=callback.from_user.id,
            username=username,
            package_stars=stars,
            amount_rub=price,
            payment_method="manual",
        )
        
        text = (
            f"🧾 Заказ #{order_id}\n"
            f"Покупатель: {user_mention}\n"
            f"Количество: {stars} ⭐\n"
            f"Сумма: {price} ₽\n\n"
            f"💳 Реквизиты для перевода на карту:\n"
            f"Телефон: {MANUAL_PAYMENT_PHONE}\n"
            f"Получатель: {MANUAL_PAYMENT_NAME}\n"
            f"Банк: {MANUAL_PAYMENT_BANK}\n\n"
            f"⚠️ После оплаты отправьте СКРИНШОТ/ЧЕК сюда же в бота, указав номер заказа #{order_id}"
        )
    
    await callback.message.edit_text(
        text,
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML"
    )
    
    admin_text = (
        f"🔔 Новый заказ #{order_id}\n"
        f"Покупатель: {user_mention}\n"
        f"Количество: {stars} ⭐\n"
        f"Сумма: {price} ₽\n"
        f"Метод: {'Крипто' if 'crypto' in callback.data else 'Карта'}\n"
        f"Статус: ожидает оплаты"
    )
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                admin_text,
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")
    
    await state.clear()
    await callback.answer()

# -------------------- Обработка чеков --------------------
@dp.message(F.photo | F.document)
async def handle_payment_proof(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or f"user{user_id}"
    user_mention = f"@{username}" if message.from_user.username else f"[Пользователь](tg://user?id={user_id})"
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 1",
            (user_id,)
        ) as cursor:
            order = await cursor.fetchone()
    
    if not order:
        await message.answer(
            "❌ У вас нет активных заказов. Сначала создайте заказ через /start",
            reply_markup=main_menu_keyboard()
        )
        return
    
    order_dict = dict(order)
    
    caption = (
        f"📎 Чек к заказу #{order_dict['id']}\n"
        f"От: {user_mention}\n"
        f"Сумма: {order_dict['amount_rub']} ₽\n"
        f"Звёзд: {order_dict['package_stars']} ⭐\n"
        f"Метод: {'Крипто' if order_dict['payment_method'] == 'crypto_manual' else 'Карта'}"
    )
    
    for admin_id in ADMIN_IDS:
        try:
            if message.photo:
                admin_msg = await bot.send_photo(
                    admin_id,
                    message.photo[-1].file_id,
                    caption=caption,
                    reply_markup=admin_order_keyboard(order_dict['id'], user_id),
                    parse_mode="HTML"
                )
            else:
                admin_msg = await bot.send_document(
                    admin_id,
                    message.document.file_id,
                    caption=caption,
                    reply_markup=admin_order_keyboard(order_dict['id'], user_id),
                    parse_mode="HTML"
                )
            
            await update_order_admin_message(order_dict['id'], admin_msg.message_id)
            
        except Exception as e:
            logger.error(f"Failed to forward proof to admin {admin_id}: {e}")
    
    await message.answer(
        f"✅ Чек по заказу #{order_dict['id']} отправлен администратору.\n"
        f"Ожидайте подтверждения. Обычно это занимает до 15 минут.",
        reply_markup=main_menu_keyboard()
    )

@dp.message(F.text.contains("чек") | F.text.contains("оплат") | F.text.contains("#"))
async def handle_payment_text(message: Message):
    await message.answer(
        "📎 Для подтверждения оплаты отправьте СКРИНШОТ или PDF-чек сюда в чат.\n"
        "Администратор проверит его и начислит звёзды.",
        reply_markup=main_menu_keyboard()
    )

# -------------------- Админ-панель --------------------
@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Доступ запрещён.")
        return
    
    await message.answer(
        "🔧 Админ-панель активирована",
        reply_markup=admin_menu_keyboard()
    )

@dp.message(F.text == "❌ Закрыть админ-панель")
async def close_admin_panel(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer(
        "Админ-панель закрыта",
        reply_markup=types.ReplyKeyboardRemove()
    )

@dp.message(F.text == "📋 Заказы")
async def admin_orders_list(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM orders ORDER BY created_at DESC LIMIT 20"
        ) as cursor:
            rows = await cursor.fetchall()
            orders = [dict(row) for row in rows]
    
    if not orders:
        await message.answer("Заказов нет.")
        return
    
    text = "📋 Последние заказы:\n\n"
    for order in orders:
        status_emoji = "✅" if order['status'] == 'paid' else "⏳"
        text += (
            f"{status_emoji} #{order['id']} | @{order['username']}\n"
            f"   {order['package_stars']} ⭐ | {order['amount_rub']} ₽ | {order['payment_method']}\n"
            f"   {order['created_at'][:16]}\n\n"
        )
    
    await message.answer(text)

@dp.message(F.text == "⏳ Ожидают подтверждения")
async def admin_pending_orders(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    orders = await get_pending_orders()
    
    if not orders:
        await message.answer("Нет заказов, ожидающих подтверждения.")
        return
    
    await message.answer(f"⏳ Заказов в ожидании: {len(orders)}\n\nДля подтверждения используйте кнопку под чеком.")
    
    for order in orders[:5]:
        user_mention = f"@{order['username']}" if order['username'] else f"[Пользователь](tg://user?id={order['user_id']})"
        text = (
            f"🔔 Заказ #{order['id']}\n"
            f"Покупатель: {user_mention}\n"
            f"Количество: {order['package_stars']} ⭐\n"
            f"Сумма: {order['amount_rub']} ₽\n"
            f"Метод: {'Крипто' if order['payment_method'] == 'crypto_manual' else 'Карта'}\n"
            f"Создан: {order['created_at'][:16]}"
        )
        
        await message.answer(
            text,
            reply_markup=admin_order_keyboard(order['id'], order['user_id']),
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("confirm_"))
async def confirm_order(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Доступ запрещён", show_alert=True)
        return
    
    order_id = int(callback.data.split("_")[1])
    order = await get_order(order_id)
    
    if not order:
        await callback.answer("Заказ не найден", show_alert=True)
        return
    
    if order["status"] != "pending":
        await callback.answer("Заказ уже обработан", show_alert=True)
        return
    
    await update_order_status(order_id, "paid")
    await update_user_balance(order["user_id"], order["package_stars"])
    
    try:
        await bot.send_message(
            order["user_id"],
            f"✅ Ваш заказ #{order_id} подтверждён!\n"
            f"На баланс зачислено {order['package_stars']} ⭐.\n"
            f"Спасибо за покупку!"
        )
    except Exception as e:
        logger.error(f"Failed to notify user {order['user_id']}: {e}")
    
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.reply("✅ Заказ подтверждён, звёзды начислены!")
    except:
        pass
    
    await callback.answer("Заказ подтверждён!", show_alert=True)

@dp.message(F.text == "📢 Рассылка")
async def broadcast_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await state.set_state(BroadcastState.waiting_for_message)
    await message.answer(
        "📢 Введите сообщение для рассылки всем пользователям:\n\n"
        "Для отмены нажмите кнопку ниже",
        reply_markup=cancel_keyboard()
    )

@dp.message(BroadcastState.waiting_for_message, F.text == "❌ Отмена")
async def broadcast_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "❌ Рассылка отменена",
        reply_markup=admin_menu_keyboard()
    )

@dp.message(BroadcastState.waiting_for_message)
async def broadcast_send(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    users = await get_all_users()
    
    success = 0
    failed = 0
    
    await message.answer(f"📤 Начинаю рассылку на {len(users)} пользователей...")
    
    for user in users:
        try:
            await bot.send_message(user['user_id'], message.text)
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"Failed to send broadcast to {user['user_id']}: {e}")
    
    await state.clear()
    await message.answer(
        f"✅ Рассылка завершена!\n"
        f"Успешно: {success}\n"
        f"Не удалось: {failed}",
        reply_markup=admin_menu_keyboard()
    )

@dp.message(F.text == "📊 Статистика")
async def admin_stats(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        async with db.execute("SELECT COUNT(*) as count FROM users") as cursor:
            users_count = (await cursor.fetchone())['count']
        
        async with db.execute("SELECT COUNT(*) as count FROM orders") as cursor:
            orders_count = (await cursor.fetchone())['count']
        
        async with db.execute("SELECT COUNT(*) as count FROM orders WHERE status = 'paid'") as cursor:
            paid_count = (await cursor.fetchone())['count']
        
        async with db.execute("SELECT SUM(amount_rub) as total FROM orders WHERE status = 'paid'") as cursor:
            total_sales = (await cursor.fetchone())['total'] or 0
        
        async with db.execute("SELECT SUM(package_stars) as total FROM orders WHERE status = 'paid'") as cursor:
            total_stars = (await cursor.fetchone())['total'] or 0
    
    text = (
        f"📊 Статистика бота:\n\n"
        f"👥 Пользователей: {users_count}\n"
        f"📦 Всего заказов: {orders_count}\n"
        f"✅ Оплачено заказов: {paid_count}\n"
        f"💰 Общая выручка: {total_sales} ₽\n"
        f"⭐ Продано звёзд: {total_stars}"
    )
    
    await message.answer(text)

# -------------------- Health check для Render --------------------
async def health_check():
    """Веб-сервер для health check на Render"""
    app = web.Application()
    
    async def health(request):
        return web.Response(text="Bot is running")
    
    app.router.add_get('/', health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    
    # Держим сервер запущенным
    while True:
        await asyncio.sleep(3600)

# -------------------- Запуск --------------------
async def main():
    await init_db()
    print("🤖 Бот запущен!")
    print(f"👑 Администратор: {ADMIN_USERNAME}")
    
    # Запускаем health check сервер для Render
    asyncio.create_task(health_check())
    
    # Уведомление админам о запуске
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"✅ Бот запущен!\n"
                f"Админ-панель: /admin"
            )
        except:
            pass
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
