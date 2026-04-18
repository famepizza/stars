import asyncio
import logging
import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from collections import defaultdict
import hashlib

import aiosqlite
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from aiohttp import web

# ---------- НАСТРОЙКИ ----------
BOT_TOKEN = "8733133714:AAFG5g1MFA_q6gAvSFtj2vbhOosxcfrJ2SI"
ADMIN_IDS = [8356674232]
ADMIN_USERNAME = "@Gabarovv"

STAR_RATE_RUB = Decimal("1.4")
MIN_STARS = 50
MAX_STARS = 100000
MAX_ORDER_AMOUNT = 500000

MANUAL_PAYMENT_PHONE = "+79026674703"
MANUAL_PAYMENT_NAME = "кирилл"
MANUAL_PAYMENT_BANK = "Озон Банк"

CRYPTO_WALLET_ADDRESS = "UQDL9Eo8eKPkBpsNY1KoUjYXu23dGmsfDTJnFG_1GgqcetDf"
CRYPTO_WALLET_NETWORK = "TON"

DATABASE_PATH = "bot.db"

# Rate limiting
RATE_LIMIT = {
    "orders_per_hour": 10,
    "proofs_per_hour": 5,
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Хранилище для rate limiting
rate_limit_storage = defaultdict(list)

# -------------------- Rate Limiting --------------------
def check_rate_limit(user_id: int, action: str) -> bool:
    """Проверка rate limiting"""
    now = datetime.now()
    key = f"{user_id}:{action}"
    
    rate_limit_storage[key] = [
        ts for ts in rate_limit_storage[key] 
        if now - ts < timedelta(hours=1)
    ]
    
    limit = RATE_LIMIT.get(f"{action}_per_hour", 10)
    
    if len(rate_limit_storage[key]) >= limit:
        return False
    
    rate_limit_storage[key].append(now)
    return True

# -------------------- Простая работа с БД (без сложного пула) --------------------
@asynccontextmanager
async def get_db():
    """Контекстный менеджер для подключения к БД"""
    conn = await aiosqlite.connect(DATABASE_PATH)
    conn.row_factory = aiosqlite.Row
    try:
        yield conn
    finally:
        await conn.close()

async def init_db():
    """Инициализация базы данных"""
    async with get_db() as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                stars_balance INTEGER DEFAULT 0,
                total_orders INTEGER DEFAULT 0,
                total_spent INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                package_stars INTEGER NOT NULL,
                amount_rub INTEGER NOT NULL,
                payment_method TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                admin_message_id INTEGER,
                proof_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        
        # Индексы для оптимизации
        await db.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_status ON orders(user_id, status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_orders_status_created ON orders(status, created_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_balance ON users(stars_balance)")
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS action_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await db.commit()

# -------------------- Вспомогательные функции --------------------
async def log_action(user_id: int, action: str, details: str = None):
    """Логирование действий пользователя"""
    async with get_db() as db:
        await db.execute(
            "INSERT INTO action_logs (user_id, action, details) VALUES (?, ?, ?)",
            (user_id, action, details)
        )
        await db.commit()

async def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    async with get_db() as db:
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

async def create_user(user_id: int, username: str = None, full_name: str = None):
    async with get_db() as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (user_id, username, full_name) VALUES (?, ?, ?)",
            (user_id, username, full_name),
        )
        await db.commit()

async def update_user_balance(user_id: int, stars_add: int):
    async with get_db() as db:
        await db.execute(
            "UPDATE users SET stars_balance = stars_balance + ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
            (stars_add, user_id),
        )
        await db.commit()

async def update_user_stats(user_id: int, amount: int):
    """Обновление статистики пользователя"""
    async with get_db() as db:
        await db.execute(
            "UPDATE users SET total_orders = total_orders + 1, total_spent = total_spent + ? WHERE user_id = ?",
            (amount, user_id)
        )
        await db.commit()

async def create_order(
    user_id: int,
    username: str,
    package_stars: int,
    amount_rub: int,
    payment_method: str,
) -> int:
    async with get_db() as db:
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
    async with get_db() as db:
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
    async with get_db() as db:
        async with db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

async def get_pending_orders() -> List[Dict[str, Any]]:
    async with get_db() as db:
        async with db.execute(
            "SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at DESC"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

async def get_user_active_orders(user_id: int) -> List[Dict[str, Any]]:
    """Получить активные (pending) заказы пользователя"""
    async with get_db() as db:
        async with db.execute(
            "SELECT * FROM orders WHERE user_id = ? AND status = 'pending' ORDER BY created_at DESC",
            (user_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

async def update_order_proof_hash(order_id: int, proof_hash: str):
    """Обновить хеш чека в заказе"""
    async with get_db() as db:
        await db.execute(
            "UPDATE orders SET proof_hash = ? WHERE id = ?",
            (proof_hash, order_id)
        )
        await db.commit()

# -------------------- Валидация --------------------
def validate_stars_amount(stars: int) -> tuple[bool, str]:
    """Валидация количества звёзд"""
    if stars < MIN_STARS:
        return False, f"❌ Минимальное количество звёзд: {MIN_STARS}"
    if stars > MAX_STARS:
        return False, f"❌ Максимальное количество звёзд: {MAX_STARS}"
    
    price = stars * STAR_RATE_RUB
    if price > MAX_ORDER_AMOUNT:
        return False, f"❌ Сумма заказа не может превышать {MAX_ORDER_AMOUNT} ₽"
    
    return True, ""

def validate_payment_proof(message: Message) -> tuple[bool, str]:
    """Валидация чека/скриншота"""
    if not (message.photo or message.document):
        return False, "Пожалуйста, отправьте фото или документ"
    
    if not check_rate_limit(message.from_user.id, "proofs"):
        return False, "❌ Слишком много запросов. Подождите час перед отправкой нового чека."
    
    return True, ""

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
        {"stars": 50, "price": int(50 * STAR_RATE_RUB)},
        {"stars": 100, "price": int(100 * STAR_RATE_RUB)},
        {"stars": 250, "price": int(250 * STAR_RATE_RUB)},
        {"stars": 500, "price": int(500 * STAR_RATE_RUB)},
        {"stars": 1000, "price": int(1000 * STAR_RATE_RUB)},
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
    builder.button(text="🗑 Очистить старые заказы")
    builder.button(text="❌ Закрыть админ-панель")
    builder.adjust(2, 2, 2)
    return builder.as_markup(resize_keyboard=True)

def cancel_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text="❌ Отмена")
    return builder.as_markup(resize_keyboard=True)

def admin_order_keyboard(order_id: int, user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить оплату", callback_data=f"confirm_{order_id}")
    builder.button(text="❌ Отклонить заказ", callback_data=f"reject_{order_id}")
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
    await log_action(message.from_user.id, "start", "User started bot")
    
    active_orders = await get_user_active_orders(message.from_user.id)
    active_warning = ""
    if active_orders:
        active_warning = f"\n\n⚠️ У вас есть {len(active_orders)} активных заказов. Отправьте чек для их подтверждения."
    
    await message.answer(
        f"🌟 Добро пожаловать в магазин Telegram Stars!\n\n"
        f"Курс: 1 ⭐ = {STAR_RATE_RUB} ₽\n"
        f"Минимальная покупка: {MIN_STARS} ⭐\n"
        f"Максимальная покупка: {MAX_STARS} ⭐{active_warning}\n\n"
        f"Способы оплаты:\n"
        f"• Перевод на криптокошелёк (TON)\n"
        f"• Перевод на карту по номеру телефона\n\n"
        f"Выберите действие:",
        reply_markup=main_menu_keyboard(),
        disable_web_page_preview=True
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
            f"👤 <b>Профиль</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🆔 ID: <code>{user['user_id']}</code>\n"
            f"⭐ Баланс звёзд: <b>{user['stars_balance']}</b>\n"
            f"📦 Всего заказов: <b>{user['total_orders']}</b>\n"
            f"💰 Потрачено: <b>{user['total_spent']}</b> ₽\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🎁 <i>За каждую покупку вы получаете бонусные звёзды!</i>"
        )
    else:
        text = "❌ Профиль не найден. Нажмите /start."
    await callback.message.edit_text(text, reply_markup=main_menu_keyboard(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "help")
async def show_help(callback: CallbackQuery):
    text = (
        "ℹ️ <b>Инструкция по покупке звёзд</b>\n\n"
        "1️⃣ <b>Выберите количество</b>\n"
        "   • Минимум: 50 ⭐\n"
        "   • Максимум: 100,000 ⭐\n\n"
        "2️⃣ <b>Выберите способ оплаты</b>\n"
        "   • Криптокошелёк (TON)\n"
        "   • Перевод на карту\n\n"
        "3️⃣ <b>Оплатите по реквизитам</b>\n"
        "   • Точная сумма указана в заказе\n"
        "   • Обязательно сохраните чек\n\n"
        "4️⃣ <b>Отправьте чек боту</b>\n"
        "   • Фото или PDF файл\n"
        "   • Укажите номер заказа в сообщении\n\n"
        "5️⃣ <b>Ожидайте подтверждения</b>\n"
        "   • Обычно до 15 минут\n"
        "   • Звёзды поступят на баланс\n\n"
        f"📞 <b>Поддержка:</b> {ADMIN_USERNAME}\n\n"
        f"⚠️ <i>Никогда не отправляйте чек третьим лицам!</i>"
    )
    await callback.message.edit_text(text, reply_markup=main_menu_keyboard(), parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "buy_stars")
async def buy_stars(callback: CallbackQuery, state: FSMContext):
    if not check_rate_limit(callback.from_user.id, "orders"):
        await callback.answer("❌ Слишком много заказов. Подождите час.", show_alert=True)
        return
    
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
        f"✏️ <b>Введите количество звёзд</b>\n\n"
        f"📊 Курс: 1 ⭐ = {STAR_RATE_RUB} ₽\n"
        f"📏 Минимум: {MIN_STARS} ⭐\n"
        f"📏 Максимум: {MAX_STARS} ⭐\n"
        f"💰 Сумма округляется до целых рублей.\n\n"
        f"<i>Пример: 100 → {int(100 * STAR_RATE_RUB)} ₽</i>",
        reply_markup=cancel_keyboard(),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.message(BuyStars.entering_custom_amount, F.text.regexp(r'^\d+$'))
async def process_custom_amount(message: Message, state: FSMContext):
    stars = int(message.text)
    
    is_valid, error_msg = validate_stars_amount(stars)
    if not is_valid:
        await message.answer(
            f"{error_msg}\n\nПожалуйста, введите другое число:",
            reply_markup=cancel_keyboard()
        )
        return
    
    price = int(stars * STAR_RATE_RUB)
    
    await state.update_data(chosen_stars=stars)
    await state.set_state(BuyStars.choosing_payment)
    
    await message.answer(
        f"✅ <b>Выбрано:</b> {stars} ⭐\n"
        f"💰 <b>Стоимость:</b> {price} ₽\n\n"
        f"Выберите способ оплаты:",
        reply_markup=payment_method_keyboard(stars),
        parse_mode="HTML"
    )

@dp.message(BuyStars.entering_custom_amount, F.text == "❌ Отмена")
async def cancel_custom_amount(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Операция отменена.", reply_markup=main_menu_keyboard())

@dp.message(BuyStars.entering_custom_amount)
async def invalid_custom_amount(message: Message):
    await message.answer(
        "❌ Пожалуйста, введите целое число (только цифры).",
        reply_markup=cancel_keyboard()
    )

@dp.callback_query(F.data.startswith("package_"), BuyStars.choosing_package)
async def package_selected(callback: CallbackQuery, state: FSMContext):
    stars = int(callback.data.split("_")[1])
    await state.update_data(chosen_stars=stars)
    await state.set_state(BuyStars.choosing_payment)
    
    price = int(stars * STAR_RATE_RUB)
    
    await callback.message.edit_text(
        f"✅ <b>Пакет:</b> {stars} ⭐\n"
        f"💰 <b>Стоимость:</b> {price} ₽\n\n"
        f"Выберите способ оплаты:",
        reply_markup=payment_method_keyboard(stars),
        parse_mode="HTML"
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
    price = int(stars * STAR_RATE_RUB)
    
    username = callback.from_user.username or f"user{callback.from_user.id}"
    user_mention = f"@{username}" if callback.from_user.username else f"[Пользователь](tg://user?id={callback.from_user.id})"
    
    active_orders = await get_user_active_orders(callback.from_user.id)
    if len(active_orders) >= 5:
        await callback.answer("❌ У вас слишком много активных заказов. Дождитесь обработки предыдущих.", show_alert=True)
        return
    
    if callback.data.startswith("pay_crypto_"):
        order_id = await create_order(
            user_id=callback.from_user.id,
            username=username,
            package_stars=stars,
            amount_rub=price,
            payment_method="crypto_manual",
        )
        
        text = (
            f"🧾 <b>Заказ #{order_id}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 Покупатель: {user_mention}\n"
            f"⭐ Количество: {stars}\n"
            f"💰 Сумма: {price} ₽\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💎 <b>Реквизиты для оплаты криптовалютой:</b>\n"
            f"🌐 Сеть: {CRYPTO_WALLET_NETWORK}\n"
            f"📮 Адрес кошелька:\n"
            f"<code>{CRYPTO_WALLET_ADDRESS}</code>\n\n"
            f"⚠️ <b>Важно!</b>\n"
            f"1️⃣ Отправьте точную сумму: {price} ₽\n"
            f"2️⃣ После оплаты отправьте СКРИНШОТ/ЧЕК сюда\n"
            f"3️⃣ Укажите номер заказа <b>#{order_id}</b>\n\n"
            f"⏱ Заказ действителен 24 часа"
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
            f"🧾 <b>Заказ #{order_id}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 Покупатель: {user_mention}\n"
            f"⭐ Количество: {stars}\n"
            f"💰 Сумма: {price} ₽\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💳 <b>Реквизиты для перевода на карту:</b>\n"
            f"📱 Телефон: <code>{MANUAL_PAYMENT_PHONE}</code>\n"
            f"👤 Получатель: {MANUAL_PAYMENT_NAME}\n"
            f"🏦 Банк: {MANUAL_PAYMENT_BANK}\n\n"
            f"⚠️ <b>Важно!</b>\n"
            f"1️⃣ Отправьте точную сумму: {price} ₽\n"
            f"2️⃣ После оплаты отправьте СКРИНШОТ/ЧЕК сюда\n"
            f"3️⃣ Укажите номер заказа <b>#{order_id}</b>\n\n"
            f"⏱ Заказ действителен 24 часа"
        )
    
    await log_action(callback.from_user.id, "create_order", f"Order #{order_id}: {stars} stars, {price} rub")
    
    await callback.message.edit_text(
        text,
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML"
    )
    
    admin_text = (
        f"🔔 <b>Новый заказ #{order_id}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Покупатель: {user_mention}\n"
        f"⭐ Количество: {stars}\n"
        f"💰 Сумма: {price} ₽\n"
        f"💳 Метод: {'Крипто' if 'crypto' in callback.data else 'Карта'}\n"
        f"⏳ Статус: ожидает оплаты"
    )
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, admin_text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")
    
    await state.clear()
    await callback.answer("✅ Заказ создан! Оплатите по реквизитам и отправьте чек.", show_alert=True)

# -------------------- Обработка чеков --------------------
@dp.message(F.photo | F.document)
async def handle_payment_proof(message: Message):
    is_valid, error_msg = validate_payment_proof(message)
    if not is_valid:
        await message.answer(error_msg, reply_markup=main_menu_keyboard())
        return
    
    user_id = message.from_user.id
    username = message.from_user.username or f"user{user_id}"
    user_mention = f"@{username}" if message.from_user.username else f"[Пользователь](tg://user?id={user_id})"
    
    active_orders = await get_user_active_orders(user_id)
    
    if not active_orders:
        await message.answer(
            "❌ У вас нет активных заказов.\n\n"
            "Чтобы создать заказ, нажмите /start и выберите '🛒 Купить звёзды'",
            reply_markup=main_menu_keyboard()
        )
        return
    
    if len(active_orders) > 1:
        builder = InlineKeyboardBuilder()
        for order in active_orders[:5]:
            builder.button(
                text=f"Заказ #{order['id']} - {order['package_stars']}⭐",
                callback_data=f"select_order_{order['id']}"
            )
        builder.adjust(1)
        
        await message.answer(
            f"📋 У вас есть {len(active_orders)} активных заказов.\n"
            f"Пожалуйста, выберите, к какому заказу относится этот чек:",
            reply_markup=builder.as_markup()
        )
        
        await dp.storage.update_data(
            chat_id=message.chat.id,
            user_id=user_id,
            data={"pending_proof": message.photo[-1].file_id if message.photo else message.document.file_id, 
                  "is_photo": bool(message.photo)}
        )
        return
    
    order = active_orders[0]
    await process_proof_for_order(message, order, user_mention)

@dp.callback_query(F.data.startswith("select_order_"))
async def select_order_for_proof(callback: CallbackQuery):
    order_id = int(callback.data.split("_")[2])
    order = await get_order(order_id)
    
    if not order or order["status"] != "pending":
        await callback.answer("❌ Заказ не найден или уже обработан", show_alert=True)
        return
    
    user_data = await dp.storage.get_data(chat_id=callback.message.chat.id, user_id=callback.from_user.id)
    if "pending_proof" not in user_data:
        await callback.answer("❌ Ошибка: чек не найден. Отправьте чек заново.", show_alert=True)
        return
    
    user_mention = f"@{callback.from_user.username}" if callback.from_user.username else f"[Пользователь](tg://user?id={callback.from_user.id})"
    
    class FakeMessage:
        def __init__(self, file_id, is_photo, chat_id, from_user):
            self.photo = [types.PhotoSize(file_id=file_id, width=0, height=0)] if is_photo else None
            self.document = types.Document(file_id=file_id) if not is_photo else None
            self.chat = types.Chat(id=chat_id)
            self.from_user = from_user
    
    fake_msg = FakeMessage(
        user_data["pending_proof"],
        user_data["is_photo"],
        callback.message.chat.id,
        callback.from_user
    )
    
    await process_proof_for_order(fake_msg, order, user_mention)
    await callback.answer()

async def process_proof_for_order(message, order: Dict, user_mention: str):
    order_id = order["id"]
    
    if order.get("proof_hash"):
        await message.answer(
            f"⚠️ Для заказа #{order_id} уже был отправлен чек.\n"
            f"Администратор обработает его в ближайшее время.",
            reply_markup=main_menu_keyboard()
        )
        return
    
    proof_hash = hashlib.md5(f"{order_id}{message.photo[-1].file_id if message.photo else message.document.file_id}{datetime.now()}".encode()).hexdigest()
    await update_order_proof_hash(order_id, proof_hash)
    
    caption = (
        f"📎 <b>Чек к заказу #{order_id}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 От: {user_mention}\n"
        f"💰 Сумма: {order['amount_rub']} ₽\n"
        f"⭐ Звёзд: {order['package_stars']}\n"
        f"💳 Метод: {'Крипто' if order['payment_method'] == 'crypto_manual' else 'Карта'}\n"
        f"⏰ Время: {datetime.now().strftime('%H:%M:%S')}"
    )
    
    for admin_id in ADMIN_IDS:
        try:
            if message.photo:
                admin_msg = await bot.send_photo(
                    admin_id,
                    message.photo[-1].file_id,
                    caption=caption,
                    reply_markup=admin_order_keyboard(order_id, order['user_id']),
                    parse_mode="HTML"
                )
            else:
                admin_msg = await bot.send_document(
                    admin_id,
                    message.document.file_id,
                    caption=caption,
                    reply_markup=admin_order_keyboard(order_id, order['user_id']),
                    parse_mode="HTML"
                )
            
            await update_order_status(order_id, "pending", admin_msg.message_id)
            
        except Exception as e:
            logger.error(f"Failed to forward proof to admin {admin_id}: {e}")
    
    await log_action(order['user_id'], "send_proof", f"Order #{order_id}")
    
    await message.answer(
        f"✅ <b>Чек по заказу #{order_id} отправлен!</b>\n\n"
        f"📋 Администратор проверит оплату в ближайшее время.\n"
        f"⏱ Обычно это занимает до 15 минут.\n\n"
        f"💡 <i>Вы можете проверить статус заказа в профиле.</i>",
        reply_markup=main_menu_keyboard(),
        parse_mode="HTML"
    )

@dp.message(F.text)
async def handle_text_messages(message: Message):
    text_lower = message.text.lower()
    
    if any(word in text_lower for word in ["чек", "оплат", "квитанц", "скриншот"]):
        active_orders = await get_user_active_orders(message.from_user.id)
        if active_orders:
            await message.answer(
                "📎 <b>Как отправить чек:</b>\n\n"
                "1️⃣ Сделайте скриншот перевода\n"
                "2️⃣ Отправьте его как ФОТО или ДОКУМЕНТ\n"
                "3️⃣ Бот автоматически привяжет чек к вашему заказу\n\n"
                f"⚠️ Ваш активный заказ: #{active_orders[0]['id']}\n"
                f"⭐ {active_orders[0]['package_stars']} звёзд | {active_orders[0]['amount_rub']} ₽",
                parse_mode="HTML",
                reply_markup=main_menu_keyboard()
            )
        else:
            await message.answer(
                "📎 У вас нет активных заказов.\n"
                "Создайте заказ через /start → '🛒 Купить звёзды'",
                reply_markup=main_menu_keyboard()
            )

# -------------------- Админ-панель --------------------
@dp.message(Command("admin"))
async def admin_panel(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Доступ запрещён.")
        return
    
    await message.answer(
        "🔧 <b>Админ-панель активирована</b>\n\n"
        "📋 <b>Доступные команды:</b>\n"
        "• Заказы - просмотр всех заказов\n"
        "• Ожидают подтверждения - заказы с чеками\n"
        "• Рассылка - отправить сообщение всем\n"
        "• Статистика - аналитика бота\n"
        "• Очистить старые заказы - удалить заказы старше 30 дней",
        reply_markup=admin_menu_keyboard(),
        parse_mode="HTML"
    )

@dp.message(F.text == "❌ Закрыть админ-панель")
async def close_admin_panel(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer(
        "🔒 Админ-панель закрыта",
        reply_markup=types.ReplyKeyboardRemove()
    )

@dp.message(F.text == "📋 Заказы")
async def admin_orders_list(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    async with get_db() as db:
        async with db.execute(
            "SELECT * FROM orders ORDER BY created_at DESC LIMIT 30"
        ) as cursor:
            rows = await cursor.fetchall()
            orders = [dict(row) for row in rows]
    
    if not orders:
        await message.answer("📭 Заказов нет.")
        return
    
    text = "📋 <b>Последние 30 заказов:</b>\n\n"
    for order in orders:
        status_emoji = "✅" if order['status'] == 'paid' else "⏳" if order['status'] == 'pending' else "❌"
        text += (
            f"{status_emoji} <b>#{order['id']}</b> | @{order['username'] or 'no_username'}\n"
            f"   ⭐ {order['package_stars']} | 💰 {order['amount_rub']} ₽ | {order['payment_method']}\n"
            f"   📅 {order['created_at'][:16]}\n\n"
        )
    
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await message.answer(text[i:i+4000], parse_mode="HTML")
    else:
        await message.answer(text, parse_mode="HTML")

@dp.message(F.text == "⏳ Ожидают подтверждения")
async def admin_pending_orders(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    orders = await get_pending_orders()
    
    if not orders:
        await message.answer("✅ Нет заказов, ожидающих подтверждения.")
        return
    
    await message.answer(f"⏳ <b>Заказов в ожидании: {len(orders)}</b>\n\nДля подтверждения используйте кнопки под чеком.", parse_mode="HTML")
    
    for order in orders[:5]:
        user_mention = f"@{order['username']}" if order['username'] else f"[Пользователь](tg://user?id={order['user_id']})"
        text = (
            f"🔔 <b>Заказ #{order['id']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 Покупатель: {user_mention}\n"
            f"⭐ Количество: {order['package_stars']}\n"
            f"💰 Сумма: {order['amount_rub']} ₽\n"
            f"💳 Метод: {'Крипто' if order['payment_method'] == 'crypto_manual' else 'Карта'}\n"
            f"📅 Создан: {order['created_at'][:16]}"
        )
        
        await message.answer(
            text,
            reply_markup=admin_order_keyboard(order['id'], order['user_id']),
            parse_mode="HTML"
        )
    
    if len(orders) > 5:
        await message.answer(f"ℹ️ Показаны первые 5 из {len(orders)} заказов. Остальные можно посмотреть в списке заказов.")

@dp.callback_query(F.data.startswith("confirm_"))
async def confirm_order(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return
    
    order_id = int(callback.data.split("_")[1])
    order = await get_order(order_id)
    
    if not order:
        await callback.answer("❌ Заказ не найден", show_alert=True)
        return
    
    if order["status"] != "pending":
        status_text = "обработан" if order["status"] == "paid" else "отклонён"
        await callback.answer(f"❌ Заказ уже {status_text}", show_alert=True)
        return
    
    await update_order_status(order_id, "paid")
    await update_user_balance(order["user_id"], order["package_stars"])
    await update_user_stats(order["user_id"], order["amount_rub"])
    await log_action(order["user_id"], "order_confirmed", f"Order #{order_id}")
    
    try:
        await bot.send_message(
            order["user_id"],
            f"✅ <b>Заказ #{order_id} подтверждён!</b>\n\n"
            f"⭐ На баланс зачислено: <b>{order['package_stars']}</b> звёзд\n"
            f"💰 Сумма: {order['amount_rub']} ₽\n\n"
            f"🎉 Спасибо за покупку!\n"
            f"💡 Проверить баланс можно в профиле: /start → '👤 Мой профиль'",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Failed to notify user {order['user_id']}: {e}")
    
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.reply(f"✅ Заказ #{order_id} подтверждён, звёзды начислены!")
    except Exception:
        pass
    
    await callback.answer("✅ Заказ подтверждён!", show_alert=True)

@dp.callback_query(F.data.startswith("reject_"))
async def reject_order(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return
    
    order_id = int(callback.data.split("_")[1])
    order = await get_order(order_id)
    
    if not order:
        await callback.answer("❌ Заказ не найден", show_alert=True)
        return
    
    if order["status"] != "pending":
        await callback.answer("❌ Заказ уже обработан", show_alert=True)
        return
    
    await update_order_status(order_id, "rejected")
    await log_action(order["user_id"], "order_rejected", f"Order #{order_id}")
    
    try:
        await bot.send_message(
            order["user_id"],
            f"❌ <b>Заказ #{order_id} отклонён</b>\n\n"
            f"Похоже, мы не получили подтверждение оплаты.\n\n"
            f"📞 Если вы оплатили, пожалуйста, свяжитесь с поддержкой: {ADMIN_USERNAME}\n"
            f"💡 Вы можете создать новый заказ через /start",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Failed to notify user {order['user_id']}: {e}")
    
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.reply(f"❌ Заказ #{order_id} отклонён")
    except Exception:
        pass
    
    await callback.answer("❌ Заказ отклонён", show_alert=True)

@dp.message(F.text == "🗑 Очистить старые заказы")
async def cleanup_old_orders(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    cutoff_date = datetime.now() - timedelta(days=30)
    
    async with get_db() as db:
        result = await db.execute(
            "DELETE FROM orders WHERE status IN ('paid', 'rejected') AND created_at < ?",
            (cutoff_date.isoformat(),)
        )
        deleted_count = result.rowcount
        await db.commit()
    
    await message.answer(f"🗑 Удалено старых заказов: {deleted_count}")

@dp.message(F.text == "📢 Рассылка")
async def broadcast_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await state.set_state(BroadcastState.waiting_for_message)
    await message.answer(
        "📢 <b>Рассылка сообщения</b>\n\n"
        "Введите сообщение для рассылки всем пользователям.\n\n"
        "Поддерживается HTML-разметка.\n"
        "Пример: <b>жирный</b>, <i>курсив</i>, <a href='https://example.com'>ссылка</a>\n\n"
        "Для отмены нажмите ❌ Отмена",
        reply_markup=cancel_keyboard(),
        parse_mode="HTML"
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
    
    async with get_db() as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            users = await cursor.fetchall()
    
    success = 0
    failed = 0
    
    status_msg = await message.answer(f"📤 Начинаю рассылку на {len(users)} пользователей...")
    
    for i, user in enumerate(users):
        try:
            await bot.send_message(
                user['user_id'],
                message.text,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            success += 1
            
            if i % 50 == 0 and i > 0:
                await status_msg.edit_text(
                    f"📤 Рассылка в процессе...\n"
                    f"✅ Успешно: {success}\n"
                    f"❌ Не удалось: {failed}\n"
                    f"📊 Прогресс: {i}/{len(users)}"
                )
            
            await asyncio.sleep(0.05)
            
        except Exception as e:
            failed += 1
            logger.error(f"Failed to send broadcast to {user['user_id']}: {e}")
    
    await state.clear()
    await status_msg.edit_text(
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"📊 Статистика:\n"
        f"• Успешно: {success}\n"
        f"• Не удалось: {failed}\n"
        f"• Всего: {len(users)}",
        parse_mode="HTML",
        reply_markup=admin_menu_keyboard()
    )

@dp.message(F.text == "📊 Статистика")
async def admin_stats(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    async with get_db() as db:
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
        
        async with db.execute("SELECT AVG(amount_rub) as avg FROM orders WHERE status = 'paid'") as cursor:
            avg_order = (await cursor.fetchone())['avg'] or 0
        
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()
        async with db.execute(
            "SELECT COUNT(*) as count FROM orders WHERE status = 'paid' AND created_at > ?",
            (week_ago,)
        ) as cursor:
            week_orders = (await cursor.fetchone())['count']
    
    text = (
        f"📊 <b>Статистика бота</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 <b>Пользователей:</b> {users_count}\n"
        f"📦 <b>Всего заказов:</b> {orders_count}\n"
        f"✅ <b>Оплачено заказов:</b> {paid_count}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Общая выручка:</b> {total_sales:,} ₽\n"
        f"⭐ <b>Продано звёзд:</b> {total_stars:,}\n"
        f"📊 <b>Средний чек:</b> {avg_order:.0f} ₽\n"
        f"📈 <b>Заказов за 7 дней:</b> {week_orders}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 <i>Конверсия: {paid_count/orders_count*100:.1f}%</i>" if orders_count > 0 else ""
    )
    
    await message.answer(text, parse_mode="HTML")

# -------------------- Health check --------------------
async def health_check():
    app = web.Application()
    
    async def health(request):
        return web.Response(text="Bot is running")
    
    async def stats(request):
        try:
            async with get_db() as db:
                async with db.execute("SELECT COUNT(*) as count FROM users") as cursor:
                    users = (await cursor.fetchone())['count']
            return web.json_response({"status": "ok", "users": users, "timestamp": datetime.now().isoformat()})
        except Exception as e:
            return web.json_response({"status": "error", "error": str(e)}, status=500)
    
    app.router.add_get('/', health)
    app.router.add_get('/stats', stats)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    
    logger.info("Health check server started on port 8080")
    
    while True:
        await asyncio.sleep(3600)

# -------------------- Запуск --------------------
async def main():
    await init_db()
    logger.info("🤖 Бот запущен!")
    logger.info(f"👑 Администратор: {ADMIN_USERNAME}")
    
    asyncio.create_task(health_check())
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"✅ <b>Бот успешно запущен!</b>\n\n"
                f"📅 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🔧 Админ-панель: /admin",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
