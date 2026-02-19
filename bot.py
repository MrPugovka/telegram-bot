import os
from aiohttp import web
import math
import re
import asyncio
import logging
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from functools import wraps
import time
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BufferedInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from sheets import get_sheet, get_reports_sheet, update_reports, update_reports_extend
from drive import upload_contract_photo, get_or_create_folder_for_bike, check_folder_exists

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

#================= КОНФИГУРАЦИЯ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = "1xrCL9RBJHfNQGETgLLvnQtrSErNhQPeYkaXVSKkjSQo"
CACHE_TTL = 30
MAX_MSG_CHARS = 3800

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

MESSAGES_TO_DELETE_KEY = "messages_to_delete"

#================= ДЕКОРАТОР ДЛЯ ЗАМЕРА ПРОИЗВОДИТЕЛЬНОСТИ =================
def timing_decorator(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start = time.time()
        result = await func(*args, **kwargs)
        duration = time.time() - start
        logger.info(f"⏱️ {func.__name__} выполнена за {duration:.3f}s")
        return result
    return wrapper

#================= КЛАСС ДЛЯ РАБОТЫ С GOOGLE SHEETS =================
class BikeRepository:
    def __init__(self, cache_ttl=30):
        self.cache = {
            'data': None,
            'timestamp': None
        }
        self.cache_ttl = cache_ttl
        self._lock = asyncio.Lock()

    async def get_all(self, force_refresh=False):
        async with self._lock:
            now = datetime.now()
            if not force_refresh and self.cache['data'] and self.cache['timestamp']:
                if (now - self.cache['timestamp']).total_seconds() < self.cache_ttl:
                    logger.info("✅ Данные получены из кэша")
                    return self.cache['data']

            logger.info("🔄 Обновление данных из Google Sheets")
            sheet, rows = await asyncio.to_thread(self._get_sheet_data)

            self.cache['data'] = (sheet, rows)
            self.cache['timestamp'] = now

            return sheet, rows

    def _get_sheet_data(self):
        sheet = get_sheet()
        rows = sheet.get_all_records()
        return sheet, rows

    def invalidate_cache(self):
        self.cache['timestamp'] = None
        logger.info("🗑️ Кэш сброшен")

    async def get_available_bikes(self):
        sheet, rows = await self.get_all()
        return [row for row in rows if str(row.get("Статус", "")).strip() == "База"]

    async def get_rented_bikes(self):
        sheet, rows = await self.get_all()
        return [row for row in rows if str(row.get("Статус", "")).strip() == "Аренда"]

    async def get_bikes_by_brand(self, brand, status="База"):
        sheet, rows = await self.get_all()
        brands_list = ["honda", "kawasaki", "suzuki", "sym", "yamaha"]
        if brand is None:
            return []

        brand_str = str(brand).lower()
        bikes = []
        for i, row in enumerate(rows, start=2):
            if str(row.get("Статус", "")).strip() != status:
                continue

            model = str(row.get("МОДЕЛЬ", "")).lower()

            if brand_str in ["other", "другие"]:
                if not any(b in model for b in brands_list):
                    bikes.append((i, row))
            elif brand_str in model:
                bikes.append((i, row))

        return bikes

    async def get_all_brands(self):
        """Возвращает список всех брендов"""
        return ["Honda", "Kawasaki", "Suzuki", "SYM", "Yamaha", "Другие"]

    async def get_bikes_by_status(self, status):
        """Получает все байки с указанным статусом"""
        sheet, rows = await self.get_all()
        result = []
        for i, row in enumerate(rows, start=2):  # Данные начинаются со 2 строки
            if str(row.get("Статус", "")).strip() == status:
                result.append((i, row))
        return result

    async def update_bike(self, row_number, updates_dict):
        await asyncio.to_thread(self._batch_update, row_number, updates_dict)
        self.invalidate_cache()

    def _batch_update(self, row_number, updates_dict):
        sheet = get_sheet()
        headers = sheet.row_values(1)
        batch_data = []
        for key, value in updates_dict.items():
            if key in headers:
                col = headers.index(key) + 1
                col_letter = self._get_column_letter(col)
                batch_data.append({
                    'range': f'{col_letter}{row_number}',
                    'values': [[value]]
                })

        if batch_data:
            sheet.batch_update(batch_data)
            logger.info(f"✅ Обновлено {len(batch_data)} полей для строки {row_number}")

    @staticmethod
    def _get_column_letter(col_num):
        string = ""
        while col_num > 0:
            col_num, remainder = divmod(col_num - 1, 26)
            string = chr(65 + remainder) + string
        return string

repo = BikeRepository(cache_ttl=CACHE_TTL)

#================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================
def get_pages_by_chars(items, formatter):
    pages = []
    current_page = []
    current_len = 0
    for item in items:
        item_str = formatter(item)
        if current_len + len(item_str) > MAX_MSG_CHARS:
            if current_page:
                pages.append(current_page)
                current_page = [item]
                current_len = len(item_str)
            else:
                current_page.append(item)
                current_len += len(item_str)
        else:
            current_page.append(item)
            current_len += len(item_str)
    if current_page:
        pages.append(current_page)
    return pages if pages else [[]]

def get_nav_keyboard(total_pages, current_page, prefix, back_target, extra_buttons=None):
    kb = []
    if extra_buttons:
        kb.extend(extra_buttons)
    if total_pages > 1:
        nav_row = []
        if current_page > 0:
            nav_row.append(InlineKeyboardButton(
                text="⬅️ ",
                callback_data=f"{prefix}_page:{current_page-1}"
            ))
        nav_row.append(InlineKeyboardButton(
            text=f"{current_page + 1}/{total_pages}",
            callback_data="ignore"
        ))
        if current_page < total_pages - 1:
            nav_row.append(InlineKeyboardButton(
                text="➡️ ",
                callback_data=f"{prefix}_page:{current_page+1}"
            ))
        kb.append(nav_row)
    kb.append([InlineKeyboardButton(text="⬅ Назад", callback_data=back_target)])
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def delete_old_messages(state: FSMContext, chat_id: int):
    data = await state.get_data()
    msg_ids = data.get(MESSAGES_TO_DELETE_KEY, [])
    
    # Убираем дубликаты и None
    msg_ids = list(set([mid for mid in msg_ids if mid is not None]))
    
    deleted_count = 0
    for msg_id in msg_ids:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            deleted_count += 1
        except Exception as e:
            logger.debug(f"Не удалось удалить сообщение {msg_id}: {e}")
    
    # Очищаем список после попытки удаления
    await state.update_data({MESSAGES_TO_DELETE_KEY: []})
    
    if deleted_count > 0:
        logger.info(f"Удалено {deleted_count} старых сообщений")

async def show_step(message: Message, state: FSMContext, text: str,
                    reply_markup: InlineKeyboardMarkup | None = None):
    await delete_old_messages(state, message.chat.id)
    
    new_msg = await bot.send_message(
        chat_id=message.chat.id,
        text=text,
        reply_markup=reply_markup
    )
    
    data = await state.get_data()
    current_msgs = data.get(MESSAGES_TO_DELETE_KEY, [])
    if new_msg.message_id not in current_msgs:
        current_msgs.append(new_msg.message_id)
    
    await state.update_data({MESSAGES_TO_DELETE_KEY: current_msgs})

def format_full_info(row):
    text = (
        f"🏍 {row.get('МОДЕЛЬ', '-')} | `{row.get('Гос. номер', '-')}`\n"
        f"💰 Цена сутки: {row.get('Цена сутки', '-')} | Месяц: {row.get('Цена месяц', '-')}\n"
        f"🔐 Залог: {row.get('Залог $') or '0'}$ / {row.get('Залог VND') or '0'} VND\n"
    )
    if str(row.get('Статус', '')).strip() == 'Аренда':
        text += f"📅 Дата возврата: {row.get('Дата окончания аренды', '-')}\n"
    text += "--------------------------\n"
    return text

def parse_rental_term(text):
    text = text.lower().strip()
    month_match = re.search(r'(\d+)\s*(мес|month|месяц)', text)
    if month_match:
        num_months = int(month_match.group(1))
        return num_months, "monthly", num_months
    if text.isdigit():
        return int(text), "daily", 1
    return None, None, None

def calculate_return_fee(planned_end, now, price_day, price_month, rent_days):
    delta = now - planned_end
    if now.date() < planned_end.date():
        return 0, "✅ Сдано раньше срока.", 0
    if now.date() == planned_end.date():
        minutes = delta.total_seconds() / 60
        if minutes <= 30:
            return 0, "✅ Сдано вовремя.", 0
        if minutes <= 60:
            return 50000, "⏱ Просрочка до 1 часа.", 0
        if minutes <= 180:
            return int(price_day * 0.5), "⏱ Просрочка до 3 часов.", 0
        return price_day, "⏱ Просрочка более 3 часов.", 0
    days_overdue = math.ceil(delta.total_seconds() / 86400)
    if price_month < price_day * days_overdue:
        months = math.ceil(days_overdue / 30)
        return price_month * months, f"📅 Просрочка {days_overdue} дн. Оплата за {months} мес.", days_overdue
    return price_day * days_overdue, f"📅 Просрочка {days_overdue} дн. Оплата посуточно.", days_overdue


#================= FSM =================
class FSM(StatesGroup):
    menu = State()
    choose_brand = State()
    choose_bike = State()
    enter_days = State()
    enter_deposit_type = State()
    enter_deposit_currency = State()
    enter_deposit_other = State()
    enter_contact = State()
    verify_folder = State()
    upload_contract_photo = State()
    confirm_rent = State()
    return_choose_brand = State()
    return_choose_bike = State()
    return_wash = State()
    return_damage = State()
    return_confirm = State()
    extend_choose_brand = State()
    extend_choose_bike = State()
    extend_enter_term = State()
    extend_confirm = State()
    replace_choose_brand = State()
    replace_choose_rent_bike = State()
    replace_choose_base_bike = State()


#================= КЛАВИАТУРЫ =================
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Выдать байк", callback_data="rent")],
        [InlineKeyboardButton(text="Возврат байка", callback_data="return")],
        [InlineKeyboardButton(text="Продление аренды", callback_data="extend")],
        [InlineKeyboardButton(text="Замена байка", callback_data="replace_start")],
        [InlineKeyboardButton(text="Свободные байки", callback_data="free_bikes_list")],
        [InlineKeyboardButton(text="Отчёт", callback_data="report")],
    ])

def brands_keyboard(prefix):
    brands = ["Honda", "Kawasaki", "Suzuki", "SYM", "Yamaha"]
    kb = [[InlineKeyboardButton(text=b, callback_data=f"{prefix}:{b}")] for b in brands]
    kb.append([InlineKeyboardButton(text="Другие", callback_data=f"{prefix}:other")])
    kb.append([InlineKeyboardButton(text="⬅ Назад", callback_data="back:menu")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def back_keyboard(callback_data):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад", callback_data=callback_data)]
    ])


#================= ОБРАБОТЧИКИ СПИСКОВ =================
@dp.callback_query(F.data == "free_bikes_list")
@dp.callback_query(F.data.startswith("free_bikes_page:"))
@timing_decorator
async def show_all_free_bikes(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split(":")[1]) if ":" in callback.data else 0
    bikes = await repo.get_available_bikes()
    if not bikes:
        await callback.answer("Свободных байков нет", show_alert=True)
        return
    pages = get_pages_by_chars(bikes, format_full_info)
    if page >= len(pages):
        page = 0
    text = f"📋 Свободные байки (Стр. {page+1}/{len(pages)}):\n\n"
    for b in pages[page]:
        text += format_full_info(b)
    await show_step(
        callback.message,
        state,
        text,
        reply_markup=get_nav_keyboard(len(pages), page, "free_bikes", "back:menu")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith(("brand:", "rent_page:")), FSM.choose_brand)
@dp.callback_query(F.data.startswith("rent_page:"), FSM.choose_bike)
@timing_decorator
async def brand_selected(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "brand:" in callback.data:
        brand = callback.data.split(":")[1]
        page = 0
        await state.update_data(brand=brand)
    elif "page:" in callback.data:
        brand = data.get("brand")
        page = int(callback.data.split(":")[1])
    else:
        brand = data.get("brand")
        page = 0

    if not brand:
        await callback.answer("Ошибка данных. Выберите марку снова.", show_alert=True)
        await rent_start(callback, state)
        return

    bikes = await repo.get_bikes_by_brand(brand, status="База")
    if not bikes:
        await callback.answer("Нет доступных байков этой марки", show_alert=True)
        return

    pages = get_pages_by_chars(bikes, lambda x: format_full_info(x[1]))
    if page >= len(pages): page = 0
    current_data = pages[page]
    bike_buttons = [
        [InlineKeyboardButton(text=f"{r['МОДЕЛЬ']} | {r['Гос. номер']}", callback_data=f"rent_bike_sel:{i}")]
        for i, r in current_data
    ]
    text = f"🔍 Доступные {brand} (Стр. {page+1}/{len(pages)}):\n\n"
    for _, r in current_data:
        text += format_full_info(r)
    await show_step(callback.message, state, text,
                    reply_markup=get_nav_keyboard(len(pages), page, "rent", "back:rent_start", bike_buttons))
    await state.set_state(FSM.choose_bike)
    await callback.answer()

@dp.callback_query(F.data.startswith(("ret_brand:", "ext_brand:", "ret_page:", "ext_page:")))
@timing_decorator
async def rented_bike_pagination(callback: CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state not in [
        FSM.return_choose_brand, FSM.extend_choose_brand,
        FSM.return_choose_bike, FSM.extend_choose_bike
    ]:
        return

    fsm_data = await state.get_data()
    mode = "ret" if "ret_" in callback.data else "ext"
    if "brand" in callback.data:
        brand = callback.data.split(":")[1]
        page = 0
        await state.update_data(brand=brand)
    else:
        brand = fsm_data.get("brand")
        page = int(callback.data.split(":")[1])

    if not brand:
        await callback.answer("Сессия истекла. Выберите категорию заново.", show_alert=True)
        if mode == "ret":
            await return_start(callback, state)
        else:
            await extend_start(callback, state)
        return

    bikes = await repo.get_bikes_by_brand(brand, status="Аренда")
    if not bikes:
        await callback.answer("Нет байков этой модели в аренде", show_alert=True)
        return

    pages = get_pages_by_chars(bikes, lambda x: format_full_info(x[1]))
    if page >= len(pages):
        page = 0

    prefix = "ret_bike_sel" if mode == "ret" else "ext_bike_sel"
    back = "back:return_start" if mode == "ret" else "back:extend_start"
    bike_buttons = [
        [InlineKeyboardButton(
            text=f"{r['МОДЕЛЬ']} | {r['Гос. номер']}",
            callback_data=f"{prefix}:{i}"
        )]
        for i, r in pages[page]
    ]
    title = "Возврат" if mode == "ret" else "Продление"
    text = f"🔄 {title} {brand} (Стр. {page+1}/{len(pages)}):\n\n"
    for _, r in pages[page]:
        text += format_full_info(r)
    await show_step(
        callback.message,
        state,
        text,
        reply_markup=get_nav_keyboard(len(pages), page, mode, back, bike_buttons)
    )
    await state.set_state(FSM.return_choose_bike if mode == "ret" else FSM.extend_choose_bike)
    await callback.answer()


#================= ОБРАБОТЧИКИ ДЕЙСТВИЙ =================
@dp.message(F.text == "/start")
async def start(message: Message, state: FSMContext):
    await delete_old_messages(state, message.chat.id)
    await state.clear()
    await state.set_state(FSM.menu)
    msg = await message.answer("Выберите действие: ", reply_markup=main_menu())
    await state.update_data({MESSAGES_TO_DELETE_KEY: [msg.message_id]})

@dp.callback_query(F.data == "rent")
async def rent_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(FSM.choose_brand)
    await show_step(
        callback.message,
        state,
        "Выберите модель: ",
        reply_markup=brands_keyboard("brand")
    )
    await callback.answer()

@dp.callback_query(F.data == "return")
async def return_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(FSM.return_choose_brand)
    await show_step(
        callback.message,
        state,
        "Возврат. Выберите модель: ",
        reply_markup=brands_keyboard("ret_brand")
    )
    await callback.answer()

@dp.callback_query(F.data == "extend")
async def extend_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(FSM.extend_choose_brand)
    await show_step(
        callback.message,
        state,
        "Продление. Выберите модель: ",
        reply_markup=brands_keyboard("ext_brand")
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("rent_bike_sel:"), FSM.choose_bike)
async def rent_bike_selected(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if callback.data.startswith("rent_bike_sel:"):
        row = int(callback.data.split(":")[1])
        await state.update_data(row=row)
    else:
        row = data.get("row")

    if not row:
        await callback.answer("Ошибка: байк не выбран", show_alert=True)
        return

    await show_step(
        callback.message,
        state,
        "Введите срок аренды (дней или 'N месяцев'):",
        reply_markup=back_keyboard("back:to_bike_list")
    )
    await state.set_state(FSM.enter_days)
    await callback.answer()

@dp.message(FSM.enter_days)
@timing_decorator
async def days_entered(message: Message, state: FSMContext):
    data = await state.get_data()
    msgs_to_del = data.get(MESSAGES_TO_DELETE_KEY, [])
    msgs_to_del.append(message.message_id)
    await state.update_data({MESSAGES_TO_DELETE_KEY: msgs_to_del})

    days, p_type, c_months = parse_rental_term(message.text)
    if days is None:
        error_msg = await message.answer("❌ Ошибка! Введите число дней или '1 месяц', '2 месяца' и т.д.")
        msgs_to_del.append(error_msg.message_id)
        await state.update_data({MESSAGES_TO_DELETE_KEY: msgs_to_del})
        return

    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]

    if p_type == "monthly":
        total = int(bike.get("Цена месяц") or 0) * c_months
        start_date = datetime.now()
        end_date = start_date + relativedelta(months=c_months)
        actual_days = (end_date - start_date).days
        await state.update_data(days=actual_days, sum=total, months_count=c_months)
    else:
        total = int(bike.get("Цена сутки") or 0) * days
        await state.update_data(days=days, sum=total, months_count=None)

    days_to_show = actual_days if p_type == "monthly" else days

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="$", callback_data="dep:usd"),
            InlineKeyboardButton(text="VND", callback_data="dep:vnd")
        ],
        [InlineKeyboardButton(text="Другое", callback_data="dep:other")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_rent_days")]
    ])
    await show_step(
        message,
        state,
        f"Срок: {days_to_show} дн. Сумма: {total} VND\nВыберите тип депозита: ",
        reply_markup=kb
    )
    await state.set_state(FSM.enter_deposit_type)

@dp.callback_query(F.data.startswith("dep:"), FSM.enter_deposit_type)
@timing_decorator
async def deposit_selected(callback: CallbackQuery, state: FSMContext):
    dt = callback.data.split(":")[1]
    data = await state.get_data()

    if dt == "usd":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="New", callback_data="usd:new"),
                InlineKeyboardButton(text="Old", callback_data="usd:old")
            ],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_dep_type")]
        ])
        await show_step(callback.message, state, "Состояние $: ", reply_markup=kb)
        await state.set_state(FSM.enter_deposit_currency)
    elif dt == "vnd":
        sheet, rows = await repo.get_all()
        dep = rows[data["row"] - 2].get("Залог VND", "0")
        await state.update_data(deposit=f"{dep} VND")
        await rent_to_contact(callback.message, state)
    else:
        await show_step(
            callback.message,
            state,
            "Введите залог текстом:",
            reply_markup=back_keyboard("back:to_dep_type")
        )
        await state.set_state(FSM.enter_deposit_other)
    await callback.answer()

@dp.callback_query(F.data.startswith("usd:"), FSM.enter_deposit_currency)
@timing_decorator
async def usd_condition(callback: CallbackQuery, state: FSMContext):
    cond = callback.data.split(":")[1]
    data = await state.get_data()
    sheet, rows = await repo.get_all()
    amt = rows[data["row"] - 2].get("Залог $") or "0"
    await state.update_data(deposit=f"{amt}$ {cond}")
    await rent_to_contact(callback.message, state)
    await callback.answer()

@dp.message(FSM.enter_deposit_other)
async def dep_other(message: Message, state: FSMContext):
    data = await state.get_data()
    msgs_to_del = data.get(MESSAGES_TO_DELETE_KEY, [])
    msgs_to_del.append(message.message_id)
    await state.update_data({MESSAGES_TO_DELETE_KEY: msgs_to_del})
    await state.update_data(deposit=message.text)
    await rent_to_contact(message, state)

async def rent_to_contact(message: Message, state: FSMContext):
    await show_step(
        message,
        state,
        "Введите контакт клиента (Телефон/WA/TG):",
        reply_markup=back_keyboard("back:to_dep_type")
    )
    await state.set_state(FSM.enter_contact)

@dp.message(FSM.enter_contact, F.text)
@timing_decorator
async def contact_in(message: Message, state: FSMContext):
    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"Не удалось удалить сообщение с контактом: {e}")

    await state.update_data(contact=message.text)

    data = await state.get_data()
    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]
    end = datetime.now() + timedelta(days=data["days"])

    text = (
        f"Проверка:\n"
        f"🏍 {bike['МОДЕЛЬ']}\n"
        f"🔢 {bike['Гос. номер']}\n"
        f"📅 {data['days']} дн.\n"
        f"💰 {data['sum']} VND\n"
        f"🔐 {data['deposit']}\n"
        f"📞 {data['contact']}\n"
        f"⏳ До: {end.strftime('%d.%m.%Y')}"
    )

    folder_name = f"{bike['МОДЕЛЬ']} {bike['Гос. номер']}"

    # Проверяем/создаём папку (не блокируем процесс при ошибке)
    folder_id = await asyncio.to_thread(
        get_or_create_folder_for_bike,
        folder_name=folder_name
    )

    await delete_old_messages(state, message.chat.id)

    if folder_id:
        folder_url = f"https://drive.google.com/drive/folders/{folder_id}"
        msg = await message.answer(
            text + "\n\n📂 Папка для байка: " + folder_name + "\n"
            f"Загрузите видео в эту папку: {folder_url}\n\n"
            "После загрузки видео нажмите кнопку ниже.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Я загрузил видео, продолжить", callback_data="bike_folder_confirmed")],
                [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_contact")]
            ]),
            disable_web_page_preview=False
        )
        await state.update_data(bike_folder_id=folder_id, MESSAGES_TO_DELETE_KEY=[msg.message_id])
    else:
        # Если папка не создана, продолжаем без неё
        logger.warning(f"Не удалось создать папку для {folder_name}, продолжаем без неё")
        msg = await message.answer(
            text + "\n\n⚠️ Не удалось создать папку для байка. Продолжите выдачу.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Продолжить без папки", callback_data="bike_folder_confirmed")],
                [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_contact")]
            ]),
            disable_web_page_preview=True
        )
        await state.update_data(bike_folder_id=None, MESSAGES_TO_DELETE_KEY=[msg.message_id])
    await state.set_state(FSM.verify_folder)

@dp.message(FSM.upload_contract_photo, F.photo)
@timing_decorator
async def contract_in(message: Message, state: FSMContext):
    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"Не удалось удалить сообщение с фото: {e}")

    data = await state.get_data()
    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]
    contract_folder_id = data.get("contract_folder_id")  # Берем сохранённый ID, если есть

    file = await bot.get_file(message.photo[-1].file_id)
    file_bytes_io = await bot.download_file(file.file_path)
    file_bytes = file_bytes_io.read()  # Преобразовать BytesIO в bytes

    logger.info(f"Скачано фото: размер {len(file_bytes)} байт")
    if len(file_bytes) == 0:
        logger.error("Фото не скачано: пустой файл")
        error_msg = await message.answer("❌ Фото не скачано. Попробуйте снова.")
        msgs_to_del.append(error_msg.message_id)
        await state.update_data({MESSAGES_TO_DELETE_KEY: msgs_to_del})
        return

    now = datetime.now()
    timestamp = now.strftime("%d.%m.%Y %H:%M")
    folder_name = f"{bike['МОДЕЛЬ']} {bike['Гос. номер']}"
    filename = f"{bike['МОДЕЛЬ']}, {bike['Гос. номер']}, {timestamp}.jpg"

    new_folder_id = await asyncio.to_thread(
        upload_contract_photo,
        file_bytes=file_bytes,
        filename=filename,
        folder_name=folder_name,
        folder_id=contract_folder_id  # Передаём существующий ID
    )

    if new_folder_id:
        # Если папка была создана впервые — сохраняем ID
        if not contract_folder_id:
            await state.update_data(contract_folder_id=new_folder_id)
            logger.info(f"Сохранён ID папки: {new_folder_id}")
    else:
        logger.error("Не удалось загрузить фото")
        error_msg = await message.answer("❌ Ошибка загрузки фото. Попробуйте снова.")
        msgs_to_del.append(error_msg.message_id)
        await state.update_data({MESSAGES_TO_DELETE_KEY: msgs_to_del})
        return

    end = datetime.now() + timedelta(days=data["days"])
    confirm_text = (
        f"⚠️ ПРОВЕРЬТЕ ДАННЫЕ ПЕРЕД ПОДТВЕРЖДЕНИЕМ:\n\n"
        f"🏍 Байк: {bike['МОДЕЛЬ']}\n"
        f"🔢 Гос. номер: {bike['Гос. номер']}\n"
        f"📅 Срок: {data['days']} дн.\n"
        f"💰 Сумма: {data['sum']} VND\n"
        f"🔐 Залог: {data['deposit']}\n"
        f"📞 Контакт: {data['contact']}\n"
        f"⏳ Дата окончания: {end.strftime('%d.%m.%Y')}\n"
        f"Всё верно?"
    )
    await delete_old_messages(state, message.chat.id)
    confirm_msg = await message.answer(confirm_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить выдачу", callback_data="rent_final")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_photo")]
    ]))
    await state.update_data({MESSAGES_TO_DELETE_KEY: [confirm_msg.message_id]})
    await state.set_state(FSM.confirm_rent)

@dp.message(FSM.upload_contract_photo)
async def contract_error(message: Message, state: FSMContext):
    data = await state.get_data()
    msgs_to_del = data.get(MESSAGES_TO_DELETE_KEY, [])
    msgs_to_del.append(message.message_id)
    await state.update_data({MESSAGES_TO_DELETE_KEY: msgs_to_del})
    error_msg = await message.answer("😐 Всё нормально? Нужно ФОТО договора.")
    msgs_to_del.append(error_msg.message_id)
    await state.update_data({MESSAGES_TO_DELETE_KEY: msgs_to_del})

@dp.callback_query(F.data == "rent_final", FSM.confirm_rent)
@timing_decorator
async def rent_final_done(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]

    model_clean = str(bike['МОДЕЛЬ']).strip()
    plate_clean = str(bike['Гос. номер']).strip()

    wait_msg = await callback.message.answer("⏳ Обновляю таблицу... Пожалуйста, не нажимайте ничего.")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
        await repo.update_bike(data["row"], {
            "Срок аренды": data["days"],
            "Сумма": data["sum"],
            "Депозит": data["deposit"],
            "Контакт клиента": data["contact"],
            "Статус": "Аренда",
            "Дата начала аренды": datetime.now().strftime("%d.%m.%Y %H:%M")
        })

        # Обновляем отчётность
        try:
            update_reports(int(data["sum"]))
        except Exception as report_err:
            logger.error(f"Ошибка при обновлении отчёта: {report_err}")

        _, updated_rows = await repo.get_all(force_refresh=True)
        updated_bike = updated_rows[data["row"] - 2]
        end_date = updated_bike.get('Дата окончания аренды', '-')

        result_text = (
            f"✅ <b>Байк выдан!</b>\n\n"
            f"🏍 {model_clean}\n"
            f"🔢 {plate_clean}\n"
            f"💰 Оплачено: {data['sum']} VND\n"
            f"🔐 Залог: {data['deposit']}\n"
            f"📞 Контакт: {data['contact']}\n"
            f"📅 Дата окончания: {end_date}"
        )

        await wait_msg.delete()
        await delete_old_messages(state, callback.message.chat.id)
        result_msg = await callback.message.answer(result_text, reply_markup=main_menu())
        await state.set_state(FSM.menu)
        # Очищаем список сообщений для удаления, чтобы не удалить финальное
        await state.update_data({MESSAGES_TO_DELETE_KEY: []})
    except Exception as e:
        logger.error(f"Ошибка при финальной выдаче: {e}")
        await wait_msg.edit_text(
            "❌ Ошибка при обновлении. Попробуйте нажать кнопку еще раз.\n"
            "Если ошибка повторится, проверьте интернет."
        )
        await callback.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Попробовать снова", callback_data="rent_final")]
            ])
        )


#================= ЛОГИКА ВОЗВРАТА И ПРОДЛЕНИЯ =================
@dp.callback_query(F.data.startswith("ret_bike_sel:"), FSM.return_choose_bike)
@timing_decorator
async def ret_selected(callback: CallbackQuery, state: FSMContext):
    """Выбран байк для возврата"""
    row = int(callback.data.split(":")[1])
    await state.update_data(row=row)

    sheet, rows = await repo.get_all()
    bike = rows[row - 2]

    p_end_raw = bike.get("Дата окончания аренды")
    if not p_end_raw:
        await callback.answer("❌ Нет даты окончания!", show_alert=True)
        return

    p_end = datetime.strptime(p_end_raw, "%d.%m.%Y %H:%M")
    fee, info, days_late = calculate_return_fee(
        p_end,
        datetime.now(),
        int(bike.get("Цена сутки") or 0),
        int(bike.get("Цена месяц") or 0),
        int(bike.get("Срок аренды") or 0)
    )

    await state.update_data(overdue_fee=fee, days_late=days_late)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🧼 Да (50к)", callback_data="wash:yes"),
            InlineKeyboardButton(text="❌ Нет", callback_data="wash:no")
        ],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_bike_list")]
    ])
    await show_step(
        callback.message,
        state,
        f"{info}\nДоплата: {fee} VND. Нужна мойка?",
        reply_markup=kb
    )
    await state.set_state(FSM.return_wash)
    await callback.answer()

@dp.callback_query(F.data.startswith("wash:"), FSM.return_wash)
@timing_decorator
async def ret_wash(callback: CallbackQuery, state: FSMContext):
    wf = 50000 if "yes" in callback.data else 0
    data = await state.get_data()
    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]

    await state.update_data(wash_fee=wf)

    folder_name = f"{bike['МОДЕЛЬ']} {bike['Гос. номер']}"

    folder_id = await asyncio.to_thread(
        check_folder_exists,
        folder_name=folder_name
    )

    if folder_id:
        folder_url = f"https://drive.google.com/drive/folders/{folder_id}"
        text = (
            f"Доплата: {data['overdue_fee'] + wf} VND.\n\n"
            f"{folder_url}\n\n"
            f"Сравните с видео осмотра, есть повреждения?"
        )
    else:
        text = (
            f"Доплата: {data['overdue_fee'] + wf} VND.\n\n"
            f"Сравните с видео осмотра, есть повреждения?"
        )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Нет", callback_data="dmg:no"),
            InlineKeyboardButton(text="🛠 Да", callback_data="dmg:yes")
        ],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_wash")]
    ])

    await show_step(
        callback.message,
        state,
        text,
        reply_markup=kb
    )

    await state.set_state(FSM.return_damage)
    await callback.answer()

@dp.callback_query(F.data == "back")
async def go_back(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    prev_state = data.get("prev_state")

    if not prev_state:
        await callback.answer("Назад невозможно", show_alert=True)
        return

    await state.set_state(prev_state)

    await callback.message.edit_text(
    )

    await callback.answer()

@dp.callback_query(F.data == "dmg:yes", FSM.return_damage)
async def ret_damage_yes(callback: CallbackQuery, state: FSMContext):
    """Обработка повреждений"""
    await callback.answer(
        "⚠️ При наличии повреждений байк нельзя принять автоматически. "
        "Обратитесь к менеджеру для оценки ущерба.",
        show_alert=True
    )

@dp.callback_query(F.data == "dmg:no", FSM.return_damage)
@timing_decorator
async def ret_confirm_view(callback: CallbackQuery, state: FSMContext):
    """Подтверждение возврата"""
    data = await state.get_data()
    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]

    total_fee = data['overdue_fee'] + data.get('wash_fee', 0)
    days_late = data.get('days_late', 0)
    deposit = bike.get('Депозит', '-')

    confirm_text = (
        f"⚠️ ПРОВЕРЬТЕ ДАННЫЕ ВОЗВРАТА:\n\n"
        f"🏍 Байк: {bike['МОДЕЛЬ']}\n"
        f"🔢 Гос. номер: {bike['Гос. номер']}\n"
        f"📞 Контакт: {bike.get('Контакт клиента', '-')}\n\n"
    )
    if days_late > 0:
        confirm_text += f"⏰ Просрочка: {days_late} дн.\n"
    if total_fee > 0:
        confirm_text += (
            f"💵  ВЗЯТЬ С КЛИЕНТА:\n"
            f"   • Просрочка: {data['overdue_fee']} VND\n"
        )
        if data.get('wash_fee', 0) > 0:
            confirm_text += f"   • Мойка: {data['wash_fee']} VND\n"
        confirm_text += f"   •  ИТОГО: {total_fee} VND\n\n"
    else:
        confirm_text += "✅ Доплаты не требуется\n\n"

    confirm_text += (
        f"🔐 ВЕРНУТЬ ЗАЛОГ: {deposit}\n\n"
        f"Принять возврат?"
    )

    await show_step(
        callback.message,
        state,
        confirm_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить возврат", callback_data="conf_ret")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_damage")]
        ])
    )
    current_state = await state.get_state()
    await state.update_data(prev_state=current_state)
    await state.set_state(FSM.return_confirm)
    await callback.answer()

@dp.callback_query(F.data == "conf_ret", FSM.return_confirm)
@timing_decorator
async def ret_done_final(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]

    await repo.update_bike(data["row"], {"Статус": "База"})

    total_fee = data['overdue_fee'] + data.get('wash_fee', 0)
    result_text = (
        f"✅ Возврат принят!\n\n"
        f"🏍 {bike['МОДЕЛЬ']} | {bike['Гос. номер']}\n"
    )
    if total_fee > 0:
        result_text += f"💵 Получено с клиента: {total_fee} VND\n"
    result_text += f"🔐 Возвращён залог: {bike.get('Депозит', '-')}"
    await delete_old_messages(state, callback.message.chat.id)
    result_msg = await callback.message.answer(result_text, reply_markup=main_menu())
    current_state = await state.get_state()
    await state.update_data(prev_state=current_state)
    await state.set_state(FSM.menu)
    await state.update_data({MESSAGES_TO_DELETE_KEY: []})

@dp.callback_query(F.data.startswith("ext_bike_sel:"), FSM.extend_choose_bike)
async def ext_selected(callback: CallbackQuery, state: FSMContext):
    row = int(callback.data.split(":")[1])
    await state.update_data(row=row)
    await show_step(
        callback.message,
        state,
        "Срок продления (дней или 'N месяцев'):",
        reply_markup=back_keyboard("back:to_bike_list")
    )
    current_state = await state.get_state()
    await state.update_data(prev_state=current_state)
    await state.set_state(FSM.extend_enter_term)
    await callback.answer()

@dp.message(FSM.extend_enter_term)
@timing_decorator
async def ext_term_in(message: Message, state: FSMContext):
    try:
        await message.delete()
    except Exception:
        pass
    data = await state.get_data()
    msgs_to_del = data.get(MESSAGES_TO_DELETE_KEY, [])
    msgs_to_del.append(message.message_id)
    await state.update_data({MESSAGES_TO_DELETE_KEY: msgs_to_del})

    days, p_type, c_months = parse_rental_term(message.text)
    if days is None:
        error_msg = await message.answer("❌ Введите корректный срок (например: '5 дней' или '1 месяц')!")
        msgs_to_del.append(error_msg.message_id)
        await state.update_data({MESSAGES_TO_DELETE_KEY: msgs_to_del})
        return

    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]

    if p_type == "monthly":
        total = int(bike.get("Цена месяц") or 0) * c_months
        current_end_raw = bike.get("Дата окончания аренды", "")
        if current_end_raw:
            current_end = datetime.strptime(current_end_raw, "%d.%m.%Y %H:%M")
            new_end = current_end + relativedelta(months=c_months)
            ext_days = (new_end - current_end).days
        else:
            start_date = datetime.now()
            new_end = start_date + relativedelta(months=c_months)
            ext_days = (new_end - start_date).days
        await state.update_data(ext_days=ext_days, ext_sum=total)
    else:
        total = int(bike.get("Цена сутки") or 0) * days
        await state.update_data(ext_days=days, ext_sum=total)

    await show_step(
        message,
        state,
        f"Продление: {bike['МОДЕЛЬ']}\n"
        f"➕ Срок: {message.text}\n"
        f"💰  К оплате: {total} VND",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data="ext_done")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="back:extend_bikes")]
        ])
    )
    current_state = await state.get_state()
    await state.update_data(prev_state=current_state)
    await state.set_state(FSM.extend_confirm)

@dp.callback_query(F.data == "ext_done", FSM.extend_confirm)
@timing_decorator
async def ext_final_done(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]

    new_term = int(bike.get("Срок аренды", 0) or 0) + data["ext_days"]
    current_sum = int(bike.get("Сумма", 0) or 0)
    new_sum = current_sum + data["ext_sum"]

    await repo.update_bike(data["row"], {
        "Срок аренды": new_term,
        "Сумма": new_sum
    })
    
    # Обновляем отчётность (только суммы, без увеличения количества выдач)
    try:
        update_reports_extend(int(data["ext_sum"]))
    except Exception as report_err:
        logger.error(f"Ошибка при обновлении отчёта: {report_err}")
    
    _, updated_rows = await repo.get_all(force_refresh=True)
    updated_bike = updated_rows[data["row"] - 2]
    new_end = updated_bike.get("Дата окончания аренды", "Не указана")

    result_text = (
        f"✅  Продлено!\n\n"
        f"🏍 {bike['МОДЕЛЬ']}\n"
        f"🔢 {bike['Гос. номер']}\n"
        f"📞 Контакт: {bike.get('Контакт клиента', '-')}\n"
        f"💰 Доплата: {data['ext_sum']} VND\n"
        f"💵 Общая сумма: {new_sum} VND\n"
        f"📅 Новая дата окончания: {new_end}"
    )
    await delete_old_messages(state, callback.message.chat.id)
    result_msg = await callback.message.answer(result_text, reply_markup=main_menu())
    current_state = await state.get_state()
    await state.update_data(prev_state=current_state)
    await state.set_state(FSM.menu)
    await state.update_data({MESSAGES_TO_DELETE_KEY: []})


#================= ОБРАБОТЧИКИ "НАЗАД" =================
@dp.callback_query(F.data == "back:to_bike_list", FSM.enter_days)
async def back_to_bike_list_from_days(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору байка из этапа ввода срока аренды"""
    data = await state.get_data()
    brand = data.get("brand")
    if not brand:
        await rent_start(callback, state)
        return

    bikes = await repo.get_bikes_by_brand(brand, status="База")
    if not bikes:
        await callback.answer("Нет доступных байков этой марки", show_alert=True)
        return

    pages = get_pages_by_chars(bikes, lambda x: format_full_info(x[1]))
    page = 0
    current_data = pages[page]
    bike_buttons = [
        [InlineKeyboardButton(text=f"{r['МОДЕЛЬ']} | {r['Гос. номер']}", callback_data=f"rent_bike_sel:{i}")]
        for i, r in current_data
    ]
    text = f"🔍 Доступные {brand} (Стр. {page+1}/{len(pages)}):\n\n"
    for _, r in current_data:
        text += format_full_info(r)
    await show_step(
        callback.message,
        state,
        text,
        reply_markup=get_nav_keyboard(len(pages), page, "rent", "back:rent_start", bike_buttons)
    )
    current_state = await state.get_state()
    await state.update_data(prev_state=current_state)
    await state.set_state(FSM.choose_bike)
    await callback.answer()

@dp.callback_query(F.data == "back:extend_bikes", FSM.extend_enter_term)
async def back_to_extend_bikes(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору байка для продления"""
    data = await state.get_data()
    brand = data.get("brand")
    if not brand:
        await extend_start(callback, state)
        return

    bikes = await repo.get_bikes_by_brand(brand, status="Аренда")
    if not bikes:
        await callback.answer("Нет байков этой модели в аренде", show_alert=True)
        return

    pages = get_pages_by_chars(bikes, lambda x: format_full_info(x[1]))
    page = 0
    current_data = pages[page]
    bike_buttons = [
        [InlineKeyboardButton(text=f"{r['МОДЕЛЬ']} | {r['Гос. номер']}", callback_data=f"ext_bike_sel:{i}")]
        for i, r in current_data
    ]
    text = f"🔄 Продление {brand} (Стр. {page+1}/{len(pages)}):\n\n"
    for _, r in current_data:
        text += format_full_info(r)
    await show_step(
        callback.message,
        state,
        text,
        reply_markup=get_nav_keyboard(len(pages), page, "ext", "back:extend_start", bike_buttons)
    )
    current_state = await state.get_state()
    await state.update_data(prev_state=current_state)
    await state.set_state(FSM.extend_choose_bike)
    await callback.answer()

@dp.callback_query(F.data == "back:menu")
async def b_menu(callback: CallbackQuery, state: FSMContext):
    await start(callback.message, state)
    await callback.answer()

@dp.callback_query(F.data == "back:rent_start")
async def b_rent(callback: CallbackQuery, state: FSMContext):
    await rent_start(callback, state)

@dp.callback_query(F.data == "back:return_start")
async def b_ret(callback: CallbackQuery, state: FSMContext):
    await return_start(callback, state)

@dp.callback_query(F.data == "back:extend_start")
async def b_ext(callback: CallbackQuery, state: FSMContext):
    await extend_start(callback, state)

@dp.callback_query(F.data == "bike_folder_confirmed")
@timing_decorator
async def bike_folder_confirmed(callback: CallbackQuery, state: FSMContext):
    await delete_old_messages(state, callback.message.chat.id)
    
    # Удаляем само сообщение с кнопкой
    try:
        await callback.message.delete()
    except Exception as e:
        logger.debug(f"Не удалось удалить сообщение с кнопкой: {e}")

    new_msg = await bot.send_message(
        chat_id=callback.message.chat.id,
        text="Отлично! Теперь 📄 Загрузите фото договора (можно несколько).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_verification")]
        ])
    )

    await state.update_data({MESSAGES_TO_DELETE_KEY: [new_msg.message_id]})
    await state.set_state(FSM.upload_contract_photo)
    await callback.answer()


#================= ОТЧЁТ =================
@dp.callback_query(F.data == "report")
async def show_report(callback: CallbackQuery, state: FSMContext):
    """Показывает отчёт за текущий день"""
    try:
        from sheets import get_reports_sheet
        from datetime import datetime
        
        sheet = get_reports_sheet()
        all_data = sheet.get_all_values()
        
        today = datetime.now().strftime("%d.%m.%Y")
        
        # Находим заголовки
        headers = all_data[0] if all_data else []
        date_col = sum_col = count_col = None
        
        for i, header in enumerate(headers):
            header_lower = header.lower().strip()
            if "дата" in header_lower:
                date_col = i
            elif "сумма выдачи" in header_lower:
                sum_col = i
            elif "количество выдач" in header_lower and "месяц" not in header_lower:
                count_col = i
        
        # Ищем данные за сегодня
        today_sum = 0
        today_count = 0
        
        for row in all_data[1:]:
            if len(row) > date_col and row[date_col] == today:
                if sum_col is not None and len(row) > sum_col:
                    today_sum = int(row[sum_col] or 0)
                if count_col is not None and len(row) > count_col:
                    today_count = int(row[count_col] or 0)
                break
        
        report_text = (
            f"📊 <b>ОТЧЁТ</b>\n\n"
            f"📅 <b>Сегодня ({today})</b>\n"
            f"💰 Сумма выдачи: {today_sum:,} VND\n"
            f"🔢 Количество выдач: {today_count}"
        )
        
        await delete_old_messages(state, callback.message.chat.id)
        report_msg = await callback.message.answer(
            report_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅ Назад", callback_data="back:menu")]
            ])
        )
        await state.update_data({MESSAGES_TO_DELETE_KEY: [report_msg.message_id]})
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка при формировании отчёта: {e}")
        await callback.answer(f"Ошибка: {e}", show_alert=True)


#================= ЛОГИКА ЗАМЕНЫ БАЙКА =================
@dp.callback_query(F.data == "replace_start")
async def replace_start(callback: CallbackQuery, state: FSMContext):
    """Начало замены байка - выбор марки байка в аренде"""
    await state.set_state(FSM.replace_choose_brand)
    await show_step(
        callback.message,
        state,
        "Замена байка. Выберите модель байка в аренде:",
        reply_markup=brands_keyboard("rep_rent_brand")
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("rep_rent_brand:"), FSM.replace_choose_brand)
async def replace_rent_brand_selected(callback: CallbackQuery, state: FSMContext):
    """После выбора бренда - показ байков в аренде этой марки"""
    brand = callback.data.split(":")[1]
    await state.update_data(rent_brand=brand)
    
    # Используем get_bikes_by_brand для всех брендов, включая "Другие"
    rent_bikes = await repo.get_bikes_by_brand(brand, status="Аренда")
    
    if not rent_bikes:
        await callback.answer("Нет байков этой модели в аренде", show_alert=True)
        return
    
    # Сохраняем список целиком
    await state.update_data(rent_bikes=rent_bikes)
    
    pages = get_pages_by_chars(rent_bikes, lambda x: format_full_info(x[1]))
    page = 0
    current_data = pages[page]
    bike_buttons = [
        [InlineKeyboardButton(text=f"{r['МОДЕЛЬ']} | {r['Гос. номер']}", callback_data=f"rep_rent_sel:{i}")]
        for i, (row, r) in enumerate(current_data)
    ]
    
    text = f"🔄 Замена байка. Выберите байк в аренде (Стр. {page+1}/{len(pages)}):\n\n"
    for row, r in current_data:
        text += format_full_info(r)
    
    await show_step(
        callback.message,
        state,
        text,
        reply_markup=get_nav_keyboard(len(pages), page, "rep_rent", "back:to_rent_brand", bike_buttons)
    )
    await state.set_state(FSM.replace_choose_rent_bike)
    await callback.answer()


@dp.callback_query(F.data.startswith("rep_rent_page:"))
async def replace_rent_page(callback: CallbackQuery, state: FSMContext):
    """Пагинация списка байков в аренде"""
    page = int(callback.data.split(":")[1])
    data = await state.get_data()
    rent_bikes = data.get("rent_bikes")
    
    if not rent_bikes:
        await replace_start(callback, state)
        return
    
    pages = get_pages_by_chars(rent_bikes, lambda x: format_full_info(x[1]))
    if page >= len(pages):
        page = 0
    
    current_data = pages[page]
    bike_buttons = [
        [InlineKeyboardButton(text=f"{r['МОДЕЛЬ']} | {r['Гос. номер']}", callback_data=f"rep_rent_sel:{i}")]
        for i, (row, r) in enumerate(current_data)
    ]
    
    text = f"🔄 Замена байка. Выберите байк в аренде (Стр. {page+1}/{len(pages)}):\n\n"
    for row, r in current_data:
        text += format_full_info(r)
    
    await show_step(
        callback.message,
        state,
        text,
        reply_markup=get_nav_keyboard(len(pages), page, "rep_rent", "back:to_rent_brand", bike_buttons)
    )
    await callback.answer()



# --- Дополнительные обработчики "Назад" ---
@dp.callback_query(F.data == "back:to_dep_type", FSM.enter_contact)
async def back_to_dep_type_from_contact(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору типа депозита из ввода контакта"""
    data = await state.get_data()
    days_to_show = data.get("days", 0)
    total = data.get("sum", 0)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="$", callback_data="dep:usd"),
            InlineKeyboardButton(text="VND", callback_data="dep:vnd")
        ],
        [InlineKeyboardButton(text="Другое", callback_data="dep:other")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_rent_days")]
    ])
    await show_step(
        callback.message,
        state,
        f"Срок: {days_to_show} дн. Сумма: {total} VND\nВыберите тип депозита: ",
        reply_markup=kb
    )
    await state.set_state(FSM.enter_deposit_type)
    await callback.answer()


@dp.callback_query(F.data == "back:to_contact", FSM.verify_folder)
async def back_to_contact_from_verify(callback: CallbackQuery, state: FSMContext):
    """Возврат к вводу контакта из этапа верификации папки"""
    await show_step(
        callback.message,
        state,
        "Введите контакт клиента (Телефон/WA/TG):",
        reply_markup=back_keyboard("back:to_dep_type")
    )
    await state.set_state(FSM.enter_contact)
    await callback.answer()


@dp.callback_query(F.data == "back:to_rent_days", FSM.enter_deposit_type)
async def back_to_days_from_deposit(callback: CallbackQuery, state: FSMContext):
    """Возврат к вводу срока аренды из выбора типа депозита"""
    data = await state.get_data()
    row = data.get("row")
    if not row:
        await callback.answer("Ошибка данных", show_alert=True)
        return
    
    await show_step(
        callback.message,
        state,
        "Введите срок аренды (дней или 'N месяцев'):",
        reply_markup=back_keyboard("back:to_bike_list")
    )
    await state.set_state(FSM.enter_days)
    await callback.answer()



@dp.callback_query(F.data == "back:to_dep_type", FSM.enter_deposit_currency)
async def back_to_dep_type_from_currency(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору типа депозита из выбора валюты"""
    data = await state.get_data()
    days_to_show = data.get("days", 0)
    total = data.get("sum", 0)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="$", callback_data="dep:usd"),
            InlineKeyboardButton(text="VND", callback_data="dep:vnd")
        ],
        [InlineKeyboardButton(text="Другое", callback_data="dep:other")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_rent_days")]
    ])
    await show_step(
        callback.message,
        state,
        f"Срок: {days_to_show} дн. Сумма: {total} VND\nВыберите тип депозита: ",
        reply_markup=kb
    )
    await state.set_state(FSM.enter_deposit_type)
    await callback.answer()


@dp.callback_query(F.data == "back:to_dep_type", FSM.enter_deposit_other)
async def back_to_dep_type_from_other(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору типа депозита из ввода другого депозита"""
    data = await state.get_data()
    days_to_show = data.get("days", 0)
    total = data.get("sum", 0)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="$", callback_data="dep:usd"),
            InlineKeyboardButton(text="VND", callback_data="dep:vnd")
        ],
        [InlineKeyboardButton(text="Другое", callback_data="dep:other")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_rent_days")]
    ])
    await show_step(
        callback.message,
        state,
        f"Срок: {days_to_show} дн. Сумма: {total} VND\nВыберите тип депозита: ",
        reply_markup=kb
    )
    await state.set_state(FSM.enter_deposit_type)
    await callback.answer()


@dp.callback_query(F.data == "back:to_verification", FSM.upload_contract_photo)
async def back_to_verification_from_photo(callback: CallbackQuery, state: FSMContext):
    """Возврат к этапу проверки (папка байка) из загрузки фото"""
    data = await state.get_data()
    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]
    end = datetime.now() + timedelta(days=data["days"])

    text = (
        f"Проверка:\n"
        f"🏍 {bike['МОДЕЛЬ']}\n"
        f"🔢 {bike['Гос. номер']}\n"
        f"📅 {data['days']} дн.\n"
        f"💰 {data['sum']} VND\n"
        f"🔐 {data['deposit']}\n"
        f"📞 {data['contact']}\n"
        f"⏳ До: {end.strftime('%d.%m.%Y')}"
    )

    folder_name = f"{bike['МОДЕЛЬ']} {bike['Гос. номер']}"

    if data.get("bike_folder_id"):
        folder_url = f"https://drive.google.com/drive/folders/{data['bike_folder_id']}"
        await show_step(
            callback.message,
            state,
            text + "\n\n📂 Папка для байка: " + folder_name + "\n"
            f"Загрузите видео в эту папку: {folder_url}\n\n"
            "После загрузки видео нажмите кнопку ниже.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Я загрузил видео, продолжить", callback_data="bike_folder_confirmed")],
                [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_contact")]
            ])
        )
    else:
        await show_step(
            callback.message,
            state,
            text + "\n\n⚠️ Не удалось создать папку для байка. Продолжите выдачу.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Продолжить без папки", callback_data="bike_folder_confirmed")],
                [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_contact")]
            ])
        )
    await state.set_state(FSM.verify_folder)
    await callback.answer()


@dp.callback_query(F.data == "back:to_photo", FSM.confirm_rent)
async def back_to_photo_from_confirm(callback: CallbackQuery, state: FSMContext):
    """Возврат к загрузке фото из подтверждения"""
    await show_step(
        callback.message,
        state,
        "📄 Загрузите фото договора (можно несколько).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_verification")]
        ])
    )
    await state.set_state(FSM.upload_contract_photo)
    await callback.answer()



# --- Обработчики "Назад" для возврата байка ---
@dp.callback_query(F.data == "back:to_bike_list", FSM.return_wash)
async def back_to_bike_list_from_wash(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору байка из этапа мойки"""
    data = await state.get_data()
    brand = data.get("brand")
    if not brand:
        from bot import return_start
        await return_start(callback, state)
        return

    bikes = await repo.get_bikes_by_brand(brand, status="Аренда")
    if not bikes:
        await callback.answer("Нет байков этой модели в аренде", show_alert=True)
        return

    pages = get_pages_by_chars(bikes, lambda x: format_full_info(x[1]))
    page = 0
    current_data = pages[page]
    bike_buttons = [
        [InlineKeyboardButton(text=f"{r['МОДЕЛЬ']} | {r['Гос. номер']}", callback_data=f"ret_bike_sel:{i}")]
        for i, r in current_data
    ]
    text = f"🔄 Возврат {brand} (Стр. {page+1}/{len(pages)}):\n\n"
    for _, r in current_data:
        text += format_full_info(r)
    await show_step(
        callback.message,
        state,
        text,
        reply_markup=get_nav_keyboard(len(pages), page, "ret", "back:menu", bike_buttons)
    )
    await state.set_state(FSM.return_choose_bike)
    await callback.answer()

@dp.callback_query(F.data == "back:to_wash", FSM.return_damage)
async def back_to_wash_from_damage(callback: CallbackQuery, state: FSMContext):
    """Возврат к этапу мойки из этапа повреждений"""
    data = await state.get_data()
    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🧼 Да (50к)", callback_data="wash:yes"),
            InlineKeyboardButton(text="❌ Нет", callback_data="wash:no")
        ],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_bike_list")]
    ])
    await show_step(
        callback.message,
        state,
        f"Доплата: {data['overdue_fee']} VND. Нужна мойка?",
        reply_markup=kb
    )
    await state.set_state(FSM.return_wash)
    await callback.answer()

@dp.callback_query(F.data == "back:to_damage", FSM.return_confirm)
async def back_to_damage_from_confirm(callback: CallbackQuery, state: FSMContext):
    """Возврат к этапу повреждений из подтверждения"""
    data = await state.get_data()
    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]
    
    folder_name = f"{bike['МОДЕЛЬ']} {bike['Гос. номер']}"
    folder_id = await asyncio.to_thread(check_folder_exists, folder_name=folder_name)
    
    total_fee = data['overdue_fee'] + data.get('wash_fee', 0)
    
    if folder_id:
        folder_url = f"https://drive.google.com/drive/folders/{folder_id}"
        text = f"Доплата: {total_fee} VND.\n\n{folder_url}\n\nСравните с видео осмотра, есть повреждения?"
    else:
        text = f"Доплата: {total_fee} VND.\n\nСравните с видео осмотра, есть повреждения?"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Нет", callback_data="dmg:no"),
            InlineKeyboardButton(text="🛠 Да", callback_data="dmg:yes")
        ],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_wash")]
    ])
    await show_step(callback.message, state, text, reply_markup=kb)
    await state.set_state(FSM.return_damage)
    await callback.answer()

# --- Обработчики "Назад" для продления ---
@dp.callback_query(F.data == "back:extend_bikes", FSM.extend_confirm)
async def back_to_extend_term_from_confirm(callback: CallbackQuery, state: FSMContext):
    """Возврат к вводу срока из подтверждения продления"""
    data = await state.get_data()
    sheet, rows = await repo.get_all()
    bike = rows[data["row"] - 2]
    
    await show_step(
        callback.message,
        state,
        f"Текущий срок: {bike.get('Срок аренды', '-')} дн.\nВведите новый срок (дней или 'N месяцев'):",
        reply_markup=back_keyboard("back:extend_bikes")
    )
    await state.set_state(FSM.extend_enter_term)
    await callback.answer()

# --- Обработчики "Назад" для замены байка ---
@dp.callback_query(F.data == "back:to_rent_brand", FSM.replace_choose_rent_bike)
async def back_to_rent_brand_from_bikes(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору бренда байка в аренде"""
    await show_step(
        callback.message,
        state,
        "Замена байка. Выберите модель байка в аренде:",
        reply_markup=brands_keyboard("rep_rent_brand")
    )
    await state.set_state(FSM.replace_choose_brand)
    await callback.answer()


@dp.callback_query(F.data == "back:to_rent_bikes", FSM.replace_choose_brand)
async def back_to_rent_bikes_from_base_brand(callback: CallbackQuery, state: FSMContext):
    """Возврат к списку байков в аренде из выбора бренда для базы"""
    data = await state.get_data()
    rent_bikes = data.get("rent_bikes")
    rent_brand = data.get("rent_brand", "")
    
    if not rent_bikes:
        await replace_start(callback, state)
        return
    
    pages = get_pages_by_chars(rent_bikes, lambda x: format_full_info(x[1]))
    page = 0
    current_data = pages[page]
    bike_buttons = [
        [InlineKeyboardButton(text=f"{r['МОДЕЛЬ']} | {r['Гос. номер']}", callback_data=f"rep_rent_sel:{i}")]
        for i, (row, r) in enumerate(current_data)
    ]
    
    text = f"🔄 Замена байка. Выберите байк в аренде (Стр. {page+1}/{len(pages)}):\n\n"
    for row, r in current_data:
        text += format_full_info(r)
    
    await show_step(
        callback.message,
        state,
        text,
        reply_markup=get_nav_keyboard(len(pages), page, "rep_rent", "back:to_rent_brand", bike_buttons)
    )
    await state.set_state(FSM.replace_choose_rent_bike)
    await callback.answer()


@dp.callback_query(F.data == "back:to_base_brand", FSM.replace_choose_base_bike)
async def back_to_base_brand_from_bikes(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору бренда для байка на базе"""
    data = await state.get_data()
    rent_bike = data.get("rent_bike")
    
    brands = await repo.get_all_brands()
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=b, callback_data=f"rep_base_brand:{b}")]
            for b in brands
        ] + [[InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_rent_bikes")]]
    )
    
    await callback.message.edit_text(
        f"Выбран байк в аренде: {rent_bike['МОДЕЛЬ']} | {rent_bike['Гос. номер']}\n\n"
        f"Теперь выберите бренд для замены:",
        reply_markup=kb
    )
    await state.set_state(FSM.replace_choose_brand)
    await callback.answer()

@dp.callback_query(F.data.startswith("rep_rent_sel:"), FSM.replace_choose_rent_bike)
async def replace_choose_brand_for_base(callback: CallbackQuery, state: FSMContext):
    """После выбора байка в аренде - показ брендов для выбора байка на базе"""
    index = int(callback.data.split(":")[1])

    data = await state.get_data()
    rent_bikes = data.get("rent_bikes")

    if not rent_bikes or index >= len(rent_bikes):
        await callback.answer("Ошибка выбора байка", show_alert=True)
        return

    rent_row, rent_bike = rent_bikes[index]

    await state.update_data(
        rent_row=rent_row,
        rent_bike=rent_bike,
        rent_index=index
    )

    # Показываем список брендов для выбора байка на базе
    brands = await repo.get_all_brands()

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=b, callback_data=f"rep_base_brand:{b}")]
            for b in brands
        ] + [[InlineKeyboardButton(text="⬅ Назад", callback_data="back:to_rent_bikes")]]
    )

    await callback.message.edit_text(
        f"Выбран байк в аренде: {rent_bike['МОДЕЛЬ']} | {rent_bike['Гос. номер']}\n\n"
        f"Теперь выберите бренд для замены:",
        reply_markup=kb
    )
    await state.set_state(FSM.replace_choose_brand)
    await callback.answer()


@dp.callback_query(F.data.startswith("rep_base_brand:"), FSM.replace_choose_brand)
async def replace_choose_base_bike(callback: CallbackQuery, state: FSMContext):
    """Выбор байка на базе после выбора бренда"""
    brand = callback.data.split(":")[1]
    await state.update_data(base_brand=brand)

    # Используем get_bikes_by_brand для всех брендов, включая "Другие"
    base_bikes = await repo.get_bikes_by_brand(brand, status="База")

    if not base_bikes:
        await callback.answer("Нет свободных байков этого бренда", show_alert=True)
        return

    # Сохраняем список базы
    await state.update_data(base_bikes=base_bikes)

    pages = get_pages_by_chars(base_bikes, lambda x: format_full_info(x[1]))
    page = 0
    current_data = pages[page]
    bike_buttons = [
        [InlineKeyboardButton(text=f"{r['МОДЕЛЬ']} | {r['Гос. номер']}", callback_data=f"rep_base_sel:{i}")]
        for i, (row, r) in enumerate(current_data)
    ]

    text = f"🏠 Байки на базе {brand} (Стр. {page+1}/{len(pages)}):\n\n"
    for _, r in current_data:
        text += format_full_info(r)

    await show_step(
        callback.message,
        state,
        text,
        reply_markup=get_nav_keyboard(len(pages), page, "rep_base", "back:to_base_brand", bike_buttons)
    )
    await state.set_state(FSM.replace_choose_base_bike)
    await callback.answer()


@dp.callback_query(F.data.startswith("rep_base_page:"))
async def replace_base_page(callback: CallbackQuery, state: FSMContext):
    """Пагинация списка байков на базе"""
    page = int(callback.data.split(":")[1])
    data = await state.get_data()
    base_bikes = data.get("base_bikes")
    brand = data.get("base_brand", "")

    if not base_bikes:
        await callback.answer("Ошибка данных", show_alert=True)
        return

    pages = get_pages_by_chars(base_bikes, lambda x: format_full_info(x[1]))
    if page >= len(pages):
        page = 0

    current_data = pages[page]
    bike_buttons = [
        [InlineKeyboardButton(text=f"{r['МОДЕЛЬ']} | {r['Гос. номер']}", callback_data=f"rep_base_sel:{i}")]
        for i, (row, r) in enumerate(current_data)
    ]

    text = f"🏠 Байки на базе {brand} (Стр. {page+1}/{len(pages)}):\n\n"
    for _, r in current_data:
        text += format_full_info(r)

    await show_step(
        callback.message,
        state,
        text,
        reply_markup=get_nav_keyboard(len(pages), page, "rep_base", "back:to_base_brand", bike_buttons)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("rep_base_sel:"), FSM.replace_choose_base_bike)
async def replace_execute(callback: CallbackQuery, state: FSMContext):
    index = int(callback.data.split(":")[1])

    data = await state.get_data()
    base_bikes = data.get("base_bikes")

    if not base_bikes or index >= len(base_bikes):
        await callback.answer("Ошибка выбора байка", show_alert=True)
        return

    rent_row = data["rent_row"]
    rent_bike = data["rent_bike"]

    base_row, base_bike = base_bikes[index]

    # Перенос данных на новый байк
    await repo.update_bike(base_row, {
        "Статус": "Аренда",
        "Дата начала аренды": rent_bike.get("Дата начала аренды", ""),
        "Срок аренды": rent_bike.get("Срок аренды", ""),
        "Сумма": rent_bike.get("Сумма", ""),
        "Контакт клиента": rent_bike.get("Контакт клиента", ""),
        "Депозит": rent_bike.get("Депозит", ""),
        "Залог": rent_bike.get("Залог", ""),
        "Залог $": rent_bike.get("Залог $", ""),
        "Залог VND": rent_bike.get("Залог VND", "")
    })

    # Очистка старого байка (сохраняем Залог $ и Залог VND)
    await repo.update_bike(rent_row, {
        "Статус": "База",
        "Дата начала аренды": "",
        "Срок аренды": "",
        "Сумма": "",
        "Контакт клиента": "",
        "Депозит": "",
        "Залог": ""
    })

    result_text = (
        f"✅ Произведена замена!\n\n"
        f"❌ {rent_bike['МОДЕЛЬ']} | {rent_bike['Гос. номер']} → База\n"
        f"✅ {base_bike['МОДЕЛЬ']} | {base_bike['Гос. номер']} → Аренда"
    )

    await delete_old_messages(state, callback.message.chat.id)
    await callback.message.answer(result_text, reply_markup=main_menu())
    await state.set_state(FSM.menu)
    await state.update_data({MESSAGES_TO_DELETE_KEY: []})
    await callback.answer()

#================= ГЛАВНАЯ ФУНКЦИЯ =================
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "supersecret")
PORT = int(os.environ.get("PORT", 8080))

BASE_URL = os.environ.get("RAILWAY_STATIC_URL") or os.environ.get("WEBHOOK_HOST", "")
WEBHOOK_URL = f"https://{BASE_URL}{WEBHOOK_PATH}" if BASE_URL else None

async def on_startup(bot: Bot):
    """Установка webhook при запуске"""
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL, secret_token=WEBHOOK_SECRET)
        logger.info(f"Webhook установлен: {WEBHOOK_URL}")
    else:
        logger.warning("WEBHOOK_URL не задан, webhook не установлен")

async def on_shutdown(bot: Bot):
    """Удаление webhook при остановке"""
    await bot.delete_webhook()
    logger.info("Webhook удалён")

def main():
    app = web.Application()

    SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token=WEBHOOK_SECRET,
    ).register(app, path=WEBHOOK_PATH)


    setup_application(app, dp, bot=bot)

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    web.run_app(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()




