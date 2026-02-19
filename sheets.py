import os
import json
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

SPREADSHEET_ID = "1xrCL9RBJHfNQGETgLLvnQtrSErNhQPeYkaXVSKkjSQo"

def get_sheet():
    creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=SCOPES
    )

    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    return spreadsheet.worksheet("список наших байков")

def get_reports_sheet():
    creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=SCOPES
    )


    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    return spreadsheet.worksheet("Отчёты")


def update_reports(rental_sum):
    from datetime import datetime, timedelta
    import logging
    logger = logging.getLogger(__name__)
    
    sheet = get_reports_sheet()
    today = datetime.now().strftime("%d.%m.%Y")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")
    
    all_data = sheet.get_all_values()
    
    headers = all_data[0] if all_data else []
    logger.info(f"Headers found: {headers}")
    
    date_col = None
    sum_col = None
    count_col = None
    monthly_sum_col = None
    monthly_count_col = None
    
    for i, header in enumerate(headers):
        header_lower = header.lower().strip()
        logger.info(f"Checking header '{header}' at index {i}")
        if "дата" in header_lower:
            date_col = i
        elif "сумма выдачи" in header_lower:
            sum_col = i
        elif "количество выдач" in header_lower and "месяц" not in header_lower:
            count_col = i
        elif "сумма за месяц в кассе" in header_lower:
            monthly_sum_col = i
        elif "количество выдач за месяц" in header_lower:
            monthly_count_col = i
    
    logger.info(f"Column indices: date_col={date_col}, sum_col={sum_col}, count_col={count_col}, monthly_sum_col={monthly_sum_col}, monthly_count_col={monthly_count_col}")
    
    # Ищем строку с сегодняшней датой
    today_row = None
    for row_idx, row in enumerate(all_data[1:], start=2):  # start=2 т.к. строка 1 - заголовки
        if len(row) > date_col and row[date_col] == today:
            today_row = row_idx
            break
    
    # Ищем строку за предыдущий день для накопительных итогов
    yesterday_row = None
    yesterday_monthly_sum = 0
    yesterday_monthly_count = 0
    for row_idx, row in enumerate(all_data[1:], start=2):
        if len(row) > date_col and row[date_col] == yesterday:
            yesterday_row = row_idx
            if monthly_sum_col is not None and len(row) > monthly_sum_col:
                yesterday_monthly_sum = int(row[monthly_sum_col] or 0)
            if monthly_count_col is not None and len(row) > monthly_count_col:
                yesterday_monthly_count = int(row[monthly_count_col] or 0)
            break
    
    logger.info(f"Yesterday values: sum={yesterday_monthly_sum}, count={yesterday_monthly_count}")
    
    if today_row is None:
        # Создаём новую строку с сегодняшней датой
        today_row = len(all_data) + 1
        sheet.update_cell(today_row, date_col + 1, today)
        # Инициализируем нулями
        if sum_col is not None:
            sheet.update_cell(today_row, sum_col + 1, 0)
        if count_col is not None:
            sheet.update_cell(today_row, count_col + 1, 0)
        if monthly_sum_col is not None:
            sheet.update_cell(today_row, monthly_sum_col + 1, yesterday_monthly_sum)
        if monthly_count_col is not None:
            sheet.update_cell(today_row, monthly_count_col + 1, yesterday_monthly_count)
        logger.info(f"Created new row {today_row} for today")
    
    # Обновляем сумму выдачи за сегодня
    if sum_col is not None:
        current_sum = int(sheet.cell(today_row, sum_col + 1).value or 0)
        sheet.update_cell(today_row, sum_col + 1, current_sum + rental_sum)
        logger.info(f"Updated sum_col: {current_sum} + {rental_sum}")
    
    # Увеличиваем количество выдач за сегодня
    if count_col is not None:
        current_count = int(sheet.cell(today_row, count_col + 1).value or 0)
        sheet.update_cell(today_row, count_col + 1, current_count + 1)
        logger.info(f"Updated count_col: {current_count} + 1")
    
    # Обновляем месячные итоги (накопительно)
    if monthly_sum_col is not None:
        current_monthly_sum = int(sheet.cell(today_row, monthly_sum_col + 1).value or 0)
        sheet.update_cell(today_row, monthly_sum_col + 1, current_monthly_sum + rental_sum)
        logger.info(f"Updated monthly_sum_col: {current_monthly_sum} + {rental_sum}")
    
    if monthly_count_col is not None:
        current_monthly_count = int(sheet.cell(today_row, monthly_count_col + 1).value or 0)
        sheet.update_cell(today_row, monthly_count_col + 1, current_monthly_count + 1)
        logger.info(f"Updated monthly_count_col: {current_monthly_count} + 1")


def update_reports_extend(rental_sum):
    """
    Обновляет отчёт при продлении байка (только суммы, без увеличения количества выдач).
    - Находит или создаёт строку с сегодняшней датой
    - Добавляет сумму к "Сумма выдачи"
    - Добавляет сумму к "Сумма за месяц в кассе"
    - НЕ увеличивает "Количество выдач" и "Количество выдач за месяц"
    """
    from datetime import datetime, timedelta
    import logging
    logger = logging.getLogger(__name__)
    
    sheet = get_reports_sheet()
    today = datetime.now().strftime("%d.%m.%Y")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")
    
    # Получаем все данные
    all_data = sheet.get_all_values()
    
    # Находим заголовки (первая строка)
    headers = all_data[0] if all_data else []
    
    # Определяем индексы колонок
    date_col = None
    sum_col = None
    monthly_sum_col = None
    
    for i, header in enumerate(headers):
        header_lower = header.lower().strip()
        if "дата" in header_lower:
            date_col = i
        elif "сумма выдачи" in header_lower:
            sum_col = i
        elif "сумма за месяц в кассе" in header_lower:
            monthly_sum_col = i
    
    # Ищем строку с сегодняшней датой
    today_row = None
    for row_idx, row in enumerate(all_data[1:], start=2):
        if len(row) > date_col and row[date_col] == today:
            today_row = row_idx
            break
    
    # Ищем строку за предыдущий день для накопительных итогов
    yesterday_monthly_sum = 0
    for row_idx, row in enumerate(all_data[1:], start=2):
        if len(row) > date_col and row[date_col] == yesterday:
            if monthly_sum_col is not None and len(row) > monthly_sum_col:
                yesterday_monthly_sum = int(row[monthly_sum_col] or 0)
            break
    
    if today_row is None:
        # Создаём новую строку с сегодняшней датой
        today_row = len(all_data) + 1
        sheet.update_cell(today_row, date_col + 1, today)
        if sum_col is not None:
            sheet.update_cell(today_row, sum_col + 1, 0)
        if monthly_sum_col is not None:
            sheet.update_cell(today_row, monthly_sum_col + 1, yesterday_monthly_sum)
    
    # Обновляем только суммы (без увеличения количества выдач)
    if sum_col is not None:
        current_sum = int(sheet.cell(today_row, sum_col + 1).value or 0)
        sheet.update_cell(today_row, sum_col + 1, current_sum + rental_sum)
    
    if monthly_sum_col is not None:
        current_monthly_sum = int(sheet.cell(today_row, monthly_sum_col + 1).value or 0)
        sheet.update_cell(today_row, monthly_sum_col + 1, current_monthly_sum + rental_sum)
