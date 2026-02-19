# -*- coding: utf-8 -*-
"""
Удаление дублирующихся обработчиков и исправление оставшихся проблем.
"""

# Read the file
with open('bot.py', 'r', encoding='utf-8') as f:
    content = f.read()

changes = []

# 1. Remove duplicate back_to_dep_type_from_contact (there are two at lines 1359 and 1451)
# Keep only one - the first one

# Find and remove the second duplicate
old_duplicate = '''@dp.callback_query(F.data == "back:to_dep_type", FSM.enter_contact)
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
        f"Срок: {days_to_show} дн. Сумма: {total} VND\\nВыберите тип депозита: ",
        reply_markup=kb
    )
    await state.set_state(FSM.enter_deposit_type)
    await callback.answer()


@dp.callback_query(F.data == "back:to_contact", FSM.upload_contract_photo)'''

new_section = '''@dp.callback_query(F.data == "back:to_contact", FSM.upload_contract_photo)'''

if old_duplicate in content:
    content = content.replace(old_duplicate, new_section)
    changes.append("Removed duplicate back_to_dep_type_from_contact handler")

# Write back
with open('bot.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Changes made:")
for c in changes:
    print(f"  - {c}")

if not changes:
    print("  No changes needed or patterns not found")