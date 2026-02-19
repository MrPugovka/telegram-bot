# -*- coding: utf-8 -*-
"""
Исправление оставшихся проблем с обработчиками.
"""

# Read the file
with open('bot.py', 'r', encoding='utf-8') as f:
    content = f.read()

changes = []

# 1. Remove the generic b_bike_list handler - it's causing conflicts
old_bike_list = '''@dp.callback_query(F.data == "back:to_bike_list")
async def b_bike_list(callback: CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state in [FSM.return_choose_bike, FSM.extend_choose_bike]:
        await rented_bike_pagination(callback, state)
    else:
        await brand_selected(callback, state)
    await callback.answer()

'''

if old_bike_list in content:
    content = content.replace(old_bike_list, "")
    changes.append("Removed generic b_bike_list handler")

# Write back
with open('bot.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Changes made:")
for c in changes:
    print(f"  - {c}")

if not changes:
    print("  No changes needed or patterns not found")