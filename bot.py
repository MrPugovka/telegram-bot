import os
import logging
from aiohttp import web

from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from sheets import get_sheet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
PORT = int(os.environ.get("PORT", 8080))
logger.info(f"PORT from env: {PORT}")
BASE_URL = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
WEBHOOK_URL = f"https://{BASE_URL}{WEBHOOK_PATH}"

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()
router = Router()

@router.message()
async def test_handler(message: Message):
    try:
        logger.info("Handler started")
        sheet = get_sheet()
        logger.info("Sheet получен успешно")
        await message.answer("OK")
    except Exception as e:
        logger.exception("Ошибка при работе с sheets")
        await message.answer("Ошибка")


dp.include_router(router)

async def on_startup(bot: Bot):
    await bot.set_webhook(WEBHOOK_URL)
    logger.info(f"Webhook установлен: {WEBHOOK_URL}")

async def on_shutdown(bot: Bot):
    await bot.delete_webhook()

def main():
    app = web.Application()

    async def health(request):
        return web.Response(text="OK")

    app.router.add_get("/", health)

    SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    ).register(app, path=WEBHOOK_PATH)

    setup_application(app, dp, bot=bot)

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    return app


if __name__ == "__main__":
    real_port = int(os.environ["PORT"])
    logger.info(f"Starting app on PORT: {real_port}")
    app = main()
    web.run_app(app, port=real_port)
