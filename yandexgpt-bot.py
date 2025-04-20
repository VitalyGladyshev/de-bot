"""
Бот для проекта по предмету инжиниринг данных

Гладышев ВВ
"""

import logging
import config
import json
import requests
import csv
import os
import pathlib
from datetime import datetime

from telegram import ForceReply, Update
from telegram.ext import Application, CallbackContext, CommandHandler, ContextTypes, MessageHandler, filters

# Запуск логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# установление уровня логирования httpx
#logging.getLogger("httpx").setLevel(logging.INFO) #WARNING)

LOG_FILE = pathlib.Path.home() / 'user_actions.csv'  # Файл в домашней директории
logger = logging.getLogger(__name__)

# Сохранение статистики в файл
def log_action(user_id: int, action: str, timestamp: datetime) -> None:
    try:
        file_exists = LOG_FILE.exists() and LOG_FILE.stat().st_size > 0
        with open(LOG_FILE, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['id', 'datetime', 'action']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()
            
            writer.writerow({
                'id': user_id,
                'datetime': timestamp.isoformat(),
                'action': action
            })

    except PermissionError as e:
        logger.error(f"Ошибка прав доступа к файлу {LOG_FILE}: {str(e)}")
        raise  # Повторно вызываем исключение для прерывания работы
    except Exception as e:
        logger.error(f"Ошибка записи в лог: {str(e)}")
        raise

# Получение IAM токена по OAUTH токену
def get_iam_token():
    response = requests.post(
        'https://iam.api.cloud.yandex.net/iam/v1/tokens',
        json={'yandexPassportOauthToken': config.OAUTH_TOKEN}
    )

    response.raise_for_status()
    return response.json()['iamToken']

# Обработчики команд
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик запроса старт /start """
    user_id = update.message.from_user.id
    timestamp = update.message.date
    action = 'start'
    log_action(user_id, action, timestamp)
    
    user = update.effective_user
    await update.message.reply_html(
        rf"Здравствуйте {user.mention_html()}! Всё готово для доступа к YandexGPT 5",
        reply_markup=ForceReply(selective=True),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик запроса /help"""
    user_id = update.message.from_user.id
    timestamp = update.message.date
    action = 'help'
    log_action(user_id, action, timestamp)
    
    await update.message.reply_text("Бот для диалога с YandexGPT 5 с визуализацией статистических показателей посещений")


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Для эхо-режима"""
    await update.message.reply_text(update.message.text)


async def process_message(update: Update, context: CallbackContext) -> None:
    """Для доступа к модели YandexGPT"""
    user_text = update.message.text

    user_id = update.message.from_user.id
    timestamp = update.message.date
    action = 'answer'
    log_action(user_id, action, timestamp)

    # Получаем IAM токен
    iam_token = get_iam_token()

    # Формируем запрос
    data = {
        "modelUri": f"gpt://b1gt3tgpol6rntep2m65/yandexgpt/rc",
        "completionOptions": {"temperature": 0.3, "maxTokens": 8000},
        "messages": [{"role": "user", "text": user_text}]
    }

    # Отправляем запрос
    response = requests.post(
        "https://llm.api.cloud.yandex.net/foundationModels/v1/completion",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {iam_token}"
        },
        json=data,
    ).json()

    # Распечатываем результат
    # print(response)

    answer = response.get('result', {})\
                     .get('alternatives', [{}])[0]\
                     .get('message', {})\
                     .get('text', {})

    await update.message.reply_text(answer)

# Проверка и создание файла при запуске
def check_log_file():
    try:
        if not LOG_FILE.exists():
            LOG_FILE.touch(mode=0o644)  # Создаем файл с правами rw-r--r--
    except Exception as e:
        logger.error(f"Can't create log file: {str(e)}")
        exit(1)


def main() -> None:
    """Инициализация бота"""
    check_log_file()

    # Создание экземпляра бота
    application = Application.builder().token(config.BOT_TOKEN).build()

    # Обработчики стандартных команд Телеграмм
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))

    # Ответ на запрос пользователя - доступ к модели YandexGPT Pro rc
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, process_message))

    # Запуск с выходом по нажатию Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)

# Точка входа
if __name__ == "__main__":
    main()
