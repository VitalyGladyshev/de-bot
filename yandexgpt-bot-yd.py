"""
Бот для проекта по предмету инжиниринг данных
Реализован доступ к модели YandexGPT 5 Pro
Реализовано сохранение CSV и XLSX файлов статистики на Яндекс.Диск

Гладышев ВВ
"""

import logging
import config
import json
import requests
import csv
import os
import pathlib
import pandas as pd
from datetime import datetime

import yadisk

from telegram import ForceReply, Update
from telegram.ext import Application, CallbackContext, CommandHandler, ContextTypes, MessageHandler, filters

# Запуск логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# установление уровня логирования httpx
#logging.getLogger("httpx").setLevel(logging.INFO) #WARNING)

LOG_FILE = pathlib.Path.home() / 'user_actions.csv'  # Файл в домашней директории
EXCEL_FILE = pathlib.Path.home() / 'user_actions.xlsx'
logger = logging.getLogger(__name__)

def convert_csv_to_xlsx():
    """Конвертирует CSV файл в XLSX формат"""
    try:
        if os.path.exists(EXCEL_FILE):
            os.remove(EXCEL_FILE)
            logger.info(f"Старый файл {EXCEL_FILE} удален.")
        
        df = pd.read_csv(LOG_FILE)
        df.to_excel(EXCEL_FILE, index=False)
        logger.info(f"Файл {LOG_FILE} успешно конвертирован в {EXCEL_FILE}")
        return True
    except Exception as e:
        logger.error(f"Ошибка конвертации CSV в XLSX: {e}")
        return False

def upload_log_to_yandex_disk():
    """Загружает CSV и XLSX файлы на Яндекс.Диск"""
    try:
        y = yadisk.YaDisk(token=config.YANDEX_DISK_TOKEN)
        
        if not y.check_token():
            logger.error("Недействительный токен Яндекс.Диска")
            return False
            
        if not LOG_FILE.exists():
            logger.error(f"Локальный файл {LOG_FILE} не существует")
            return False
            
        remote_path = "/bot_logs/user_actions.csv"
        remote_path_xlsx = "/bot_logs/user_actions.xlsx"
        remote_dir = "/bot_logs"
        
        if not y.exists(remote_dir):
            y.mkdir(remote_dir)
            logger.info(f"Создана папка {remote_dir} на Яндекс.Диске")
            
        y.upload(str(LOG_FILE), remote_path, overwrite=True)
        logger.info(f"Файл {LOG_FILE} успешно загружен на Яндекс.Диск")

        y.upload(str(EXCEL_FILE), remote_path_xlsx, overwrite=True)
        logger.info(f"Файл {EXCEL_FILE} успешно загружен на Яндекс.Диск")
        return True
        
    except yadisk.exceptions.UnauthorizedError:
        logger.error("Ошибка авторизации. Проверьте токен Яндекс.Диска")
    except Exception as e:
        logger.error(f"Ошибка загрузки на Яндекс.Диск: {str(e)}")
    return False

# Сохранение статистики в файл и на Яндекс.Диск
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

        if convert_csv_to_xlsx():
            logger.info(f"Файл {EXCEL_FILE} успешно создан")

        # Вызываем загрузку после каждой записи
        if not upload_log_to_yandex_disk():
            logger.warning("Не удалось выполнить резервное копирование")

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
        if not EXCEL_FILE.exists():
            EXCEL_FILE.touch(mode=0o644)  # Создаем файл с правами rw-r--r--
    except Exception as e:
        logger.error(f"Can't create log file: {str(e)}")
        exit(1)


def main() -> None:
    """Инициализация бота"""
    check_log_file()

    # Проверка подключения к Яндекс.Диску
    try:
        y = yadisk.YaDisk(token=config.YANDEX_DISK_TOKEN)
        if y.check_token():
            logger.info("Подключение к Яндекс.Диску успешно")
        else:
            logger.warning("Проблемы с подключением к Яндекс.Диску")
    except Exception as e:
        logger.error(f"Ошибка подключения: {str(e)}")

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
