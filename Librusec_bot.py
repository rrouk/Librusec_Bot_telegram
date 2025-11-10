import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import zipfile
import os
import json
import sys
import logging
import re
from datetime import datetime
from telebot.apihelper import ApiTelegramException
import io
import sqlite3
import hashlib
from lxml import etree

BOT_TOKEN = os.getenv('BOT_TOKEN', None) 

# ID АДМИНА: Читаем строку "id1,id2" из переменной ADMIN_IDS и преобразуем в список
try:
    # Используем os.getenv для чтения списка ID
    ADMIN_IDS_STR = os.getenv('ADMIN_IDS', '')
    ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(',') if x.strip()]
except Exception:
    ADMIN_IDS = []

# 2. Пути к файлам и директориям (универсальные пути ВНУТРИ контейнера)
# Все эти пути будут определены в секции environment в docker-compose.yml
READER_FOLDER = os.getenv('READER_FOLDER', '/app/data/reader')
INPX_FILE = os.getenv('INPX_FILE', '/app/books/librusec_local_fb2.inpx')
BOOKS_DIR = os.getenv('BOOKS_DIR', '/app/books')
DOWNLOAD_FOLDER = os.getenv('DOWNLOAD_FOLDER', '/app/data/downloads')
USERS_JSON_FILE = "/app/data/users_librusec.json"
PENDING_USERS_JSON_FILE = "/app/data/pending_users_librusec.json"
LOG_FILE = "/app/log/Log_librusecBase_bot.log"
DB_FILE = "/app/data/reader_data.db"

# 3. Настройки
# Читаем из окружения, если не задано, используем значение по умолчанию
PAGE_SIZE = int(os.getenv('PAGE_SIZE', 2000))
MAX_BOOKS = int(os.getenv('MAX_BOOKS', 10))

# =================================================================
# ПРОВЕРКА КРИТИЧЕСКИХ НАСТРОЕК
# =================================================================
if not BOT_TOKEN:
    print("❌ Ошибка: Переменная окружения BOT_TOKEN не установлена. Запуск бота невозможен.")
    sys.exit(1)
    
# Создание директорий, чтобы избежать ошибок
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
os.makedirs(READER_FOLDER, exist_ok=True)

# Поля в .inp файле
FIELDS = ['AUTHOR', 'GENRE', 'TITLE', 'SERIES', 'SERNO', 'FILE', 'SIZE', 'LIBID', 'DEL', 'EXT', 'DATE', 'LANG', 'RATING', 'KEYWORDS']

# Глобальные переменные для хранения данных
books_data = []
# Словарь для хранения результатов поиска и текущей страницы для каждого пользователя
user_search_results = {}
user_data = {}
# Теперь registered_users будет словарем с полной информацией о пользователях
registered_users = {}
# Теперь pending_users будет словарем, где ключ - user_id, а значение - словарь с данными
pending_users = {}
is_processing_link = {}
user_state = {}

# Настройки для пагинации
results_per_page = 10

# Инициализация бота
bot = telebot.TeleBot(BOT_TOKEN)

# =================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# =================================================================
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# =================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ
# =================================================================

def db_connect():
    """Подключается к базе данных и возвращает соединение."""
    conn = sqlite3.connect(DB_FILE)
    return conn

def create_table():
    """Создает таблицу для хранения данных о книгах, если она не существует."""
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reading_sessions (
            user_id INTEGER,
            book_id TEXT PRIMARY KEY,
            book_title TEXT,
            book_author TEXT,
            book_series TEXT,
            series_number INTEGER,
            book_content TEXT,
            current_page INTEGER,
            total_pages INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Таблица базы данных успешно создана или уже существует.")


# =================================================================
# ФУНКЦИИ ПРОВЕРКИ ДОСТУПА
# =================================================================
def is_user_approved(user_id):
    """
    Проверяет, является ли пользователь одобренным.
    """
    return str(user_id) in registered_users or user_id in ADMIN_IDS

def is_user_admin(user_id):
    """
    Проверяет, является ли пользователь администратором.
    """
    return user_id in ADMIN_IDS


def save_user_state(user_id, title, author, series, series_number, content, page, total_pages):
    """Сохраняет или обновляет текущее состояние чтения пользователя."""
    book_id = hashlib.sha256(f"{user_id}{title}{author}{series}{series_number}".encode('utf-8')).hexdigest()
    conn = db_connect()
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM reading_sessions WHERE user_id = ?', (user_id,))
    book_count = cursor.fetchone()[0]

    if book_count >= MAX_BOOKS:
        cursor.execute('SELECT 1 FROM reading_sessions WHERE user_id = ? AND book_id = ?', (user_id, book_id))
        if cursor.fetchone() is None:
            conn.close()
            return 'limit_reached'

    cursor.execute('''
        INSERT OR REPLACE INTO reading_sessions 
        (user_id, book_id, book_title, book_author, book_series, series_number, book_content, current_page, total_pages)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (user_id, book_id, title, author, series, series_number, content, page, total_pages))
    conn.commit()
    conn.close()
    logger.info(f"Состояние чтения для пользователя {user_id} сохранено. Страница: {page}.")
    return 'success'

def load_user_state(user_id, book_id):
    """Загружает текущее состояние чтения пользователя для конкретной книги."""
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute('SELECT book_title, book_author, book_series, series_number, book_content, current_page, total_pages FROM reading_sessions WHERE user_id = ? AND book_id = ?', (user_id, book_id))
    result = cursor.fetchone()
    conn.close()
    if result:
        return {'title': result[0], 'author': result[1], 'series': result[2], 'series_number': result[3], 'content': result[4], 'current_page': result[5], 'total_pages': result[6]}
    return None

def get_user_books(user_id):
    """Возвращает список всех книг, которые читает пользователь."""
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute('SELECT book_id, book_title, book_author, book_series, series_number, current_page, total_pages FROM reading_sessions WHERE user_id = ? ORDER BY timestamp DESC', (user_id,))
    books = cursor.fetchall()
    conn.close()
    return books

def delete_user_book(user_id, book_id):
    """Удаляет книгу из базы данных для пользователя."""
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM reading_sessions WHERE user_id = ? AND book_id = ?', (user_id, book_id))
    conn.commit()
    conn.close()
    logger.info(f"Книга с id '{book_id}' удалена для пользователя {user_id}.")

# =================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =================================================================

def escape_markdown(text):
    """Экранирует специальные символы в тексте для MarkdownV2."""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def parse_fb2(file_content):
    """Парсит FB2-файл и возвращает его содержимое с Markdown-форматированием."""
    try:
        tree = etree.fromstring(file_content)
        namespaces = {'fb': 'http://www.gribuser.ru/xml/fictionbook/2.0'}
        
        # Парсинг названия
        title_element = tree.find('.//fb:book-title', namespaces=namespaces)
        title = title_element.text.strip() if title_element is not None and title_element.text else "Без названия"
        
        # Парсинг автора
        author_elements = tree.findall('.//fb:author', namespaces=namespaces)
        author = "Неизвестный автор"
        if author_elements:
            author_names = []
            for author_elem in author_elements:
                first_name_elem = author_elem.find('fb:first-name', namespaces=namespaces)
                last_name_elem = author_elem.find('fb:last-name', namespaces=namespaces)
                nickname_elem = author_elem.find('fb:nickname', namespaces=namespaces)
                
                name_parts = []
                if first_name_elem is not None and first_name_elem.text:
                    name_parts.append(first_name_elem.text.strip())
                if last_name_elem is not None and last_name_elem.text:
                    name_parts.append(last_name_elem.text.strip())
                if nickname_elem is not None and nickname_elem.text:
                    name_parts.append(f'({nickname_elem.text.strip()})')
                
                if name_parts:
                    author_names.append(' '.join(name_parts))
            
            if author_names:
                author = ', '.join(author_names)

        # Парсинг серии и номера
        series = "Нет серии"
        series_number = -1
        sequence_elem = tree.find('.//fb:sequence', namespaces=namespaces)
        if sequence_elem is not None:
            series = sequence_elem.attrib.get('name', "Нет серии")
            number_str = sequence_elem.attrib.get('number', '-1')
            try:
                series_number = int(number_str)
            except (ValueError, TypeError):
                series_number = -1
        
        formatted_text = ""
        for body in tree.findall('.//fb:body', namespaces=namespaces):
            for section in body.findall('.//fb:section', namespaces=namespaces):
                for elem in section.iterchildren():
                    if elem.tag.endswith('p') or elem.tag.endswith('empty-line'):
                        formatted_text += "\n\n"
                        current_paragraph = ""
                        if elem.text:
                            current_paragraph += escape_markdown(elem.text)
                        
                        for child in elem:
                            tag_name = child.tag.split('}')[-1]
                            if tag_name in ['strong', 'b']:
                                current_paragraph += f"**{escape_markdown(child.text or '')}**"
                            elif tag_name in ['emphasis', 'i']:
                                current_paragraph += f"*{escape_markdown(child.text or '')}*"
                            else:
                                current_paragraph += escape_markdown(child.text or '')
                            
                            if child.tail:
                                current_paragraph += escape_markdown(child.tail)
                        
                        formatted_text += current_paragraph.strip()
                    elif elem.tag.endswith('subtitle') or elem.tag.endswith('h1'):
                        formatted_text += f"\n\n**{escape_markdown(elem.text.strip())}**\n"
        
        return title, author, series, series_number, formatted_text.strip()

    except Exception as e:
        logger.error(f"Ошибка при парсинге FB2: {e}")
        return None, None, None, -1, None

def get_page_text(book_content, page_number):
    """Возвращает текст для заданной страницы, избегая разрывов внутри абзацев."""
    paragraphs = book_content.split("\n\n")
    
    pages = []
    current_page = []
    current_len = 0

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        # Если абзац полностью влезает в текущую страницу
        if current_len + len(para) + 2 <= PAGE_SIZE:
            current_page.append(para)
            current_len += len(para) + 2
        else:
            # Если абзац длинный — режем его по кускам
            if len(para) > PAGE_SIZE:
                start = 0
                while start < len(para):
                    chunk = para[start:start+PAGE_SIZE]
                    current_page.append(chunk)
                    pages.append("\n\n".join(current_page))
                    current_page = []
                    current_len = 0
                    start += PAGE_SIZE
            else:
                # Закрываем текущую страницу
                pages.append("\n\n".join(current_page))
                current_page = [para]
                current_len = len(para) + 2
    
    if current_page:
        pages.append("\n\n".join(current_page))
    
    # Возврат нужной страницы
    if page_number < len(pages):
        return pages[page_number]
    return ""

def get_reading_keyboard(book_id, total_pages, current_page):
    """Создает клавиатуру с кнопками 'Назад', 'Мои книги' и 'Далее'."""
    markup = InlineKeyboardMarkup()
    short_book_id = book_id[:16]
    
    buttons = []
    if current_page > 0:
        buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"prev_page:{short_book_id}"))
    
    buttons.append(InlineKeyboardButton("📚 Мои книги", callback_data="my_books"))
    
    if current_page < total_pages - 1:
        buttons.append(InlineKeyboardButton("➡️ Далее", callback_data=f"next_page:{short_book_id}"))
        
    markup.row(*buttons)
    return markup

def get_book_actions_keyboard(book_id):
    """Создает клавиатуру для действий с книгой (Читать, удалить)."""
    markup = InlineKeyboardMarkup()
    short_book_id = book_id[:16]
    markup.row(
        InlineKeyboardButton("📕 Читать", callback_data=f"read_book:{short_book_id}"),
        InlineKeyboardButton("➡️ Страница", callback_data=f"goto_page:{short_book_id}"),
        InlineKeyboardButton("❌ Удалить", callback_data=f"delete_book:{short_book_id}")
    )
    return markup
 
@bot.callback_query_handler(func=lambda call: call.data.startswith('goto_page:'))
def handle_goto_page(call):
    chat_id = call.message.chat.id
    short_book_id = call.data.split(':')[1]

    # Ищем полный book_id
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute('SELECT book_id FROM reading_sessions WHERE user_id = ? AND book_id LIKE ?', (chat_id, f"{short_book_id}%"))
    result = cursor.fetchone()
    conn.close()

    if not result:
        bot.answer_callback_query(call.id, "Книга не найдена.")
        return

    book_id = result[0]
    # Запоминаем состояние — ждём ввода номера страницы
    user_state[chat_id] = {"action": "goto_page", "book_id": book_id}
    bot.send_message(chat_id, "Введите номер страницы, на которую хотите перейти:")
    bot.answer_callback_query(call.id)

@bot.message_handler(func=lambda message: message.chat.id in user_state and user_state[message.chat.id]["action"] == "goto_page")
def handle_page_input(message):
    chat_id = message.chat.id
    state = user_state.get(chat_id, {})
    book_id = state.get("book_id")

    if not book_id:
        return

    try:
        page_number = int(message.text) - 1  # переводим в индекс
    except ValueError:
        bot.send_message(chat_id, "Введите корректный номер страницы (число).")
        return

    reading_state = load_user_state(chat_id, book_id)
    if not reading_state:
        bot.send_message(chat_id, "Ошибка: книга не найдена.")
        return

    total_pages = reading_state['total_pages']
    if page_number < 0 or page_number >= total_pages:
        bot.send_message(chat_id, f"Укажите число от 1 до {total_pages}.")
        return

    page_text = get_page_text(reading_state['content'], page_number)
    response_text = f"**{escape_markdown(reading_state['title'])}**\n"
    if reading_state['series'] and reading_state['series'] != "Нет серии":
        series_info = f"_{escape_markdown(reading_state['series'])}"
        if reading_state['series_number'] != -1:
            series_info += f" №{reading_state['series_number']}"
        response_text += f"{series_info}_\n"
    if reading_state['author']:
        response_text += f"_{escape_markdown(reading_state['author'])}_\n\n"
    response_text += page_text
    response_text += f"\n\n_Страница {page_number + 1} из {total_pages}_"

    bot.send_message(chat_id, response_text, reply_markup=get_reading_keyboard(book_id, total_pages, page_number), parse_mode="MarkdownV2")

    # Сохраняем прогресс
    save_user_state(chat_id, reading_state['title'], reading_state['author'], reading_state['series'],
                    reading_state['series_number'], reading_state['content'], page_number, total_pages)

    # Чистим состояние
    del user_state[chat_id] 
    
@bot.message_handler(commands=['mybooks'])
def show_my_books(message):
    handle_my_books_callback(message)

@bot.message_handler(regexp=r"^/delete\_[a-f0-9]{64}$")
def delete_book_by_command(message):
    chat_id = message.chat.id
    book_id = message.text.split('_')[1]
    delete_user_book(chat_id, book_id)
    bot.send_message(chat_id, "Книга успешно удалена\\.", parse_mode="MarkdownV2")

@bot.message_handler(content_types=['document'])
def handle_document(message):
    file_name = message.document.file_name
    chat_id = message.chat.id
    
    logger.info(f"Получен файл от {chat_id}: {file_name}")

    if not (file_name.endswith('.fb2') or file_name.endswith('.fb2.zip')):
        bot.send_message(chat_id, "Пожалуйста, отправьте файл в формате **\\.fb2** или **\\.fb2\\.zip**\\.", parse_mode="MarkdownV2")
        return

    try:
        file_info = bot.get_file(message.document.file_id)
        downloaded_file = bot.download_file(file_info.file_path)
    except Exception as e:
        logger.error(f"Ошибка при скачивании файла: {e}")
        bot.send_message(chat_id, "Не удалось скачать файл\\. Попробуйте еще раз\\.", parse_mode="MarkdownV2")
        return

    file_content = None
    if file_name.endswith('.fb2.zip'):
        try:
            with zipfile.ZipFile(io.BytesIO(downloaded_file), 'r') as zip_file:
                fb2_file = next((f for f in zip_file.namelist() if f.endswith('.fb2')), None)
                if fb2_file:
                    with zip_file.open(fb2_file) as f:
                        file_content = f.read()
        except zipfile.BadZipFile:
            bot.send_message(chat_id, "Это поврежденный ZIP\\-архив\\.", parse_mode="MarkdownV2")
            return
    elif file_name.endswith('.fb2'):
        file_content = downloaded_file

    if not file_content:
        bot.send_message(chat_id, "Не удалось извлечь FB2\\-файл из архива\\.", parse_mode="MarkdownV2")
        return
        
    title, author, series, series_number, book_text = parse_fb2(file_content)
    
    if not book_text:
        bot.send_message(chat_id, "Не удалось прочитать книгу\\. Возможно, файл поврежден\\.", parse_mode="MarkdownV2")
        return

    total_pages = (len(book_text) + PAGE_SIZE - 1) // PAGE_SIZE
    
    save_result = save_user_state(chat_id, title, author, series, series_number, book_text, 0, total_pages)
    if save_result == 'limit_reached':
        bot.send_message(chat_id, f"Вы достигли лимита в {MAX_BOOKS} книг\\. Пожалуйста, удалите одну из старых книг с помощью команды /mybooks, чтобы добавить новую\\.", parse_mode="MarkdownV2")
        return
        
    book_id = hashlib.sha256(f"{chat_id}{title}{author}{series}{series_number}".encode('utf-8')).hexdigest()
    
    first_page_text = get_page_text(book_text, 0)
    
    response_text = f"**Начинаем читать:** {escape_markdown(title)}\n"
    if series and series != "Нет серии":
        series_info = f"_{escape_markdown(series)}"
        if series_number != -1:
            series_info += f" №{series_number}"
        response_text += f"{series_info}_\n"
    if author:
        response_text += f"_{escape_markdown(author)}_\n\n"
    response_text += first_page_text
    response_text += f"\n\n_Страница 1 из {total_pages}_"
    
    bot.send_message(chat_id, response_text, reply_markup=get_reading_keyboard(book_id, total_pages, 0), parse_mode="MarkdownV2")
    logger.info(f"Пользователь {chat_id} начал читать книгу '{title}'.")
    
# =================================================================
# ОБРАБОТЧИКИ CALLBACK-КНОПОК
# =================================================================

@bot.callback_query_handler(func=lambda call: call.data.startswith('next_page:'))
def handle_next_page(call):
    chat_id = call.message.chat.id
    short_book_id = call.data.split(':')[1]
    
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute('SELECT book_id FROM reading_sessions WHERE user_id = ? AND book_id LIKE ?', (chat_id, f"{short_book_id}%"))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        bot.send_message(chat_id, "Сессия чтения завершена\\. Пожалуйста, выберите книгу из списка или отправьте новую\\.", parse_mode="MarkdownV2")
        return
        
    book_id = result[0]
    
    reading_state = load_user_state(chat_id, book_id)
    if not reading_state:
        bot.send_message(chat_id, "Сессия чтения завершена\\. Пожалуйста, выберите книгу из списка или отправьте новую\\.", parse_mode="MarkdownV2")
        return

    current_page = reading_state['current_page']
    total_pages = reading_state['total_pages']
    
    if current_page >= total_pages - 1:
        bot.answer_callback_query(call.id, "Вы на последней странице.")
        return

    next_page_number = current_page + 1
    page_text = get_page_text(reading_state['content'], next_page_number)
    
    response_text = f"**{escape_markdown(reading_state['title'])}**\n"
    if reading_state['series'] and reading_state['series'] != "Нет серии":
        series_info = f"_{escape_markdown(reading_state['series'])}"
        if reading_state['series_number'] != -1:
            series_info += f" №{reading_state['series_number']}"
        response_text += f"{series_info}_\n"
    if reading_state['author']:
        response_text += f"_{escape_markdown(reading_state['author'])}_\n\n"
    response_text += page_text
    response_text += f"\n\n_Страница {next_page_number + 1} из {total_pages}_"
    
    bot.edit_message_text(response_text, chat_id, call.message.message_id, reply_markup=get_reading_keyboard(book_id, total_pages, next_page_number), parse_mode="MarkdownV2")
    bot.answer_callback_query(call.id)
    save_user_state(chat_id, reading_state['title'], reading_state['author'], reading_state['series'], reading_state['series_number'], reading_state['content'], next_page_number, total_pages)

@bot.callback_query_handler(func=lambda call: call.data.startswith('prev_page:'))
def handle_prev_page(call):
    chat_id = call.message.chat.id
    short_book_id = call.data.split(':')[1]
    
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute('SELECT book_id FROM reading_sessions WHERE user_id = ? AND book_id LIKE ?', (chat_id, f"{short_book_id}%"))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        bot.send_message(chat_id, "Сессия чтения завершена\\. Пожалуйста, выберите книгу из списка или отправьте новую\\.", parse_mode="MarkdownV2")
        return
        
    book_id = result[0]
    
    reading_state = load_user_state(chat_id, book_id)
    if not reading_state:
        bot.send_message(chat_id, "Сессия чтения завершена\\. Пожалуйста, выберите книгу из списка или отправьте новую\\.", parse_mode="MarkdownV2")
        return

    current_page = reading_state['current_page']
    total_pages = reading_state['total_pages']
    
    if current_page <= 0:
        bot.answer_callback_query(call.id, "Вы на первой странице.")
        return

    prev_page_number = current_page - 1
    page_text = get_page_text(reading_state['content'], prev_page_number)
    
    response_text = f"**{escape_markdown(reading_state['title'])}**\n"
    if reading_state['series'] and reading_state['series'] != "Нет серии":
        series_info = f"_{escape_markdown(reading_state['series'])}"
        if reading_state['series_number'] != -1:
            series_info += f" №{reading_state['series_number']}"
        response_text += f"{series_info}_\n"
    if reading_state['author']:
        response_text += f"_{escape_markdown(reading_state['author'])}_\n\n"
    response_text += page_text
    response_text += f"\n\n_Страница {prev_page_number + 1} из {total_pages}_"
    
    bot.edit_message_text(response_text, chat_id, call.message.message_id, reply_markup=get_reading_keyboard(book_id, total_pages, prev_page_number), parse_mode="MarkdownV2")
    bot.answer_callback_query(call.id)
    save_user_state(chat_id, reading_state['title'], reading_state['author'], reading_state['series'], reading_state['series_number'], reading_state['content'], prev_page_number, total_pages)
    


@bot.callback_query_handler(func=lambda call: call.data == 'my_books')
@bot.message_handler(func=lambda message: message.text == 'Мои книги')
def handle_my_books(update):
    is_callback = hasattr(update, 'data')

    if is_callback:
        chat_id = update.message.chat.id
    else:
        chat_id = update.chat.id
    
    books = get_user_books(chat_id)
    
    if not books:
        text = "У вас пока нет сохраненных книг\\. Отправьте мне FB2\\-файл, чтобы начать читать\\. \n\nВы можете воспользоваться поиском\\, найти книгу и начать ее читать в данном боте \\(одновременно можно читать до 10 книг\\)\\."
        if is_callback:
            bot.edit_message_text(text, chat_id, update.message.message_id, parse_mode="MarkdownV2")
            bot.answer_callback_query(update.id)
        else:
            bot.send_message(chat_id, text, parse_mode="MarkdownV2")
        return

    # Отправляем сообщение-заголовок
    text_header = "Вот ваши книги\\. Выберите одну, чтобы продолжить читать или удалить\\:"
    if is_callback:
        bot.edit_message_text(text_header, chat_id, update.message.message_id, parse_mode="MarkdownV2")
    else:
        bot.send_message(chat_id, text_header, parse_mode="MarkdownV2")

    # Отправляем отдельное сообщение для каждой книги с кнопками
    for book_id, title, author, series, series_number, page, total_pages in books:
        text = f"**{escape_markdown(title)}**\n"
        if series and series != "Нет серии":
            series_info = f"_{escape_markdown(series)}"
            if series_number != -1:
                series_info += f" №{series_number}"
            text += f"{series_info}_\n"
        if author:
            text += f"_{escape_markdown(author)}_\n"
        text += f"_{escape_markdown(f'Страница {page + 1} из {total_pages}')}_"
        
        bot.send_message(chat_id, text, reply_markup=get_book_actions_keyboard(book_id), parse_mode="MarkdownV2")
    
    if is_callback:
        bot.answer_callback_query(update.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith('read_book:'))
def handle_read_book_callback(call):
    chat_id = call.message.chat.id
    short_book_id = call.data.split(':')[1]
    
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute('SELECT book_id FROM reading_sessions WHERE user_id = ? AND book_id LIKE ?', (chat_id, f"{short_book_id}%"))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        bot.answer_callback_query(call.id, "Книга не найдена\\.")
        return
        
    book_id = result[0]

    reading_state = load_user_state(chat_id, book_id)
    if not reading_state:
        bot.answer_callback_query(call.id, "Книга не найдена\\.")
        return
    
    current_page = reading_state['current_page']
    total_pages = reading_state['total_pages']

    page_text = get_page_text(reading_state['content'], current_page)
    
    response_text = f"**Продолжаем читать:** {escape_markdown(reading_state['title'])}\n"
    if reading_state['series'] and reading_state['series'] != "Нет серии":
        series_info = f"_{escape_markdown(reading_state['series'])}"
        if reading_state['series_number'] != -1:
            series_info += f" №{reading_state['series_number']}"
        response_text += f"{series_info}_\n"
    if reading_state['author']:
        response_text += f"_{escape_markdown(reading_state['author'])}_\n\n"
    response_text += page_text
    response_text += f"\n\n_Страница {current_page + 1} из {total_pages}_"
    
    bot.edit_message_text(response_text, chat_id, call.message.message_id, reply_markup=get_reading_keyboard(book_id, total_pages, current_page), parse_mode="MarkdownV2")
    bot.answer_callback_query(call.id)
    
@bot.callback_query_handler(func=lambda call: call.data.startswith('delete_book:'))
def handle_delete_book_callback(call):
    chat_id = call.message.chat.id
    short_book_id = call.data.split(':')[1]
    
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute('SELECT book_id FROM reading_sessions WHERE user_id = ? AND book_id LIKE ?', (chat_id, f"{short_book_id}%"))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        bot.answer_callback_query(call.id, "Книга не найдена\\.")
        return
        
    book_id = result[0]
    
    delete_user_book(chat_id, book_id)
    bot.send_message(chat_id, "Книга успешно удалена\\.", parse_mode="MarkdownV2")
    bot.answer_callback_query(call.id)


# =================================================================
# ФУНКЦИИ ИЗ НАШЕЙ ПРОГРАММЫ
# =================================================================
def normalize_query(text):
    """
    Normalizes a string by replacing double letters with single ones.
    Example: 'ss' -> 's', 'pp' -> 'p'.
    """
    # This regex matches any letter (a-z) followed by the same letter.
    # It replaces the pair with a single instance of the letter.
    return re.sub(r'(.)\1+', r'\1', text.lower())

def load_inpx_data(inpx_path):
    """Загружает и парсит данные из всех INP-файлов."""
    global books_data
    books_data = []
    try:
        with zipfile.ZipFile(inpx_path, 'r') as archive:
            inp_files = [f for f in archive.namelist() if f.lower().endswith('.inp')]
            if not inp_files:
                logger.error("Ошибка: В INPX-архиве не найдено ни одного .inp файла.")
                return False

            logger.info(f"Найдено {len(inp_files)} INP-файлов. Загрузка...")
            for inp_file_name in inp_files:
                with archive.open(inp_file_name) as inp_file:
                    for line in inp_file:
                        try:
                            decoded_line = line.decode('utf-8', errors='ignore').strip()
                            parts = decoded_line.split('')
                            if len(parts) >= len(FIELDS):
                                book_info = dict(zip(FIELDS, parts))
                                
                                if ':' in book_info['AUTHOR']:
                                    book_info['AUTHOR'] = book_info['AUTHOR'].replace(':', '')
                                if ':' in book_info['GENRE']:
                                    book_info['GENRE'] = book_info['GENRE'].replace(':', '')

                                book_info['INP_ARCHIVE_NAME'] = inp_file_name.replace('.inp', '.zip')
                                books_data.append(book_info)
                        except (UnicodeDecodeError, IndexError, ValueError):
                            continue
    except (FileNotFoundError, zipfile.BadZipFile) as e:
        logger.error(f"Ошибка при загрузке каталога: {e}")
        return False
    return True

def search_book(books_data, author, title, series, series_number, date):
    """
    Ищет книгу в списке данных по заданным критериям.
    Возвращает список найденных книг.
    """
    results = []
    author = author.lower()
    title = title.lower()
    series = series.lower()
    series_number = series_number.lower()
    date = date.lower()

    for book in books_data:
        match = True
        
        if author and author not in book['AUTHOR'].lower():
            match = False
        
        if title and title not in book['TITLE'].lower():
            match = False
        
        if series and series not in book['SERIES'].lower():
            match = False
            
        if series_number and series_number not in book['SERNO'].lower():
            match = False
            
        if date and date not in book['DATE'].lower():
            match = False
            
        if match:
            results.append(book)
            
    # Сортировка результатов по номеру серии
    results.sort(key=lambda x: int(x.get('SERNO', '0')) if x.get('SERNO', '0').isdigit() else float('inf'))
            
    return results

def search_book_smart(books_data, query):
    """
    Ищет книгу по одному запросу, ищет совпадения в авторе, названии, серии и номере серии,
    с учетом нормализации двойных букв.
    """
    results = []
    # Normalize the user's query
    normalized_query = normalize_query(query)
    query_parts = normalized_query.split()

    for book in books_data:
        # Create a search string from book info and normalize it
        search_string = f"{book['AUTHOR']} {book['TITLE']} {book['SERIES']} {book['SERNO']}".lower()
        normalized_search_string = normalize_query(search_string)
        
        # Check if all parts of the normalized query are in the normalized search string
        if all(part in normalized_search_string for part in query_parts):
            results.append(book)
            
    # Сортировка результатов по номеру серии
    results.sort(key=lambda x: int(x.get('SERNO', '0')) if x.get('SERNO', '0').isdigit() else float('inf'))
            
            
    return results

def sanitize_filename(filename):
    """
    Очищает имя файла от запрещенных в Windows символов, заменяя их одним подчеркиванием.
    """
    # Паттерн для поиска одного или более запрещенных символов
    forbidden_chars = r'[<>:"/\|?*\s,;%&№]+'
    
    # Заменяем все последовательности запрещенных символов на одно подчеркивание
    sanitized_name = re.sub(forbidden_chars, '_', filename)
    
    # Удаляем ведущие и завершающие подчеркивания, если они есть
    sanitized_name = sanitized_name.strip('_')
    return sanitized_name

def get_book_file(book_info):
    """
    Находит и извлекает файл книги из соответствующего ZIP-архива.
    Возвращает путь к сохраненному файлу или None в случае ошибки.
    """
    file_name_in_zip = f"{book_info['FILE']}.{book_info['EXT']}"
    archive_name = book_info['INP_ARCHIVE_NAME']
    archive_path = os.path.join(BOOKS_DIR, 'lib.rus.ec', archive_name)
    
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    
    # Формируем имя файла из названия книги и расширения
    title_part = book_info['TITLE']
    extension_part = book_info['EXT']
    
    # Очищаем имя файла от запрещенных символов
    sanitized_title = sanitize_filename(title_part)
    # Определяем максимальную безопасную длину имени файла (например, 200 символов)
    # Это позволяет избежать ошибки WinError 3
    max_len = 100 - len(f".{extension_part}")
    
    if len(sanitized_title) > max_len:
        # Если имя слишком длинное, обрезаем его
        truncated_title = sanitized_title[:max_len]
        temp_filename = f"{truncated_title}.{extension_part}"
        logger.warning(f"Имя файла было слишком длинным и обрезано: {temp_filename}")
    else:
        temp_filename = f"{sanitized_title}.{extension_part}"
        
    output_path = os.path.join(DOWNLOAD_FOLDER, temp_filename)
    
    try:
        with zipfile.ZipFile(archive_path, 'r') as archive:
            # Извлекаем файл под его оригинальным именем
            extracted_file_path = os.path.join(DOWNLOAD_FOLDER, file_name_in_zip)
            archive.extract(file_name_in_zip, DOWNLOAD_FOLDER)
            
            # Если файл успешно извлечен, переименовываем его в новое имя
            if os.path.exists(extracted_file_path):
                os.rename(extracted_file_path, output_path)
                logger.info(f"Файл '{file_name_in_zip}' успешно извлечен и переименован в '{temp_filename}'.")
                return output_path
            else:
                logger.error(f"Не удалось найти извлеченный файл: {extracted_file_path} из архива {archive_name}")
                return None
    except (FileNotFoundError, KeyError, Exception) as e:
        logger.error(f"Ошибка при извлечении файла '{file_name_in_zip}' из архива '{archive_name}': {e}")
        return None

def get_dir_size_gb(path):
    """
    Рекурсивно вычисляет общий размер всех файлов в папке (в ГБ).
    """
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    except FileNotFoundError:
        logger.error(f"Папка не найдена: {path}")
        return 0
    except Exception as e:
        logger.error(f"Ошибка при расчете размера папки: {e}")
        return 0
        
    return total_size / (1024 * 1024 * 1024)

# =================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С ID ПОЛЬЗОВАТЕЛЕЙ
# =================================================================
def load_users():
    """Загружает ID пользователей из JSON-файла."""
    global registered_users
    if os.path.exists(USERS_JSON_FILE):
        try:
            with open(USERS_JSON_FILE, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
                if isinstance(loaded_data, dict):
                    registered_users = loaded_data
                else:
                    logger.warning("Файл пользователей имеет неверный формат. Создаем новый.")
                    registered_users = {}
            logger.info(f"Загружено {len(registered_users)} пользователей из {USERS_JSON_FILE}.")
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Ошибка при чтении JSON-файла пользователей: {e}. Начинаем с пустого списка.")
            registered_users = {}
    else:
        logger.info(f"Файл {USERS_JSON_FILE} не найден. Начинаем с пустого списка пользователей.")
        registered_users = {}

def save_users():
    """Сохраняет ID пользователей в JSON-файл."""
    try:
        with open(USERS_JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(registered_users, f, indent=4, ensure_ascii=False)
        logger.info(f"Сохранено {len(registered_users)} пользователей в {USERS_JSON_FILE}.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении пользователей: {e}")

def load_pending_users():
    """Загружает ID ожидающих одобрения пользователей из JSON-файла."""
    global pending_users
    if os.path.exists(PENDING_USERS_JSON_FILE):
        try:
            with open(PENDING_USERS_JSON_FILE, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
                if isinstance(loaded_data, dict):
                    pending_users = {int(k): v for k, v in loaded_data.items()}
                else:
                    logger.warning("Файл ожидающих пользователей имеет неверный формат. Создаем новый.")
                    pending_users = {}
            logger.info(f"Загружено {len(pending_users)} ожидающих пользователей из {PENDING_USERS_JSON_FILE}.")
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Ошибка при чтении JSON-файла ожидающих пользователей: {e}. Начинаем с пустого списка.")
            pending_users = {}
    else:
        logger.info(f"Файл {PENDING_USERS_JSON_FILE} не найден. Начинаем с пустого списка ожидающих пользователей.")
        pending_users = {}

def save_pending_users():
    """Сохраняет ID ожидающих одобрения пользователей в JSON-файл."""
    try:
        with open(PENDING_USERS_JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(pending_users, f, indent=4, ensure_ascii=False)
        logger.info(f"Сохранено {len(pending_users)} ожидающих пользователей в {PENDING_USERS_JSON_FILE}.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении ожидающих пользователей: {e}")

def approve_user(user_id):
    """Одобряет пользователя, перемещая его из pending_users в registered_users."""
    if user_id in pending_users:
        user_info = pending_users[user_id]
        
        del pending_users[user_id]
        
        registered_users[str(user_id)] = {
            'username': user_info.get('username'),
            'first_name': user_info.get('first_name'),
            'last_name': user_info.get('last_name')
        }
        
        save_pending_users()
        save_users()
        logger.info(f"Пользователь {user_id} одобрен и добавлен в список зарегистрированных.")
        return True
    return False
    
def reject_user(user_id):
    """Отклоняет заявку пользователя, удаляя его из pending_users."""
    if user_id in pending_users:
        del pending_users[user_id]
        save_pending_users()
        logger.info(f"Заявка пользователя {user_id} отклонена и удалена из списка ожидающих.")
        return True
    return False

def remove_user(user_id):
    """Удаляет пользователя из списка одобренных."""
    if str(user_id) in registered_users:
        del registered_users[str(user_id)]
        save_users()
        logger.info(f"Пользователь {user_id} удален из списка зарегистрированных.")
        return True
    return False

# =================================================================
# ОБРАБОТЧИКИ КОМАНД И СООБЩЕНИЙ TELEGRAM-БОТА
# =================================================================
user_keyboard = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
user_keyboard.row('Инфо', 'Мои книги')
user_keyboard.row('Умный поиск', 'Последовательный поиск')

admin_keyboard = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
admin_keyboard.row('Инфо', 'Мои книги')
admin_keyboard.row('Умный поиск', 'Последовательный поиск')
admin_keyboard.row('Список пользователей', 'Заявки на одобрение')
admin_keyboard.row('Перезапустить бота')

def get_keyboard(user_id):
    """Возвращает соответствующую клавиатуру в зависимости от ID пользователя."""
    return admin_keyboard if is_user_admin(user_id) else user_keyboard

def check_for_button_press(message):
    """Проверяет, было ли нажатие кнопки и переключает на нужный обработчик."""
    if not is_user_approved(message.from_user.id):
        return True

    if message.text == 'Инфо':
        bot.send_message(message.chat.id, "Поиск прерван.", reply_markup=get_keyboard(message.from_user.id))
        handle_info_button(message)
        return True
    elif message.text == 'Умный поиск':
        bot.send_message(message.chat.id, "Поиск прерван. Начинаем умный поиск.", reply_markup=get_keyboard(message.from_user.id))
        handle_smart_find_button(message)
        return True
    elif message.text == 'Последовательный поиск':
        bot.send_message(message.chat.id, "Поиск прерван. Начинаем последовательный поиск.", reply_markup=get_keyboard(message.from_user.id))
        handle_sequential_find_button(message)
        return True
    elif message.text == 'Список пользователей':
        bot.send_message(message.chat.id, "Поиск прерван.", reply_markup=get_keyboard(message.from_user.id))
        handle_list_users(message)
        return True
    elif message.text == 'Заявки на одобрение':
        bot.send_message(message.chat.id, "Поиск прерван.", reply_markup=get_keyboard(message.from_user.id))
        handle_list_pending(message)
        return True
    elif message.text == 'Перезапустить бота':
        bot.send_message(message.chat.id, "Поиск прерван.", reply_markup=get_keyboard(message.from_user.id))
        handle_admin_restart(message)
        return True
    elif message.text == 'Мои книги':
        bot.send_message(message.chat.id, "Поиск прерван.", reply_markup=get_keyboard(message.from_user.id))
        handle_my_books(message)
        return True
    return False

# захват ссылок с Либрусека
@bot.message_handler(func=lambda message: is_user_approved(message.from_user.id) and re.search(r'lib\.rus\.ec/b/(\d+)', message.text))
def handle_librus_link_message(message):
    chat_id = message.chat.id
    
    # Avoid processing the same link multiple times or while another action is in progress
    if chat_id in is_processing_link and is_processing_link[chat_id]:
        return
        
    is_processing_link[chat_id] = True

    try:
        link = message.text
        # Изменено регулярное выражение для поиска 'lib.rus.ec'
        match = re.search(r'lib\.rus\.ec/b/(\d+)', link)

        if not match:
            # This should ideally not happen due to the handler's func, but as a fallback
            bot.send_message(chat_id, "Неверный формат ссылки. Пожалуйста, введите ссылку в формате `http://lib.rus.ec/b/XXXXXX`.")
            is_processing_link[chat_id] = False
            return

        libid = match.group(1)
        logger.info(f"Пользователь {chat_id} отправил ссылку, найден LIBID: {libid}")

        selected_book = next((book for book in books_data if book['LIBID'] == libid), None)

        if not selected_book:
            bot.send_message(chat_id, "Произошла ошибка: книга с таким LIBID не найдена в базе.", reply_markup=get_keyboard(chat_id))
            is_processing_link[chat_id] = False
            return

        bot.send_message(chat_id, f"Начинаю скачивание книги: {selected_book['TITLE']}...")
        file_path = get_book_file(selected_book)

        if file_path and os.path.exists(file_path):
            try:
                with open(file_path, 'rb') as book_file:
                    full_filename = f"Автор: {selected_book['AUTHOR']}\nНазвание книги: {selected_book['TITLE']}\nСерия: {selected_book['SERIES']}\nНомер в серии: {selected_book['SERNO']}"
                    bot.send_document(chat_id, book_file, caption=f"{full_filename}\nСсылка на сайт: [link](http://lib.rus.ec/b/{libid})", parse_mode="Markdown")

                # --- добавляем кнопку "Читать книгу" ---
                keyboard = InlineKeyboardMarkup()
                keyboard.add(InlineKeyboardButton("📕 Читать книгу", callback_data=f"add_book:{selected_book['LIBID']}"))                    
                    
                    
                bot.send_message(
                    chat_id,
                    "Книга отправлена. \nЗагрузите данный файл на ваше устройство и откройте читалкой FB2 файлов. \n\n"
                    "Также вы можете выбрать другую книгу из списка выше, либо начать новый поиск.\n\n"
                    "Или воспользуйтесь встроенным ридером:",
                    reply_markup=keyboard
                )
                
                
                
                logger.info(f"Файл книги '{selected_book['TITLE']}' успешно отправлен пользователю {chat_id}.")
            except ApiTelegramException as e:
                logger.error(f"Telegram API Error while sending file to {chat_id}: {e}")
                bot.send_message(chat_id, "Произошла ошибка при отправке файла. Возможно, он слишком большой.", reply_markup=get_keyboard(chat_id))
            except Exception as e:
                logger.error(f"Ошибка при отправке файла '{selected_book['TITLE']}': {e}")
                bot.send_message(chat_id, f"Произошла ошибка при отправке файла: {e}", reply_markup=get_keyboard(chat_id))
            finally:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Файл '{selected_book['TITLE']}' удален.")
        else:
            bot.send_message(chat_id, "Произошла ошибка при скачивании файла.", reply_markup=get_keyboard(chat_id))

    finally:
        is_processing_link[chat_id] = False


@bot.message_handler(commands=['start'])
def handle_start(message):
    user_id = message.from_user.id
    if is_user_approved(user_id):
        handle_info_button(message)
    elif user_id not in pending_users:
        pending_users[user_id] = {
            'username': message.from_user.username,
            'first_name': message.from_user.first_name,
            'last_name': message.from_user.last_name,
            'request_time': datetime.now().isoformat()
        }
        save_pending_users()
        
        bot.send_message(user_id, "Здравствуйте! Доступ к этому боту ограничен. \n\nВаша заявка отправлена администратору на рассмотрение. Пожалуйста, ожидайте.")
        
        for admin_id in ADMIN_IDS:
            try:
                username = pending_users[user_id]['username'] or "N/A"
                first_name = pending_users[user_id]['first_name'] or "N/A"
                last_name = pending_users[user_id]['last_name'] or ""
                
                escaped_username = username.replace('_', r'\_')
                
                admin_message = (
                    f"🔔 **Новая заявка на одобрение!**\n\n"
                    f"**ID:** `{user_id}`\n"
                    f"**Username:** @{escaped_username}\n"
                    f"**Имя:** {first_name} {last_name}"
                )
                
                keyboard = InlineKeyboardMarkup()
                approve_button = InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{user_id}")
                reject_button = InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject:{user_id}")
                keyboard.add(approve_button, reject_button)
                
                bot.send_message(admin_id, admin_message, parse_mode="Markdown", reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление администратору {admin_id}: {e}")
    else:
        bot.send_message(user_id, "Ваша заявка уже на рассмотрении. Пожалуйста, ожидайте.")

@bot.message_handler(func=lambda message: message.text == 'Перезапустить бота' and is_user_admin(message.from_user.id))
def handle_admin_restart(message):
    """Обработчик кнопки 'Перезапустить бота' (доступно только админам)."""
    user_id = message.from_user.id
    logger.info(f"Администратор {user_id} запросил перезапуск.")
    bot.send_message(user_id, "Перезапускаю бота...")
    os.execv(sys.executable, ['python'] + sys.argv)

@bot.message_handler(func=lambda message: message.text == 'Список пользователей' and is_user_admin(message.from_user.id))
def handle_list_users(message):
    """Показывает список одобренных пользователей с кнопками для удаления."""
    user_list_ids = sorted(registered_users.keys())
    if not user_list_ids:
        bot.send_message(message.chat.id, "Список одобренных пользователей пуст.")
        return

    bot.send_message(message.chat.id, "Одобренные пользователи:")

    for user_id_str in user_list_ids:
        user_id = int(user_id_str)
        user_info = registered_users[user_id_str]
        
        username = user_info.get('username') or "N/A"
        first_name = user_info.get('first_name') or ""
        last_name = user_info.get('last_name') or ""
        
        escaped_username = username.replace('_', r'\_')

        user_text = f"- ID: `{user_id}` | Имя: {first_name} {last_name} | Username: @{escaped_username}"
        
        keyboard = InlineKeyboardMarkup()
        remove_button = InlineKeyboardButton(text=f"❌ Удалить", callback_data=f"remove_user:{user_id}")
        keyboard.add(remove_button)
        
        bot.send_message(message.chat.id, user_text, reply_markup=keyboard, parse_mode="Markdown")

@bot.message_handler(func=lambda message: message.text == 'Заявки на одобрение' and is_user_admin(message.from_user.id))
def handle_list_pending(message):
    """Показывает список ожидающих одобрения пользователей с кнопками для одобрения и отклонения."""
    pending_list = sorted(pending_users.keys())
    if not pending_list:
        bot.send_message(message.chat.id, "Нет новых заявок на одобрение.")
        return

    bot.send_message(message.chat.id, "Заявки на одобрение:")

    for user_id in pending_list:
        user_info = pending_users.get(user_id, {})
        username = user_info.get('username') or 'N/A'
        first_name = user_info.get('first_name') or 'N/A'
        
        escaped_username = username.replace('_', r'\_')

        user_text = f"**ID:** `{user_id}` | **Имя:** {first_name} | **Username:** @{escaped_username}"
        
        keyboard = InlineKeyboardMarkup()
        approve_button = InlineKeyboardButton(text=f"✅ Одобрить", callback_data=f"approve:{user_id}")
        reject_button = InlineKeyboardButton(text=f"❌ Отклонить", callback_data=f"reject:{user_id}")
        keyboard.add(approve_button, reject_button)
        
        bot.send_message(message.chat.id, user_text, reply_markup=keyboard, parse_mode="Markdown")

@bot.message_handler(commands=['info'], func=lambda m: is_user_approved(m.from_user.id))
@bot.message_handler(func=lambda message: message.text == 'Инфо' and is_user_approved(message.from_user.id))
def handle_info_button(message):
    """Обработчик для кнопки 'Инфо'."""
    user_id = message.chat.id
    
    logger.info(f"Пользователь {user_id} запросил информацию о боте.")

    total_books_dir_size_gb = get_dir_size_gb(BOOKS_DIR)
    
    inpx_file_mod_time = os.path.getmtime(INPX_FILE)
    last_updated_date = datetime.fromtimestamp(inpx_file_mod_time)
    formatted_date = last_updated_date.strftime('%d.%m.%Y')

    response = (
        "Привет! \nЯ бот для поиска и скачивания книг из библиотеки LibRusEc.\n\n"
        "Библиотека [LibRusEc](http://lib.rus.ec/) - одна из самых больших сетевых библиотек художественной литературы на русском языке.\n\n"
        f"Используемая база книг: [База LibRusEc](https://booktracker.org/viewtopic.php?t=1198)\n"
        f"Год выпуска: 2009 - 2025 \nФормат книг: FB2 \n"
        f"Сейчас книг в базе: {len(books_data)}\n"
        f"Общий объем библиотеки: {total_books_dir_size_gb:.2f} ГБ\n"
        f"Дата последнего обновления базы: **{formatted_date}**\n"
        f"Всего пользователей бота: {len(registered_users)}\n\n"
        "Обновление раздачи планируется осуществлять в начале каждого календарного месяца путём добавления нового архива с книгами, в накопительном режиме.\n\n"
        "Последние новости по работе бота и его обновлениям, можно посмотреть и обсудить в [группе](https://t.me/flibusta_librusec/3/9).\n\n"
        "Чтобы начать поиск книги, нажми на одну из кнопок поиска ниже. \n\n"
        "Чем открыть файл FB2 можешь узнать тут: /reader \n\n"
        f"Если не нашел свою книгу в LibRusEc, можно поискать ее на [Flibusta](https://t.me/FlibustaBase_bot).\n\n"
        "Написать автору бота можно тут: [PostToMe](https://t.me/PostToMe_bot)\n\n"
        f"Отблагодарить автора бота можно донатом на кошелек TRC20: `TSCxhHQpSTpwwk8W1vJwPtyTm6Ep1eP5dd`"
    )
    bot.send_message(user_id, response, parse_mode="Markdown", reply_markup=get_keyboard(user_id), disable_web_page_preview=True)
    
@bot.message_handler(commands=['reader'], func=lambda m: is_user_approved(m.from_user.id))
def handle_reader_command(message):
    """
    Обработчик для команды /reader.
    Отправляет пользователю информацию о читалке.
    """
    chat_id = message.chat.id
    
    # Формируем текст сообщения
    response_text = (
        "📖 Как читать файлы формата FB2 \n\n"
        "Веб-приложение:\n Используйте онлайн-ридер для быстрого доступа к файлам FB2 без необходимости установки ПО: [ссылка](https://omnireader.ru/) \n\n"
        "Мобильные платформы (Android):\n Для удобного чтения на мобильных устройствах рекомендуется приложение FBReader Premium: [ссылка](https://t.me/files_to_you/7/8)\n\n"
        "Десктопные решения (Windows/Linux/macOS):\n Для настольных компьютеров доступна версия FBReader, которую можно загрузить с официального сайта: [ссылка](https://fbreader.org/windows)\n\n"
        "Интеграция с Telegram:\n Вы также можете использовать встроенный ридер этого бота, или специализированные боты-читалки для мгновенного доступа к файлам: [ссылка](https://t.me/Book_Reader_TG_bot)\n\n"
	)
    
    # Отправляем текстовое сообщение
    bot.send_message(chat_id, response_text, parse_mode="Markdown", reply_markup=get_keyboard(chat_id), disable_web_page_preview=True)
    
    logger.info(f"Пользователю {chat_id} отправлена информация о читалке.")

@bot.message_handler(func=lambda message: message.text == 'Последовательный поиск' and is_user_approved(message.from_user.id))
def handle_sequential_find_button(message):
    """Начинает пошаговый процесс поиска по нажатию кнопки 'Последовательный поиск'."""
    user_id = message.chat.id
    
    logger.info(f"Пользователь {user_id} начал последовательный поиск.")
    user_data[user_id] = {}
    msg = bot.send_message(user_id,
                           "Введите имя автора (можно не полностью).\n"
                           "Для пропуска введите `-`.",
                           reply_markup=get_keyboard(user_id))
    bot.register_next_step_handler(msg, request_series)


def request_series(message):
    """Запрашивает название серии."""
    if not is_user_approved(message.from_user.id):
        return

    if check_for_button_press(message):
        return

    chat_id = message.chat.id
    user_data[chat_id]['author'] = message.text if message.text != '-' else ''
    logger.info(f"Последовательный поиск: пользователь {chat_id} ввёл автора: '{user_data[chat_id]['author']}'")
    msg = bot.send_message(chat_id,
                           "Введите название серии (можно не полностью).\n"
                           "Для пропуска введите `-`.",
                           reply_markup=get_keyboard(chat_id))
    bot.register_next_step_handler(msg, request_book_number)

def request_book_number(message):
    """Запрашивает номер книги в серии."""
    if not is_user_approved(message.from_user.id):
        return

    if check_for_button_press(message):
        return

    chat_id = message.chat.id
    user_data[chat_id]['series'] = message.text if message.text != '-' else ''
    logger.info(f"Последовательный поиск: пользователь {chat_id} ввёл серию: '{user_data[chat_id]['series']}'")
    msg = bot.send_message(chat_id,
                           "Введите номер книги в серии (можно не полностью).\n"
                           "Например, `1` или `3-4`. Для пропуска введите `-`.",
                           reply_markup=get_keyboard(chat_id))
    bot.register_next_step_handler(msg, request_publish_year)

def request_publish_year(message):
    """Запрашивает год издания книги."""
    if not is_user_approved(message.from_user.id):
        return
    
    if check_for_button_press(message):
        return
            
    chat_id = message.chat.id
    user_data[chat_id]['series_number'] = message.text if message.text != '-' else ''
    logger.info(f"Последовательный поиск: пользователь {chat_id} ввёл номер издания: '{user_data[chat_id]['series_number']}'")
    msg = bot.send_message(chat_id,
                           "Введите год издания книги (можно не полностью).\n"
                           "Например, `2024`. Для пропуска введите `-`.",
                           reply_markup=get_keyboard(chat_id))
    bot.register_next_step_handler(msg, request_title)

def request_title(message):
    """Запрашивает название книги."""
    if not is_user_approved(message.from_user.id):
        return
    
    if check_for_button_press(message):
        return
            
    chat_id = message.chat.id
    user_data[chat_id]['date'] = message.text if message.text != '-' else ''
    logger.info(f"Последовательный поиск: пользователь {chat_id} ввёл год издания: '{user_data[chat_id]['date']}'")
    msg = bot.send_message(chat_id,
                           "Введите название книги (можно не полностью).\n"
                           "Для пропуска введите `-`.",
                           reply_markup=get_keyboard(chat_id))
    bot.register_next_step_handler(msg, process_sequential_search)

def process_sequential_search(message):
    """Выполняет пошаговый поиск и выводит результаты."""
    if not is_user_approved(message.from_user.id):
        return

    if check_for_button_press(message):
        return

    chat_id = message.chat.id
    user_data[chat_id]['title'] = message.text if message.text != '-' else ''
    
    author = user_data[chat_id].get('author', '')
    series = user_data[chat_id].get('series', '')
    series_number = user_data[chat_id].get('series_number', '')
    date = user_data[chat_id].get('date', '')
    title = user_data[chat_id].get('title', '')
    
    user_data.pop(chat_id, None)
    logger.info(f"Последовательный поиск: пользователь {chat_id} ввёл название: '{title}'. Итоговый запрос: Автор='{author}', Серия='{series}', Номер серии='{series_number}', Год='{date}', Название='{title}'")

    if not any([author, series, title, series_number, date]):
        bot.send_message(chat_id, "Вы не ввели ни одного критерия для поиска. Попробуйте снова.", reply_markup=get_keyboard(chat_id))
        return

    bot.send_message(chat_id, "Ищу книги по вашим критериям...")
    found_books = search_book(books_data, author, title, series, series_number, date)
    
    user_search_results[chat_id] = {
        'results': found_books,
        'page': 0
    }
    display_results(chat_id)


@bot.message_handler(func=lambda message: message.text == 'Умный поиск' and is_user_approved(message.from_user.id))
def handle_smart_find_button(message):
    """Обработчик для кнопки 'Умный поиск'."""
    user_id = message.chat.id
    
    logger.info(f"Пользователь {user_id} начал умный поиск.")
    msg = bot.send_message(user_id,
                           "Введите ваш запрос в одном сообщении.\n"
                           "Например: `Глуховский Метро 2033` или `Метро #3`.",
                           reply_markup=get_keyboard(user_id))
    bot.register_next_step_handler(msg, process_smart_search_and_display)


def process_smart_search_and_display(message):
    """Выполняет умный поиск и выводит результаты."""
    if not is_user_approved(message.from_user.id):
        return

    if check_for_button_press(message):
        return

    chat_id = message.chat.id
    query = message.text
    
    if not query.strip() or query == '-':
        bot.send_message(chat_id, "Вы не ввели запрос для поиска. Попробуйте еще раз.", reply_markup=get_keyboard(chat_id))
        return

    logger.info(f"Умный поиск: пользователь {chat_id} ввёл запрос: '{query}'")
    bot.send_message(chat_id, f"Выполняю умный поиск по запросу: \"{query}\"...")
    found_books = search_book_smart(books_data, query)
    
    user_search_results[chat_id] = {
        'results': found_books,
        'page': 0
    }
    display_results(chat_id)


@bot.callback_query_handler(func=lambda call: call.data.startswith('download:'))
def handle_download_callback(call):
    chat_id = call.message.chat.id
    
    # Отправляем всплывающее уведомление, что скачивание началось.
    bot.answer_callback_query(call.id, text="Начинаю скачивание...")

    book_libid = call.data.split(':')[1]
    
    selected_book = next((book for book in books_data if book['LIBID'] == book_libid), None)
    
    if not selected_book:
        bot.send_message(chat_id, "Произошла ошибка: книга не найдена.")
        return

    # Запускаем извлечение файла
    file_path = get_book_file(selected_book)
    
    if file_path and os.path.exists(file_path):
        try:
            with open(file_path, 'rb') as book_file:
                link_book = selected_book['LIBID']
                full_filename = (
                    f"Автор: {selected_book['AUTHOR']}\n"
                    f"Название книги: {selected_book['TITLE']}\n"
                    f"Серия: {selected_book['SERIES']}\n"
                    f"Номер в серии: {selected_book['SERNO']}"
                )
                bot.send_document(
                    chat_id,
                    book_file,
                    caption=f"{full_filename}\nСсылка на сайт: [link](http://lib.rus.ec/b/{link_book})",
                    parse_mode="Markdown"
                )
            
            # --- добавляем кнопку "Читать книгу" ---
            keyboard = InlineKeyboardMarkup()
            keyboard.add(InlineKeyboardButton("📕 Читать книгу", callback_data=f"add_book:{selected_book['LIBID']}"))

            bot.send_message(
                chat_id,
                "Книга отправлена. \nЗагрузите данный файл на ваше устройство и откройте читалкой FB2 файлов. \n\n"
                "Также вы можете выбрать другую книгу из списка выше, либо начать новый поиск.\n\n"
                "Или воспользуйтесь встроенным ридером:",
                reply_markup=keyboard
            )

            logger.info(f"Файл книги '{selected_book['TITLE']}' успешно отправлен пользователю {chat_id}.")
        except Exception as e:
            logger.error(f"Ошибка при отправке файла '{selected_book['TITLE']}': {e}")
            bot.send_message(chat_id, f"Произошла ошибка при отправке файла: {e}", reply_markup=get_keyboard(chat_id))
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Файл '{selected_book['TITLE']}' удален.")
    else:
        bot.send_message(chat_id, "Произошла ошибка при скачивании файла.", reply_markup=get_keyboard(chat_id))


def process_and_save_book(chat_id, file_content):
    """
    Парсит FB2-файл, сохраняет его в базе данных и отправляет сообщение пользователю.
    """
    title, author, series, series_number, book_text = parse_fb2(file_content)
    
    if not book_text:
        bot.send_message(chat_id, "Не удалось прочитать книгу\\. Возможно, файл поврежден\\.", parse_mode="MarkdownV2")
        return
    
    total_pages = (len(book_text) + PAGE_SIZE - 1) // PAGE_SIZE
    
    save_result = save_user_state(chat_id, title, author, series, series_number, book_text, 0, total_pages)
    if save_result == 'limit_reached':
        bot.send_message(chat_id, f"Вы достигли лимита в {MAX_BOOKS} книг\\. Пожалуйста, удалите одну из старых книг с помощью команды /mybooks, чтобы добавить новую\\.", parse_mode="MarkdownV2")
        return
    
    book_id = hashlib.sha256(f"{chat_id}{title}{author}{series}{series_number}".encode('utf-8')).hexdigest()
    
    first_page_text = get_page_text(book_text, 0)
    
    response_text = f"**Начинаем читать:** {escape_markdown(title)}\n"
    if series and series != "Нет серии":
        series_info = f"_{escape_markdown(series)}"
        if series_number != -1:
            series_info += f" №{series_number}"
        response_text += f"{series_info}_\n"
    if author:
        response_text += f"_{escape_markdown(author)}_\n\n"
    response_text += first_page_text
    response_text += f"\n\n_Страница 1 из {total_pages}_"
    
    bot.send_message(chat_id, response_text, reply_markup=get_reading_keyboard(book_id, total_pages, 0), parse_mode="MarkdownV2")
    logger.info(f"Пользователь {chat_id} начал читать книгу '{title}'.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('add_book:'))
def handle_add_book_callback(call):
    chat_id = call.message.chat.id
    book_file_name = call.data.split(':')[1]
    
    # Найдите информацию о книге по имени файла
    book_info = next((book for book in books_data if book['FILE'] == book_file_name), None)
    
    if not book_info:
        bot.answer_callback_query(call.id, "Информация о книге не найдена.")
        return
        
    try:
        # Получаем путь к файлу книги
        book_path = get_book_file(book_info)
        if not book_path:
            bot.send_message(chat_id, "Не удалось найти файл книги. Попробуйте другой вариант.")
            bot.answer_callback_query(call.id)
            return

        with open(book_path, 'rb') as f:
            file_content = f.read()
        
        # Удаляем временный файл
        os.remove(book_path)
        
        # Парсим и сохраняем книгу
        process_and_save_book(chat_id, file_content)
        bot.answer_callback_query(call.id)

    except Exception as e:
        logger.error(f"Ошибка при обработке добавления книги: {e}")
        bot.send_message(chat_id, "Произошла ошибка при добавлении книги. Пожалуйста, попробуйте еще раз.")
        bot.answer_callback_query(call.id)
def display_results(chat_id):
    """
    Отправляет результаты поиска пользователю, обрабатывая пагинацию.
    """
    if chat_id not in user_search_results or not user_search_results[chat_id]['results']:
        logger.info(f"Для пользователя {chat_id} ничего не найдено.")
        bot.send_message(chat_id, "По вашему запросу ничего не найдено. Попробуйте еще раз.", reply_markup=get_keyboard(chat_id))
        user_search_results.pop(chat_id, None)
        return

    found_books = user_search_results[chat_id]['results']
    current_page = user_search_results[chat_id]['page']
    start_index = current_page * results_per_page
    end_index = start_index + results_per_page

    books_to_display = found_books[start_index:end_index]
    total_books = len(found_books)
    total_pages = (total_books + results_per_page - 1) // results_per_page

    response_text = f"Найдено {total_books} книг. Страница {current_page + 1} из {total_pages}:\n\n"
    
    download_keyboard = InlineKeyboardMarkup()
    download_buttons = []

    for i, book in enumerate(books_to_display):
        original_index = start_index + i + 1
        author_text = book['AUTHOR']
        title_text = book['TITLE']
        series_text = f" (Серия: {book['SERIES']}, #{book['SERNO']})" if book['SERIES'] else ""
        file_size_bytes = int(book.get('SIZE', 0))
        file_size_mb = file_size_bytes / (1024 * 1024)
        size_info = f" ({file_size_mb:.2f} МБ)" if file_size_bytes > 0 else ""
        
        book_id = book['LIBID']
        book_link = f"https://lib.rus.ec/b/{book_id}"
        
        book_line = f"{original_index}. <a href='{book_link}'>{author_text} - \"{title_text}\"</a>{series_text}{size_info}\n"
        response_text += book_line

        callback_data = f"download:{book['LIBID']}"
        download_buttons.append(InlineKeyboardButton(text=str(original_index), callback_data=callback_data))

    # --- ИЗМЕНЕНИЯ ЗДЕСЬ: Распределение кнопок по рядам ---
    buttons_per_row = 5
    for i in range(0, len(download_buttons), buttons_per_row):
        download_keyboard.row(*download_buttons[i:i + buttons_per_row])
    
    # --- ДОБАВЛЕНИЕ НОВОГО ТЕКСТА ---
    response_text += "\nДля скачки выбранной книги, нажмите кнопку с ее номером."

    navigation_buttons = []
    if current_page > 0:
        navigation_buttons.append(InlineKeyboardButton(text="⏪ Начало", callback_data=f"page:start:{chat_id}"))
    else:
        navigation_buttons.append(InlineKeyboardButton(text="⛔", callback_data="ignore"))

    if current_page > 0:
        navigation_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"page:prev:{chat_id}"))
    else:
        navigation_buttons.append(InlineKeyboardButton(text="⛔", callback_data="ignore"))

    if end_index < total_books:
        navigation_buttons.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"page:next:{chat_id}"))
    else:
        navigation_buttons.append(InlineKeyboardButton(text="⛔", callback_data="ignore"))

    if end_index < total_books:
        navigation_buttons.append(InlineKeyboardButton(text="Конец ⏩", callback_data=f"page:end:{chat_id}"))
    else:
        navigation_buttons.append(InlineKeyboardButton(text="⛔", callback_data="ignore"))
    
    download_keyboard.row(*navigation_buttons)

    bot.send_message(chat_id, response_text, reply_markup=download_keyboard, parse_mode="HTML", disable_web_page_preview=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith('page:'))
def handle_page_navigation(call):
    if not is_user_approved(call.from_user.id):
        bot.answer_callback_query(call.id, text="У вас нет доступа к этому боту.")
        return

    parts = call.data.split(':')
    action = parts[1]
    chat_id = int(parts[2])
    
    bot.delete_message(chat_id, call.message.message_id)
    
    if chat_id not in user_search_results:
        bot.answer_callback_query(call.id, text="Ошибка: Результаты поиска устарели.")
        return

    found_books = user_search_results[chat_id]['results']
    current_page = user_search_results[chat_id]['page']
    total_books = len(found_books)
    total_pages = (total_books + results_per_page - 1) // results_per_page
    
    if action == 'start':
        user_search_results[chat_id]['page'] = 0
    elif action == 'prev' and current_page > 0:
        user_search_results[chat_id]['page'] -= 1
    elif action == 'next' and current_page < total_pages - 1:
        user_search_results[chat_id]['page'] += 1
    elif action == 'end':
        user_search_results[chat_id]['page'] = total_pages - 1
    
    display_results(chat_id)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith('approve:'))
def handle_approve_callback(call):
    admin_id = call.from_user.id
    if not is_user_admin(admin_id):
        bot.answer_callback_query(call.id, text="У вас нет прав для этого действия.")
        return

    parts = call.data.split(':')
    user_to_approve_id = int(parts[1])
    
    if approve_user(user_to_approve_id):
        bot.edit_message_text(f"Заявка пользователя `{user_to_approve_id}` одобрена.",
                              chat_id=call.message.chat.id,
                              message_id=call.message.message_id,
                              parse_mode="Markdown")
        
        try:
            bot.send_message(user_to_approve_id, "Поздравляю! Ваша заявка одобрена. Теперь вы можете пользоваться ботом. Нажмите /start для начала.")
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение одобренному пользователю {user_to_approve_id}: {e}")
            
    else:
        bot.answer_callback_query(call.id, text="Пользователь уже одобрен или его заявка устарела.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('reject:'))
def handle_reject_callback(call):
    admin_id = call.from_user.id
    if not is_user_admin(admin_id):
        bot.answer_callback_query(call.id, text="У вас нет прав для этого действия.")
        return

    parts = call.data.split(':')
    user_to_reject_id = int(parts[1])
    
    if reject_user(user_to_reject_id):
        bot.edit_message_text(f"Заявка пользователя `{user_to_reject_id}` отклонена.",
                              chat_id=call.message.chat.id,
                              message_id=call.message.message_id,
                              parse_mode="Markdown")
        
        try:
            bot.send_message(user_to_reject_id, "К сожалению, ваша заявка на доступ к боту была отклонена администратором.")
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение отклоненному пользователю {user_to_reject_id}: {e}")
            
    else:
        bot.answer_callback_query(call.id, text="Пользователь уже не в списке ожидающих.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('remove_user:'))
def handle_remove_user_callback(call):
    admin_id = call.from_user.id
    if not is_user_admin(admin_id):
        bot.answer_callback_query(call.id, text="У вас нет прав для этого действия.")
        return

    user_to_remove_id = int(call.data.split(':')[1])
    if remove_user(user_to_remove_id):
        bot.edit_message_text(f"Пользователь `{user_to_remove_id}` удален из списка одобренных.",
                              chat_id=call.message.chat.id,
                              message_id=call.message.message_id,
                              parse_mode="Markdown")
        try:
            bot.send_message(user_to_remove_id, "Ваш доступ к боту был отозван администратором.")
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение удаленному пользователю {user_to_remove_id}: {e}")
    else:
        bot.answer_callback_query(call.id, text="Пользователь не найден в списке одобренных.")

@bot.message_handler(func=lambda message: message.text.isdigit() and message.chat.id in user_search_results)
def handle_book_selection(message):
    chat_id = message.chat.id
    if not is_user_approved(chat_id):
        return

    try:
        index = int(message.text) - 1
        found_books = user_search_results.get(chat_id)['results']
        
        if found_books and 0 <= index < len(found_books):
            selected_book = found_books[index]
            logger.info(f"Пользователь {chat_id} выбрал книгу: '{selected_book['TITLE']}' (ID: {index + 1})")
            bot.send_message(chat_id, f"Вы выбрали книгу: {selected_book['TITLE']}. Начинаю скачивание...")
            
            file_path = get_book_file(selected_book)
            if file_path and os.path.exists(file_path):
                with open(file_path, 'rb') as book_file:
                    bot.send_document(chat_id, book_file)
                os.remove(file_path)
                logger.info(f"Файл книги '{selected_book['TITLE']}' успешно отправлен пользователю {chat_id}.")
                bot.send_message(chat_id, "Готово! Можете выбрать другое действие.", reply_markup=get_keyboard(chat_id))
            else:
                logger.error(f"Не удалось отправить файл книги '{selected_book['TITLE']}' пользователю {chat_id}.")
                bot.send_message(chat_id, "Произошла ошибка при скачивании файла.", reply_markup=get_keyboard(chat_id))
        else:
            logger.warning(f"Пользователь {chat_id} ввёл неверный номер книги: {message.text}.")
            bot.send_message(chat_id, "Неверный номер книги. Пожалуйста, выберите номер из списка.", reply_markup=get_keyboard(chat_id))
            
    except (ValueError, IndexError):
        logger.warning(f"Пользователь {chat_id} ввёл нечисловой или некорректный номер: {message.text}.")
        bot.send_message(chat_id, "Неверный ввод. Пожалуйста, введите число.", reply_markup=get_keyboard(chat_id))
    finally:
        if chat_id in user_search_results:
            user_search_results.pop(chat_id, None)
            logger.debug(f"Очищены результаты поиска для пользователя {chat_id}.")

# =================================================================
# ЗАПУСК БОТА
# =================================================================
if __name__ == '__main__':
    logger.info("Запуск бота. Инициализация каталога библиотеки...")
    load_users()
    load_pending_users()
    if load_inpx_data(INPX_FILE):
        logger.info(f"Каталог загружен. Всего книг: {len(books_data)}.")
        logger.info("Бот запущен. Начните общение в Telegram.")
        while True:
            try:
                bot.polling(none_stop=True)
            except Exception as e:
                logger.error(f"Ошибка в основном цикле. Перезапускаю бота. Ошибка: {e}", exc_info=True)
                import time
                time.sleep(5)
    else:
        logger.error("Не удалось загрузить каталог. Бот не будет запущен.")