import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone, time

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    ConversationHandler,
    filters,
)

DB_NAME = "booking_v7.db"
MAX_BOOKINGS_PER_USER = 5
ADMIN_IDS = {8224923198}  # <-- замени на свой Telegram ID
UTC_PLUS_3 = timezone(timedelta(hours=3))

INSTRUCTOR_CONTACT_TEXT = "@dtdvld33"  # <-- замени на свой контакт
MORNING_REPORT_HOUR = 8
MORNING_REPORT_MINUTE = 0

# Состояния
PROFILE_NAME, PROFILE_PHONE, EDIT_NAME, EDIT_PHONE = range(4)
ADD_STUDENT_NAME, ADD_STUDENT_PHONE, ADD_STUDENT_COMMENT = range(4, 7)
ADD_BOOKING_QUERY = 7
FIND_STUDENT_QUERY = 8
MARKBOT_QUERY = 9


# -----------------------------
# DATABASE
# -----------------------------
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            full_name TEXT NOT NULL,
            phone TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS manual_students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            phone TEXT NOT NULL UNIQUE,
            comment TEXT,
            source_status TEXT DEFAULT 'offline',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slot_date TEXT NOT NULL,
            slot_time TEXT NOT NULL,
            is_booked INTEGER DEFAULT 0,
            booked_by_user_id INTEGER,
            booked_by_name TEXT,
            booked_by_phone TEXT,
            booked_source TEXT DEFAULT 'bot',
            reminder_24_sent INTEGER DEFAULT 0,
            reminder_2_sent INTEGER DEFAULT 0,
            confirm_sent INTEGER DEFAULT 0,
            confirm_status TEXT DEFAULT NULL,   -- confirmed / declined / pending / NULL
            morning_report_sent TEXT DEFAULT NULL, -- YYYY-MM-DD
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(slot_date, slot_time)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS waitlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slot_date TEXT NOT NULL,
            slot_time TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(slot_date, slot_time, user_id)
        )
    """)

    conn.commit()
    conn.close()


def db_execute(query, params=(), fetch=False, fetchone=False):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute(query, params)

    if fetchone:
        row = cur.fetchone()
        conn.close()
        return row

    if fetch:
        rows = cur.fetchall()
        conn.close()
        return rows

    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected


# -----------------------------
# HELPERS
# -----------------------------
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def now_msk():
    return datetime.now(UTC_PLUS_3)


def today_str():
    return now_msk().strftime("%Y-%m-%d")


def tomorrow_str():
    return (now_msk() + timedelta(days=1)).strftime("%Y-%m-%d")


def format_date_ru(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%d.%m.%Y")


def valid_phone(phone: str) -> bool:
    digits = re.sub(r"\D", "", phone)
    return len(digits) in (11, 12)


def normalize_phone(phone: str) -> str:
    return re.sub(r"\s+", " ", phone.strip())


def cleanup_past_slots():
    db_execute("""
        DELETE FROM waitlist
        WHERE slot_date < date('now')
    """)
    db_execute("""
        DELETE FROM slots
        WHERE slot_date < date('now')
    """)


def build_dates_keyboard(prefix: str, dates: list[str]) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(format_date_ru(d), callback_data=f"{prefix}|{d}")]
        for d in dates
    ]
    return InlineKeyboardMarkup(keyboard)


def build_times_keyboard(prefix: str, slot_date: str, times: list[str], back_cb: str) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(t, callback_data=f"{prefix}|{slot_date}|{t}")]
        for t in times
    ]
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(keyboard)


def main_menu_keyboard():
    keyboard = [
        ["📅 Записаться", "🗓 Свободные слоты"],
        ["🔥 Свободно сегодня", "📖 Мои записи"],
        ["❌ Отменить запись", "👤 Мой профиль"],
        ["☎ Связаться с инструктором", "🏠 Меню"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def user_help_text():
    return (
        "🚗 Бот записи на занятия\n\n"
        "Можно пользоваться кнопками внизу 👇\n\n"
        "Команды:\n"
        "/slots — свободные слоты\n"
        "/todayfree — свободно сегодня\n"
        "/book — записаться\n"
        "/mybookings — мои записи\n"
        "/cancel_booking — отменить запись\n"
        "/profile — мой профиль\n"
        "/editprofile — изменить профиль\n"
    )


def admin_help_text():
    return (
        "\nАдмин-команды:\n"
        "/quickslots — массово добавить слоты\n"
        "/genslots — сгенерировать слоты по будням\n"
        "/today — записи на сегодня\n"
        "/tomorrow — записи на завтра\n"
        "/week — записи на неделю\n"
        "/allslots — все слоты\n"
        "/addstudent — добавить ученика вручную\n"
        "/students — список учеников\n"
        "/findstudent — поиск ученика\n"
        "/addbooking — записать ученика вручную\n"
        "/markbot — отметить, что ученик перешёл в бота\n"
    )


# -----------------------------
# USERS
# -----------------------------
def get_user_profile(user_id: int):
    return db_execute("""
        SELECT user_id, full_name, phone
        FROM users
        WHERE user_id = ?
    """, (user_id,), fetchone=True)


def save_user_profile(user_id: int, full_name: str, phone: str):
    return db_execute("""
        INSERT INTO users (user_id, full_name, phone)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            full_name = excluded.full_name,
            phone = excluded.phone
    """, (user_id, full_name, phone))


def has_profile(user_id: int) -> bool:
    return get_user_profile(user_id) is not None


# -----------------------------
# MANUAL STUDENTS
# -----------------------------
def save_manual_student(full_name: str, phone: str, comment: str = ""):
    return db_execute("""
        INSERT INTO manual_students (full_name, phone, comment)
        VALUES (?, ?, ?)
        ON CONFLICT(phone) DO UPDATE SET
            full_name = excluded.full_name,
            comment = excluded.comment
    """, (full_name, phone, comment))


def get_manual_student_by_phone(phone: str):
    return db_execute("""
        SELECT id, full_name, phone, comment, source_status
        FROM manual_students
        WHERE phone = ?
    """, (phone,), fetchone=True)


def get_manual_students():
    return db_execute("""
        SELECT id, full_name, phone, comment, source_status
        FROM manual_students
        ORDER BY full_name
    """, fetch=True)


def find_manual_students(query: str):
    like_query = f"%{query.lower()}%"
    return db_execute("""
        SELECT id, full_name, phone, comment, source_status
        FROM manual_students
        WHERE LOWER(full_name) LIKE ?
           OR REPLACE(phone, ' ', '') LIKE REPLACE(?, ' ', '')
        ORDER BY full_name
    """, (like_query, query), fetch=True)


def mark_manual_student_as_bot(phone: str):
    return db_execute("""
        UPDATE manual_students
        SET source_status = 'bot'
        WHERE phone = ?
    """, (phone,))


# -----------------------------
# SLOTS
# -----------------------------
def add_slot(slot_date: str, slot_time: str):
    return db_execute(
        "INSERT INTO slots (slot_date, slot_time) VALUES (?, ?)",
        (slot_date, slot_time),
    )


def get_slot(slot_date: str, slot_time: str):
    return db_execute("""
        SELECT id, slot_date, slot_time, is_booked, booked_by_user_id, booked_by_name, booked_by_phone
        FROM slots
        WHERE slot_date = ? AND slot_time = ?
    """, (slot_date, slot_time), fetchone=True)


def get_free_dates():
    rows = db_execute("""
        SELECT DISTINCT slot_date
        FROM slots
        WHERE is_booked = 0
          AND slot_date >= date('now')
        ORDER BY slot_date
    """, fetch=True)
    return [r[0] for r in rows]


def get_free_today_times():
    rows = db_execute("""
        SELECT slot_time
        FROM slots
        WHERE slot_date = date('now')
          AND is_booked = 0
        ORDER BY slot_time
    """, fetch=True)
    return [r[0] for r in rows]


def get_free_times(slot_date: str):
    rows = db_execute("""
        SELECT slot_time
        FROM slots
        WHERE slot_date = ?
          AND is_booked = 0
        ORDER BY slot_time
    """, (slot_date,), fetch=True)
    return [r[0] for r in rows]


def get_user_bookings(user_id: int):
    return db_execute("""
        SELECT id, slot_date, slot_time
        FROM slots
        WHERE booked_by_user_id = ?
          AND is_booked = 1
        ORDER BY slot_date, slot_time
    """, (user_id,), fetch=True)


def count_user_active_bookings(user_id: int) -> int:
    row = db_execute("""
        SELECT COUNT(*)
        FROM slots
        WHERE booked_by_user_id = ?
          AND is_booked = 1
    """, (user_id,), fetchone=True)
    return row[0] if row else 0


def book_slot(slot_date: str, slot_time: str, user_id: int, full_name: str, phone: str):
    return db_execute("""
        UPDATE slots
        SET is_booked = 1,
            booked_by_user_id = ?,
            booked_by_name = ?,
            booked_by_phone = ?,
            booked_source = 'bot',
            reminder_24_sent = 0,
            reminder_2_sent = 0,
            confirm_sent = 0,
            confirm_status = NULL
        WHERE slot_date = ?
          AND slot_time = ?
          AND is_booked = 0
    """, (user_id, full_name, phone, slot_date, slot_time))


def admin_book_manual_slot(slot_date: str, slot_time: str, full_name: str, phone: str):
    return db_execute("""
        UPDATE slots
        SET is_booked = 1,
            booked_by_user_id = NULL,
            booked_by_name = ?,
            booked_by_phone = ?,
            booked_source = 'manual',
            reminder_24_sent = 0,
            reminder_2_sent = 0,
            confirm_sent = 0,
            confirm_status = NULL
        WHERE slot_date = ?
          AND slot_time = ?
          AND is_booked = 0
    """, (full_name, phone, slot_date, slot_time))


def cancel_booking(slot_date: str, slot_time: str, user_id: int):
    return db_execute("""
        UPDATE slots
        SET is_booked = 0,
            booked_by_user_id = NULL,
            booked_by_name = NULL,
            booked_by_phone = NULL,
            booked_source = 'bot',
            reminder_24_sent = 0,
            reminder_2_sent = 0,
            confirm_sent = 0,
            confirm_status = NULL
        WHERE slot_date = ?
          AND slot_time = ?
          AND booked_by_user_id = ?
          AND is_booked = 1
    """, (slot_date, slot_time, user_id))


def admin_release_slot(slot_date: str, slot_time: str):
    return db_execute("""
        UPDATE slots
        SET is_booked = 0,
            booked_by_user_id = NULL,
            booked_by_name = NULL,
            booked_by_phone = NULL,
            booked_source = 'bot',
            reminder_24_sent = 0,
            reminder_2_sent = 0,
            confirm_sent = 0,
            confirm_status = NULL
        WHERE slot_date = ?
          AND slot_time = ?
          AND is_booked = 1
    """, (slot_date, slot_time))


def mark_confirmation_sent(slot_id: int):
    db_execute("""
        UPDATE slots
        SET confirm_sent = 1,
            confirm_status = COALESCE(confirm_status, 'pending')
        WHERE id = ?
    """, (slot_id,))


def set_confirmation_status(slot_date: str, slot_time: str, status: str):
    db_execute("""
        UPDATE slots
        SET confirm_status = ?
        WHERE slot_date = ? AND slot_time = ?
    """, (status, slot_date, slot_time))


def get_bookings_by_date(slot_date: str):
    return db_execute("""
        SELECT id, slot_time, booked_by_name, booked_by_phone, booked_source, booked_by_user_id, confirm_status
        FROM slots
        WHERE slot_date = ?
          AND is_booked = 1
        ORDER BY slot_time
    """, (slot_date,), fetch=True)


def get_bookings_between(date_from: str, date_to: str):
    return db_execute("""
        SELECT slot_date, slot_time, booked_by_name, booked_by_phone, booked_source, confirm_status
        FROM slots
        WHERE slot_date >= ?
          AND slot_date <= ?
          AND is_booked = 1
        ORDER BY slot_date, slot_time
    """, (date_from, date_to), fetch=True)


def get_all_slots():
    return db_execute("""
        SELECT slot_date, slot_time, is_booked, booked_by_name, booked_by_phone, booked_source, confirm_status
        FROM slots
        ORDER BY slot_date, slot_time
    """, fetch=True)


# -----------------------------
# WAITLIST
# -----------------------------
def add_to_waitlist(slot_date: str, slot_time: str, user_id: int):
    return db_execute("""
        INSERT INTO waitlist (slot_date, slot_time, user_id)
        VALUES (?, ?, ?)
    """, (slot_date, slot_time, user_id))


def remove_waitlist_user(slot_date: str, slot_time: str, user_id: int):
    return db_execute("""
        DELETE FROM waitlist
        WHERE slot_date = ? AND slot_time = ? AND user_id = ?
    """, (slot_date, slot_time, user_id))


def clear_waitlist_for_slot(slot_date: str, slot_time: str):
    return db_execute("""
        DELETE FROM waitlist
        WHERE slot_date = ? AND slot_time = ?
    """, (slot_date, slot_time))


def get_waitlist_for_slot(slot_date: str, slot_time: str):
    return db_execute("""
        SELECT user_id
        FROM waitlist
        WHERE slot_date = ? AND slot_time = ?
        ORDER BY id
    """, (slot_date, slot_time), fetch=True)


def is_user_on_waitlist(slot_date: str, slot_time: str, user_id: int):
    row = db_execute("""
        SELECT id
        FROM waitlist
        WHERE slot_date = ? AND slot_time = ? AND user_id = ?
    """, (slot_date, slot_time, user_id), fetchone=True)
    return row is not None


# -----------------------------
# REMINDERS / CONFIRMATIONS / REPORTS
# -----------------------------
def get_pending_reminders(hours_before: int):
    now = now_msk()
    rows = db_execute("""
        SELECT id, slot_date, slot_time, booked_by_user_id,
               reminder_24_sent, reminder_2_sent
        FROM slots
        WHERE is_booked = 1
          AND booked_by_user_id IS NOT NULL
    """, fetch=True)

    result = []
    for slot_id, slot_date, slot_time, user_id, sent24, sent2 in rows:
        slot_dt = datetime.strptime(
            f"{slot_date} {slot_time}", "%Y-%m-%d %H:%M"
        ).replace(tzinfo=UTC_PLUS_3)

        delta = slot_dt - now
        hours_left = delta.total_seconds() / 3600

        if hours_before == 24 and 23 <= hours_left <= 24 and not sent24:
            result.append((slot_id, slot_date, slot_time, user_id))

        if hours_before == 2 and 1 <= hours_left <= 2 and not sent2:
            result.append((slot_id, slot_date, slot_time, user_id))

    return result


def mark_reminder_sent(slot_id: int, reminder_type: str):
    if reminder_type == "24":
        db_execute("UPDATE slots SET reminder_24_sent = 1 WHERE id = ?", (slot_id,))
    elif reminder_type == "2":
        db_execute("UPDATE slots SET reminder_2_sent = 1 WHERE id = ?", (slot_id,))


def get_pending_confirmations():
    now = now_msk()
    target_date = tomorrow_str()

    rows = db_execute("""
        SELECT id, slot_date, slot_time, booked_by_user_id, booked_by_name, confirm_sent
        FROM slots
        WHERE is_booked = 1
          AND booked_by_user_id IS NOT NULL
          AND slot_date = ?
    """, (target_date,), fetch=True)

    result = []
    for slot_id, slot_date, slot_time, user_id, booked_name, confirm_sent in rows:
        slot_dt = datetime.strptime(
            f"{slot_date} {slot_time}", "%Y-%m-%d %H:%M"
        ).replace(tzinfo=UTC_PLUS_3)
        delta = slot_dt - now
        hours_left = delta.total_seconds() / 3600

        if 18 <= hours_left <= 30 and not confirm_sent:
            result.append((slot_id, slot_date, slot_time, user_id, booked_name))

    return result


def get_today_report_rows():
    return get_bookings_by_date(today_str())


async def notify_waitlist(context: ContextTypes.DEFAULT_TYPE, slot_date: str, slot_time: str):
    users = get_waitlist_for_slot(slot_date, slot_time)
    if not users:
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Забрать слот", callback_data=f"bookslot|{slot_date}|{slot_time}")]
    ])

    for (user_id,) in users:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"🔥 Освободилось окно\n"
                    f"📅 {format_date_ru(slot_date)}\n"
                    f"🕒 {slot_time}\n\n"
                    f"Если хочешь, забирай слот:"
                ),
                reply_markup=keyboard,
            )
        except Exception:
            pass


async def reminder_24_job(context: ContextTypes.DEFAULT_TYPE):
    rows = get_pending_reminders(24)
    for slot_id, slot_date, slot_time, user_id in rows:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"⏰ Напоминание: у тебя занятие завтра\n"
                    f"📅 {format_date_ru(slot_date)}\n"
                    f"🕒 {slot_time}"
                ),
            )
            mark_reminder_sent(slot_id, "24")
        except Exception:
            pass


async def reminder_2_job(context: ContextTypes.DEFAULT_TYPE):
    rows = get_pending_reminders(2)
    for slot_id, slot_date, slot_time, user_id in rows:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"⏰ Напоминание: занятие скоро\n"
                    f"📅 {format_date_ru(slot_date)}\n"
                    f"🕒 {slot_time}\n"
                    f"До начала около 2 часов."
                ),
            )
            mark_reminder_sent(slot_id, "2")
        except Exception:
            pass


async def confirmation_job(context: ContextTypes.DEFAULT_TYPE):
    rows = get_pending_confirmations()
    for slot_id, slot_date, slot_time, user_id, booked_name in rows:
        try:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Приду", callback_data=f"confirm_yes|{slot_date}|{slot_time}")],
                [InlineKeyboardButton("❌ Не смогу", callback_data=f"confirm_no|{slot_date}|{slot_time}")],
            ])
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"Напоминание о занятии\n\n"
                    f"Завтра\n"
                    f"📅 {format_date_ru(slot_date)}\n"
                    f"🕒 {slot_time}\n\n"
                    f"Подтверди, пожалуйста, участие:"
                ),
                reply_markup=keyboard,
            )
            mark_confirmation_sent(slot_id)
        except Exception:
            pass


async def morning_report_job(context: ContextTypes.DEFAULT_TYPE):
    rows = get_today_report_rows()

    if not rows:
        text = "Доброе утро ☀️\n\nНа сегодня записей нет."
    else:
        text = "Доброе утро ☀️\n\nСегодняшние записи:\n\n"
        for _, slot_time, name, phone, source, _, confirm_status in rows:
            src = "офлайн" if source == "manual" else "бот"
            phone_text = f" | {phone}" if phone else ""
            confirm_text = f" | {confirm_status}" if confirm_status else ""
            text += f"• {slot_time} — {name}{phone_text} | {src}{confirm_text}\n"

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=text)
        except Exception:
            pass


async def cleanup_job(context: ContextTypes.DEFAULT_TYPE):
    cleanup_past_slots()


# -----------------------------
# REGISTRATION / PROFILE
# -----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cleanup_past_slots()
    user_id = update.effective_user.id

    if not has_profile(user_id):
        await update.message.reply_text(
            "Привет! Сначала давай оформим профиль.\n\n"
            "Введи имя и фамилию.\n"
            "Например: Иван Петров"
        )
        return PROFILE_NAME

    text = user_help_text()
    if is_admin(user_id):
        text += admin_help_text()

    await update.message.reply_text(text, reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def profile_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    full_name = update.message.text.strip()
    if len(full_name.split()) < 2:
        await update.message.reply_text("Напиши имя и фамилию. Например: Иван Петров")
        return PROFILE_NAME

    context.user_data["reg_full_name"] = full_name
    await update.message.reply_text("Теперь введи номер телефона. Например: +7 999 123-45-67")
    return PROFILE_PHONE


async def profile_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = normalize_phone(update.message.text)
    if not valid_phone(phone):
        await update.message.reply_text("Телефон выглядит неверно. Попробуй ещё раз.")
        return PROFILE_PHONE

    save_user_profile(update.effective_user.id, context.user_data["reg_full_name"], phone)
    context.user_data.clear()

    await update.message.reply_text(
        "✅ Профиль сохранён. Теперь можно пользоваться ботом: /book",
        reply_markup=main_menu_keyboard(),
    )
    return ConversationHandler.END


async def profile_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    profile = get_user_profile(update.effective_user.id)
    if not profile:
        await update.message.reply_text("Профиль ещё не заполнен. Нажми /start")
        return

    _, full_name, phone = profile
    await update.message.reply_text(
        f"Твой профиль:\n\nИмя: {full_name}\nТелефон: {phone}",
        reply_markup=main_menu_keyboard(),
    )


async def editprofile_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    profile = get_user_profile(update.effective_user.id)
    if not profile:
        await update.message.reply_text("Профиль ещё не заполнен. Нажми /start")
        return ConversationHandler.END

    await update.message.reply_text("Введи новое имя и фамилию.")
    return EDIT_NAME


async def edit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    full_name = update.message.text.strip()
    if len(full_name.split()) < 2:
        await update.message.reply_text("Напиши имя и фамилию. Например: Иван Петров")
        return EDIT_NAME

    context.user_data["edit_full_name"] = full_name
    await update.message.reply_text("Теперь введи новый номер телефона.")
    return EDIT_PHONE


async def edit_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = normalize_phone(update.message.text)
    if not valid_phone(phone):
        await update.message.reply_text("Телефон выглядит неверно. Попробуй ещё раз.")
        return EDIT_PHONE

    save_user_profile(update.effective_user.id, context.user_data["edit_full_name"], phone)
    context.user_data.clear()

    await update.message.reply_text("✅ Профиль обновлён.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def cancel_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Действие отменено.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


# -----------------------------
# USER COMMANDS
# -----------------------------
async def slots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cleanup_past_slots()
    dates = get_free_dates()
    if not dates:
        await update.message.reply_text("Свободных слотов пока нет.", reply_markup=main_menu_keyboard())
        return

    await update.message.reply_text(
        "Выбери дату:",
        reply_markup=build_dates_keyboard("showdate", dates),
    )


async def todayfree(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cleanup_past_slots()
    times = get_free_today_times()
    if not times:
        await update.message.reply_text("На сегодня свободных окон нет.", reply_markup=main_menu_keyboard())
        return

    keyboard = [
        [InlineKeyboardButton(t, callback_data=f"bookslot|{today_str()}|{t}")]
        for t in times
    ]
    await update.message.reply_text(
        f"🔥 Свободные окна сегодня ({format_date_ru(today_str())}):",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cleanup_past_slots()
    user_id = update.effective_user.id

    if not has_profile(user_id):
        await update.message.reply_text("Сначала заполни профиль через /start")
        return

    active_count = count_user_active_bookings(user_id)
    if active_count >= MAX_BOOKINGS_PER_USER:
        await update.message.reply_text(
            f"У тебя уже {active_count} активных записей. Лимит — {MAX_BOOKINGS_PER_USER}.",
            reply_markup=main_menu_keyboard(),
        )
        return

    dates = get_free_dates()
    if not dates:
        await update.message.reply_text("Свободных слотов пока нет.", reply_markup=main_menu_keyboard())
        return

    await update.message.reply_text(
        "📅 Выбери дату для записи:",
        reply_markup=build_dates_keyboard("bookdate", dates),
    )


async def mybookings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_user_bookings(update.effective_user.id)

    if not rows:
        await update.message.reply_text("У тебя пока нет активных записей.", reply_markup=main_menu_keyboard())
        return

    text = f"Твои записи ({len(rows)}/{MAX_BOOKINGS_PER_USER}):\n\n"
    for _, d, t in rows:
        text += f"• {format_date_ru(d)} {t}\n"

    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def cancel_booking_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_user_bookings(update.effective_user.id)

    if not rows:
        await update.message.reply_text("У тебя нет активных записей.", reply_markup=main_menu_keyboard())
        return

    keyboard = [
        [InlineKeyboardButton(f"{format_date_ru(d)} {t}", callback_data=f"cancel|{d}|{t}")]
        for _, d, t in rows
    ]

    await update.message.reply_text(
        "Выбери запись для отмены:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def contact_instructor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Связаться с инструктором:\n{INSTRUCTOR_CONTACT_TEXT}",
        reply_markup=main_menu_keyboard(),
    )


async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = user_help_text()
    if is_admin(update.effective_user.id):
        text += admin_help_text()
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


# -----------------------------
# CALLBACKS
# -----------------------------
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    user = update.effective_user
    cleanup_past_slots()

    if data.startswith("showdate|"):
        _, slot_date = data.split("|", 1)
        times = get_free_times(slot_date)

        text = f"Свободное время на {format_date_ru(slot_date)}:\n\n"
        if times:
            text += "\n".join([f"• {t}" for t in times])
        else:
            text += "Свободных окон нет."

        occupied_rows = db_execute("""
            SELECT slot_time
            FROM slots
            WHERE slot_date = ?
              AND is_booked = 1
            ORDER BY slot_time
        """, (slot_date,), fetch=True)

        keyboard_rows = []
        for (busy_time,) in occupied_rows:
            wait_text = f"🔔 Ждать {busy_time}"
            keyboard_rows.append([
                InlineKeyboardButton(wait_text, callback_data=f"waitlist|{slot_date}|{busy_time}")
            ])

        keyboard_rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_slots")])

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard_rows))
        return

    if data == "back_slots":
        dates = get_free_dates()
        if not dates:
            await query.edit_message_text("Свободных слотов пока нет.")
            return

        await query.edit_message_text(
            "Выбери дату:",
            reply_markup=build_dates_keyboard("showdate", dates),
        )
        return

    if data.startswith("bookdate|"):
        _, slot_date = data.split("|", 1)

        if count_user_active_bookings(user.id) >= MAX_BOOKINGS_PER_USER:
            await query.edit_message_text(
                f"У тебя уже {MAX_BOOKINGS_PER_USER} активных записей. Сначала отмени одну."
            )
            return

        times = get_free_times(slot_date)
        if not times:
            occupied_rows = db_execute("""
                SELECT slot_time
                FROM slots
                WHERE slot_date = ?
                  AND is_booked = 1
                ORDER BY slot_time
            """, (slot_date,), fetch=True)

            keyboard = [
                [InlineKeyboardButton(f"🔔 Ждать {busy_time}", callback_data=f"waitlist|{slot_date}|{busy_time}")]
                for (busy_time,) in occupied_rows
            ]
            keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_book_dates")])

            await query.edit_message_text(
                "На эту дату всё занято. Можно встать в лист ожидания:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        await query.edit_message_text(
            f"Выбери время на {format_date_ru(slot_date)}:",
            reply_markup=build_times_keyboard("bookslot", slot_date, times, "back_book_dates"),
        )
        return

    if data == "back_book_dates":
        dates = get_free_dates()
        if not dates:
            await query.edit_message_text("Свободных слотов пока нет.")
            return

        await query.edit_message_text(
            "📅 Выбери дату для записи:",
            reply_markup=build_dates_keyboard("bookdate", dates),
        )
        return

    if data.startswith("bookslot|"):
        _, slot_date, slot_time = data.split("|", 2)

        if count_user_active_bookings(user.id) >= MAX_BOOKINGS_PER_USER:
            await query.edit_message_text(f"У тебя уже {MAX_BOOKINGS_PER_USER} активных записей.")
            return

        profile = get_user_profile(user.id)
        if not profile:
            await query.edit_message_text("Сначала заполни профиль через /start")
            return

        _, full_name, phone = profile
        ok = book_slot(slot_date, slot_time, user.id, full_name, phone)

        if ok:
            active_count = count_user_active_bookings(user.id)
            remove_waitlist_user(slot_date, slot_time, user.id)
            await query.edit_message_text(
                f"✅ Ты записан на {format_date_ru(slot_date)} в {slot_time}\n"
                f"Имя: {full_name}\n"
                f"Телефон: {phone}\n"
                f"Активных записей: {active_count}/{MAX_BOOKINGS_PER_USER}"
            )
        else:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔔 Сообщить если освободится", callback_data=f"waitlist|{slot_date}|{slot_time}")]
            ])
            await query.edit_message_text(
                "Слот уже занят. Могу поставить тебя в лист ожидания.",
                reply_markup=keyboard,
            )
        return

    if data.startswith("waitlist|"):
        _, slot_date, slot_time = data.split("|", 2)

        if not has_profile(user.id):
            await query.edit_message_text("Сначала заполни профиль через /start")
            return

        if is_user_on_waitlist(slot_date, slot_time, user.id):
            await query.edit_message_text(
                f"Ты уже в листе ожидания на {format_date_ru(slot_date)} {slot_time}"
            )
            return

        try:
            add_to_waitlist(slot_date, slot_time, user.id)
            await query.edit_message_text(
                f"🔔 Готово. Я сообщу, если освободится слот:\n"
                f"{format_date_ru(slot_date)} {slot_time}"
            )
        except sqlite3.IntegrityError:
            await query.edit_message_text(
                f"Ты уже в листе ожидания на {format_date_ru(slot_date)} {slot_time}"
            )
        return

    if data.startswith("cancel|"):
        _, slot_date, slot_time = data.split("|", 2)
        ok = cancel_booking(slot_date, slot_time, user.id)

        if ok:
            active_count = count_user_active_bookings(user.id)
            await query.edit_message_text(
                f"❌ Запись отменена: {format_date_ru(slot_date)} {slot_time}\n"
                f"Активных записей: {active_count}/{MAX_BOOKINGS_PER_USER}"
            )
            await notify_waitlist(context, slot_date, slot_time)
        else:
            await query.edit_message_text("Не удалось отменить запись.")
        return

    if data.startswith("confirm_yes|"):
        _, slot_date, slot_time = data.split("|", 2)
        set_confirmation_status(slot_date, slot_time, "confirmed")
        await query.edit_message_text(
            f"✅ Отлично, занятие подтверждено:\n{format_date_ru(slot_date)} {slot_time}"
        )
        return

    if data.startswith("confirm_no|"):
        _, slot_date, slot_time = data.split("|", 2)
        slot = get_slot(slot_date, slot_time)
        if not slot:
            await query.edit_message_text("Слот не найден.")
            return

        slot_id, _, _, is_booked, booked_user_id, booked_name, booked_phone = slot
        if booked_user_id != user.id:
            await query.edit_message_text("Эта запись не принадлежит тебе.")
            return

        set_confirmation_status(slot_date, slot_time, "declined")
        admin_release_slot(slot_date, slot_time)
        await query.edit_message_text(
            f"❌ Запись отменена:\n{format_date_ru(slot_date)} {slot_time}"
        )
        await notify_waitlist(context, slot_date, slot_time)
        return

    if data.startswith("adminbookdate|"):
        _, phone, slot_date = data.split("|", 2)
        times = get_free_times(slot_date)
        if not times:
            await query.edit_message_text("На эту дату свободных слотов уже нет.")
            return

        keyboard = [
            [InlineKeyboardButton(t, callback_data=f"adminbookslot|{phone}|{slot_date}|{t}")]
            for t in times
        ]
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"adminbackdates|{phone}")])

        await query.edit_message_text(
            f"Выбери время на {format_date_ru(slot_date)}:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("adminbackdates|"):
        _, phone = data.split("|", 1)
        dates = get_free_dates()
        if not dates:
            await query.edit_message_text("Свободных слотов пока нет.")
            return

        keyboard = [
            [InlineKeyboardButton(format_date_ru(d), callback_data=f"adminbookdate|{phone}|{d}")]
            for d in dates
        ]
        await query.edit_message_text(
            "Выбери дату для ручной записи:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("adminbookslot|"):
        _, phone, slot_date, slot_time = data.split("|", 3)
        student = get_manual_student_by_phone(phone)
        if not student:
            await query.edit_message_text("Ученик не найден.")
            return

        _, full_name, phone, comment, source_status = student
        ok = admin_book_manual_slot(slot_date, slot_time, full_name, phone)

        if ok:
            extra = f"\nКомментарий: {comment}" if comment else ""
            await query.edit_message_text(
                f"✅ Ученик записан вручную\n"
                f"{full_name}\n{phone}\n"
                f"Статус: {source_status}\n"
                f"📅 {format_date_ru(slot_date)} {slot_time}"
                f"{extra}"
            )
        else:
            await query.edit_message_text("Не удалось записать. Возможно, слот уже занят.")
        return


# -----------------------------
# ADMIN COMMANDS
# -----------------------------
async def quickslots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return

    context.user_data["awaiting_quickslots"] = True
    context.user_data["awaiting_genslots"] = False

    await update.message.reply_text(
        "Отправь список слотов, каждый с новой строки:\n\n"
        "2026-03-15 10:00\n"
        "2026-03-15 12:00\n"
        "2026-03-16 16:00"
    )


async def genslots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return

    context.user_data["awaiting_genslots"] = True
    context.user_data["awaiting_quickslots"] = False

    await update.message.reply_text(
        "Отправь шаблон в 2 строки:\n\n"
        "1) Количество дней вперёд\n"
        "2) Время через запятую\n\n"
        "Пример:\n"
        "14\n"
        "10:00, 12:00, 14:00, 16:00, 18:00\n\n"
        "⚠️ Слоты будут созданы только по будням."
    )


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return

    rows = get_bookings_by_date(today_str())

    if not rows:
        await update.message.reply_text("На сегодня записей нет.")
        return

    text = f"Записи на сегодня ({format_date_ru(today_str())}):\n\n"
    for _, t, name, phone, source, _, confirm_status in rows:
        src = "офлайн" if source == "manual" else "бот"
        phone_text = f" | {phone}" if phone else ""
        confirm_text = f" | {confirm_status}" if confirm_status else ""
        text += f"• {t} — {name}{phone_text} | {src}{confirm_text}\n"

    await update.message.reply_text(text)


async def tomorrow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return

    rows = get_bookings_by_date(tomorrow_str())

    if not rows:
        await update.message.reply_text("На завтра записей нет.")
        return

    text = f"Записи на завтра ({format_date_ru(tomorrow_str())}):\n\n"
    for _, t, name, phone, source, _, confirm_status in rows:
        src = "офлайн" if source == "manual" else "бот"
        phone_text = f" | {phone}" if phone else ""
        confirm_text = f" | {confirm_status}" if confirm_status else ""
        text += f"• {t} — {name}{phone_text} | {src}{confirm_text}\n"

    await update.message.reply_text(text)


async def week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return

    start_date = now_msk().date()
    end_date = start_date + timedelta(days=6)
    rows = get_bookings_between(start_date.isoformat(), end_date.isoformat())

    if not rows:
        await update.message.reply_text("На ближайшую неделю записей нет.")
        return

    text = "Записи на 7 дней:\n\n"
    current_date = None

    for d, t, name, phone, source, confirm_status in rows:
        if d != current_date:
            current_date = d
            text += f"\n{format_date_ru(d)}:\n"
        src = "офлайн" if source == "manual" else "бот"
        phone_text = f" | {phone}" if phone else ""
        confirm_text = f" | {confirm_status}" if confirm_status else ""
        text += f"• {t} — {name}{phone_text} | {src}{confirm_text}\n"

    await update.message.reply_text(text)


async def allslots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return

    rows = get_all_slots()
    if not rows:
        await update.message.reply_text("Слотов пока нет.")
        return

    text = "Все слоты:\n\n"
    for d, t, booked, name, phone, source, confirm_status in rows:
        if booked:
            src = "офлайн" if source == "manual" else "бот"
            status = f"занят ({name}"
            if phone:
                status += f", {phone}"
            if confirm_status:
                status += f", {confirm_status}"
            status += f", {src})"
        else:
            status = "свободен"
        text += f"• {format_date_ru(d)} {t} — {status}\n"

    await update.message.reply_text(text[:4000])


# -----------------------------
# ADD STUDENT
# -----------------------------
async def addstudent_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return ConversationHandler.END

    await update.message.reply_text("Введи имя и фамилию ученика.")
    return ADD_STUDENT_NAME


async def addstudent_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    full_name = update.message.text.strip()
    if len(full_name.split()) < 2:
        await update.message.reply_text("Напиши имя и фамилию. Например: Иван Петров")
        return ADD_STUDENT_NAME

    context.user_data["manual_student_name"] = full_name
    await update.message.reply_text("Теперь введи телефон ученика.")
    return ADD_STUDENT_PHONE


async def addstudent_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = normalize_phone(update.message.text)
    if not valid_phone(phone):
        await update.message.reply_text("Телефон выглядит неверно. Попробуй ещё раз.")
        return ADD_STUDENT_PHONE

    context.user_data["manual_student_phone"] = phone
    await update.message.reply_text(
        "Теперь введи комментарий.\n"
        "Например: боится парковки, удобнее вечером.\n"
        "Если комментарий не нужен — напиши: -"
    )
    return ADD_STUDENT_COMMENT


async def addstudent_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comment = update.message.text.strip()
    if comment == "-":
        comment = ""

    full_name = context.user_data["manual_student_name"]
    phone = context.user_data["manual_student_phone"]

    save_manual_student(full_name, phone, comment)
    context.user_data.clear()

    extra = f"\nКомментарий: {comment}" if comment else ""
    await update.message.reply_text(
        f"✅ Ученик добавлен:\n{full_name}\n{phone}{extra}",
        reply_markup=main_menu_keyboard(),
    )
    return ConversationHandler.END


async def students_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return

    students = get_manual_students()
    if not students:
        await update.message.reply_text("Пока нет офлайн-учеников в базе.")
        return

    text = "Ученики:\n\n"
    for _, full_name, phone, comment, source_status in students[:100]:
        status = "в боте" if source_status == "bot" else "офлайн"
        text += f"• {full_name} | {phone} | {status}\n"
        if comment:
            text += f"  └ {comment}\n"

    await update.message.reply_text(text[:4000])


# -----------------------------
# FIND STUDENT
# -----------------------------
async def findstudent_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Введи имя, часть имени или телефон для поиска.\n"
        "Например:\nИван\nили\n999"
    )
    return FIND_STUDENT_QUERY


async def findstudent_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    rows = find_manual_students(query)

    if not rows:
        await update.message.reply_text("Ничего не найдено.")
        return ConversationHandler.END

    text = "Результаты поиска:\n\n"
    for _, full_name, phone, comment, source_status in rows[:30]:
        status = "в боте" if source_status == "bot" else "офлайн"
        text += f"• {full_name} | {phone} | {status}\n"
        if comment:
            text += f"  └ {comment}\n"

    await update.message.reply_text(text[:4000], reply_markup=main_menu_keyboard())
    return ConversationHandler.END


# -----------------------------
# MARK STUDENT AS BOT
# -----------------------------
async def markbot_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Введи телефон ученика, которого нужно отметить как перешедшего в бота."
    )
    return MARKBOT_QUERY


async def markbot_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = normalize_phone(update.message.text)
    updated = mark_manual_student_as_bot(phone)

    if updated:
        await update.message.reply_text("✅ Ученик отмечен как перешедший в бота.", reply_markup=main_menu_keyboard())
    else:
        await update.message.reply_text("Ученик с таким телефоном не найден.", reply_markup=main_menu_keyboard())

    return ConversationHandler.END


# -----------------------------
# ADD BOOKING MANUALLY
# -----------------------------
async def addbooking_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Введи телефон ученика, которого хочешь записать.\n"
        "Он должен уже быть добавлен через /addstudent"
    )
    return ADD_BOOKING_QUERY


async def addbooking_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = normalize_phone(update.message.text)
    student = get_manual_student_by_phone(phone)

    if not student:
        await update.message.reply_text(
            "Ученик с таким телефоном не найден. Сначала добавь его через /addstudent"
        )
        return ADD_BOOKING_QUERY

    dates = get_free_dates()
    if not dates:
        await update.message.reply_text("Свободных слотов пока нет.")
        return ConversationHandler.END

    keyboard = [
        [InlineKeyboardButton(format_date_ru(d), callback_data=f"adminbookdate|{phone}|{d}")]
        for d in dates
    ]

    await update.message.reply_text(
        "Выбери дату для ручной записи:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return ConversationHandler.END


# -----------------------------
# ADMIN TEXT FLOWS
# -----------------------------
async def quickslots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return

    context.user_data["awaiting_quickslots"] = True
    context.user_data["awaiting_genslots"] = False

    await update.message.reply_text(
        "Отправь список слотов, каждый с новой строки:\n\n"
        "2026-03-15 10:00\n"
        "2026-03-15 12:00\n"
        "2026-03-16 16:00"
    )


async def genslots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Команда только для администратора.")
        return

    context.user_data["awaiting_genslots"] = True
    context.user_data["awaiting_quickslots"] = False

    await update.message.reply_text(
        "Отправь шаблон в 2 строки:\n\n"
        "1) Количество дней вперёд\n"
        "2) Время через запятую\n\n"
        "Пример:\n"
        "14\n"
        "10:00, 12:00, 14:00, 16:00, 18:00\n\n"
        "⚠️ Слоты будут созданы только по будням."
    )


async def quickslots_text_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lines = [line.strip() for line in update.message.text.splitlines() if line.strip()]
    added = 0
    errors = []

    for line in lines:
        try:
            dt = datetime.strptime(line, "%Y-%m-%d %H:%M")
            add_slot(dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M"))
            added += 1
        except sqlite3.IntegrityError:
            errors.append(f"{line} — уже существует")
        except ValueError:
            errors.append(f"{line} — неверный формат")

    context.user_data["awaiting_quickslots"] = False

    answer = f"✅ Добавлено слотов: {added}"
    if errors:
        answer += "\n\nОшибки:\n" + "\n".join(errors[:20])

    await update.message.reply_text(answer, reply_markup=main_menu_keyboard())


async def genslots_text_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lines = [line.strip() for line in update.message.text.splitlines() if line.strip()]
    context.user_data["awaiting_genslots"] = False

    if len(lines) < 2:
        await update.message.reply_text("Нужно 2 строки: количество дней и список времени.")
        return

    try:
        days_count = int(lines[0])
        times = [t.strip() for t in lines[1].split(",") if t.strip()]
        for t in times:
            datetime.strptime(t, "%H:%M")
    except ValueError:
        await update.message.reply_text("Проверь формат. Пример:\n14\n10:00, 12:00, 14:00")
        return

    added = 0
    skipped = 0
    today_date = now_msk().date()

    for day_offset in range(days_count):
        d = today_date + timedelta(days=day_offset)
        if d.weekday() > 4:
            continue

        date_str = d.strftime("%Y-%m-%d")
        for t in times:
            try:
                add_slot(date_str, t)
                added += 1
            except sqlite3.IntegrityError:
                skipped += 1

    await update.message.reply_text(
        f"✅ Сгенерировано слотов: {added}\n"
        f"↪️ Пропущено существующих: {skipped}\n"
        f"📅 Только будни",
        reply_markup=main_menu_keyboard(),
    )


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Не понял команду. Нажми /start", reply_markup=main_menu_keyboard())
        return

    if context.user_data.get("awaiting_quickslots"):
        await quickslots_text_flow(update, context)
        return

    if context.user_data.get("awaiting_genslots"):
        await genslots_text_flow(update, context)
        return

    await update.message.reply_text("Не понял сообщение. Нажми /start", reply_markup=main_menu_keyboard())


# -----------------------------
# MENU BUTTONS
# -----------------------------
async def menu_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if context.user_data.get("awaiting_quickslots") or context.user_data.get("awaiting_genslots"):
        await text_handler(update, context)
        return

    if text == "📅 Записаться":
        await book(update, context)
        return

    if text == "🗓 Свободные слоты":
        await slots(update, context)
        return

    if text == "🔥 Свободно сегодня":
        await todayfree(update, context)
        return

    if text == "📖 Мои записи":
        await mybookings(update, context)
        return

    if text == "❌ Отменить запись":
        await cancel_booking_menu(update, context)
        return

    if text == "👤 Мой профиль":
        await profile_cmd(update, context)
        return

    if text == "☎ Связаться с инструктором":
        await contact_instructor(update, context)
        return

    if text == "🏠 Меню":
        await show_menu(update, context)
        return

    await text_handler(update, context)


# -----------------------------
# MAIN
# -----------------------------
def main():
    init_db()
    cleanup_past_slots()

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Не задан BOT_TOKEN")

    app = ApplicationBuilder().token(token).build()

    # Регистрация
    app.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("start", start)],
            states={
                PROFILE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_name)],
                PROFILE_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, profile_phone)],
            },
            fallbacks=[CommandHandler("cancel", cancel_any)],
            allow_reentry=True,
        )
    )

    # Редактирование профиля
    app.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("editprofile", editprofile_start)],
            states={
                EDIT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_name)],
                EDIT_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_phone)],
            },
            fallbacks=[CommandHandler("cancel", cancel_any)],
            allow_reentry=True,
        )
    )

    # Добавление ученика
    app.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("addstudent", addstudent_start)],
            states={
                ADD_STUDENT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, addstudent_name)],
                ADD_STUDENT_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, addstudent_phone)],
                ADD_STUDENT_COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, addstudent_comment)],
            },
            fallbacks=[CommandHandler("cancel", cancel_any)],
            allow_reentry=True,
        )
    )

    # Ручная запись ученика
    app.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("addbooking", addbooking_start)],
            states={
                ADD_BOOKING_QUERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, addbooking_phone)],
            },
            fallbacks=[CommandHandler("cancel", cancel_any)],
            allow_reentry=True,
        )
    )

    # Поиск ученика
    app.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("findstudent", findstudent_start)],
            states={
                FIND_STUDENT_QUERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, findstudent_query)],
            },
            fallbacks=[CommandHandler("cancel", cancel_any)],
            allow_reentry=True,
        )
    )

    # Пометить как bot
    app.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("markbot", markbot_start)],
            states={
                MARKBOT_QUERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, markbot_query)],
            },
            fallbacks=[CommandHandler("cancel", cancel_any)],
            allow_reentry=True,
        )
    )

    # Команды
    app.add_handler(CommandHandler("profile", profile_cmd))
    app.add_handler(CommandHandler("slots", slots))
    app.add_handler(CommandHandler("todayfree", todayfree))
    app.add_handler(CommandHandler("book", book))
    app.add_handler(CommandHandler("mybookings", mybookings))
    app.add_handler(CommandHandler("cancel_booking", cancel_booking_menu))
    app.add_handler(CommandHandler("quickslots", quickslots))
    app.add_handler(CommandHandler("genslots", genslots))
    app.add_handler(CommandHandler("today", today))
    app.add_handler(CommandHandler("tomorrow", tomorrow))
    app.add_handler(CommandHandler("week", week))
    app.add_handler(CommandHandler("allslots", allslots))
    app.add_handler(CommandHandler("students", students_cmd))

    # Callback
    app.add_handler(CallbackQueryHandler(callback_router))

    # Кнопки меню
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_buttons))

    # Джобы
    if app.job_queue:
        app.job_queue.run_repeating(reminder_24_job, interval=3600, first=10, name="reminder_24")
        app.job_queue.run_repeating(reminder_2_job, interval=1800, first=20, name="reminder_2")
        app.job_queue.run_repeating(confirmation_job, interval=3600, first=30, name="confirmation_job")
        app.job_queue.run_daily(
            morning_report_job,
            time=time(hour=MORNING_REPORT_HOUR, minute=MORNING_REPORT_MINUTE, tzinfo=UTC_PLUS_3),
            name="morning_report_job",
        )
        app.job_queue.run_repeating(cleanup_job, interval=21600, first=40, name="cleanup_old")

    print("Bot started...")
    app.run_polling()


if __name__ == "__main__":
    main()
