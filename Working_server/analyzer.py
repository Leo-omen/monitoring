import os
import json
import requests
import sqlite3
from datetime import datetime, timezone

# ===============================================================
# НАСТРОЙКИ
# ===============================================================
SERVER_URL = "http://194.87.76.183:5000"
API_KEY = "qwertyuiop"
HEADERS = {'Content-Type': 'application/json', 'Authorization': f'Bearer {API_KEY}'}
LOCAL_DB_FILE = 'client_database.db'
DEAD_AFTER_CAMPAIGN_FOLDER = 'accounts/Мертвые после рассылки'  # Для снимков кампаний
DEAD_PERMANENT_FOLDER = 'accounts/Мертвые'  # Для режима 4

# --- Глобальная переменная для запоминания последней рассылки ---
last_campaign_name = None

# ===============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ПУТЕЙ
# ===============================================================
def is_path_inside_folder(path, folder):
    """Возвращает True, если path находится внутри folder (учитывает разные разделители путей)."""
    try:
        path_abs = os.path.abspath(path)
        folder_abs = os.path.abspath(folder)
        return os.path.commonpath([path_abs, folder_abs]) == folder_abs
    except ValueError:
        return False

# ===============================================================
# ЛОКАЛЬНАЯ БАЗА ДАННЫХ
# ===============================================================
def init_local_db():
    """Создает локальную БД и таблицу для хранения состава кампаний."""
    conn = sqlite3.connect(LOCAL_DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS local_campaigns (
        campaign_name TEXT PRIMARY KEY,
        scan_date TEXT NOT NULL,
        associated_accounts TEXT NOT NULL
    )
    ''')
    conn.commit()
    conn.close()

def save_campaign_locally(campaign_name, accounts_list):
    """Сохраняет или обновляет информацию о кампании в локальной БД."""
    conn = sqlite3.connect(LOCAL_DB_FILE)
    cursor = conn.cursor()
    phone_numbers = json.dumps([acc['phone'] for acc in accounts_list])
    scan_date = datetime.now().strftime("%Y-%m-%d")
    
    cursor.execute(
        "INSERT OR REPLACE INTO local_campaigns (campaign_name, scan_date, associated_accounts) VALUES (?, ?, ?)",
        (campaign_name, scan_date, phone_numbers)
    )
    conn.commit()
    conn.close()
    print(f"ℹ️ Информация о составе кампании '{campaign_name}' ({len(accounts_list)} акк.) сохранена/обновлена локально.")

def get_last_campaign_for_account(phone):
    """Находит последнюю кампанию для аккаунта по max scan_date."""
    conn = sqlite3.connect(LOCAL_DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT campaign_name, scan_date FROM local_campaigns")
    rows = cursor.fetchall()
    conn.close()
    
    last_campaign = None
    max_date = None
    for row in rows:
        campaign_name, scan_date = row
        accounts = json.loads(row[2])
        if phone in accounts:
            date_obj = datetime.strptime(scan_date, "%Y-%m-%d")
            if max_date is None or date_obj > max_date:
                max_date = date_obj
                last_campaign = campaign_name
    return last_campaign

# ===============================================================
# ОСНОВНЫЕ ФУНКЦИИ
# ===============================================================
def get_account_status(data):
    """Определяет статус аккаунта на основе JSON."""
    if data.get('spamblock') == 'permanent': return "Permanent Spamblock"
    if data.get('spamblock') == 'temporary': return "Temporary Spamblock"
    if data.get('freeze_until'):
        try:
            freeze_until_dt = datetime.fromisoformat(data['freeze_until'])
            if freeze_until_dt > datetime.now(timezone.utc):
                return "Frozen"
        except (ValueError, TypeError):
            return "Frozen"
    return "Working"

def read_account_file(filepath, is_dead=False):
    """Читает JSON-файл аккаунта."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        registration_date = "N/A"
        reg_epoch = data.get("register_time")
        if isinstance(reg_epoch, (int, float)) and reg_epoch > 0:
            registration_date = datetime.fromtimestamp(int(reg_epoch), timezone.utc).date().isoformat()  # Исправлено
        else:
            reg_date_raw = data.get("session_created_date")
            if isinstance(reg_date_raw, str) and len(reg_date_raw) >= 10:
                registration_date = reg_date_raw[:10]

        filename = os.path.basename(filepath)
        phone = filename.split('.')[0]
        
        status = "Banned" if is_dead else get_account_status(data)
        
        return {
            "phone": phone,
            "registration_date": registration_date,
            "status": status,
            "messages_sent": data.get("stats_spam_count", 0),
            "invites_sent": data.get("stats_invites_count", 0)
        }
    except (json.JSONDecodeError, FileNotFoundError):
        print(f"⚠️ Предупреждение: Не удалось прочитать файл {filepath}. Пропускаем.")
        return None

def scan_folder(folder_path, is_dead=False):
    """Сканирует папку и возвращает список данных аккаунтов."""
    if not os.path.isdir(folder_path):
        print(f"❌ Ошибка: Папка '{folder_path}' не найдена.")
        return []
    
    accounts_list = []
    for f in os.listdir(folder_path):
        if f.endswith('.json'):
            acc_data = read_account_file(os.path.join(folder_path, f), is_dead=is_dead)
            if acc_data:
                accounts_list.append(acc_data)
    return accounts_list

def find_and_scan_accounts(campaign_name, snapshot_type):
    """Ищет аккаунты для кампании, включая 'Мертвые после рассылки'."""
    print(f"Ищем аккаунты для кампании '{campaign_name}' по списку из локальной БД...")
    conn = sqlite3.connect(LOCAL_DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT associated_accounts FROM local_campaigns WHERE campaign_name = ?", (campaign_name,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        print(f"❌ Ошибка: В локальной БД нет информации о кампании '{campaign_name}'.")
        return None

    phone_numbers = set(json.loads(row[0]))
    print(f"   Нужно найти {len(phone_numbers)} аккаунтов из 'ДО'.")

    search_paths = [
        f'clients/{campaign_name}',
        'accounts',
        DEAD_AFTER_CAMPAIGN_FOLDER
    ]
    
    accounts_details = []
    new_dead_accounts = []
    seen = set()
    
    for path in search_paths:
        if not os.path.isdir(path):
            continue
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.json'):
                    filepath = os.path.join(root, file)
                    is_dead = is_path_inside_folder(filepath, DEAD_AFTER_CAMPAIGN_FOLDER)
                    acc_data = read_account_file(filepath, is_dead=is_dead)
                    if acc_data and acc_data['phone'] not in seen:
                        seen.add(acc_data['phone'])
                        if acc_data['phone'] in phone_numbers or is_dead:
                            accounts_details.append(acc_data)
                            if is_dead and acc_data['phone'] not in phone_numbers:
                                new_dead_accounts.append(acc_data)

    if new_dead_accounts:
        print(f"ℹ️ Найдено {len(new_dead_accounts)} новых мертвых аккаунтов. Добавляем в БД для '{campaign_name}'.")
        all_accounts = [acc for acc in accounts_details if acc['phone'] in phone_numbers] + new_dead_accounts
        save_campaign_locally(campaign_name, all_accounts)

    return accounts_details

def link_accounts_to_campaign():
    # ... (без изменений)
    pass  # Опущено для краткости

def scan_after_immediate():
    """2. Снимок 'Сразу ПОСЛЕ'"""
    print("\n--- Создание снимка 'Сразу ПОСЛЕ' ---")
    campaign_name = input("Введите имя рассылки для сканирования (или Enter для последней): ") or last_campaign_name
    if not campaign_name: return

    accounts_list = find_and_scan_accounts(campaign_name, "after_immediate")
    if not accounts_list:
        print("Не найдено аккаунтов для отправки.")
        return

    seen = set()
    dedup = []
    for a in accounts_list:
        if a and a.get('phone') and a['phone'] not in seen:
            seen.add(a['phone'])
            dedup.append(a)
    accounts_list = dedup

    print(f"Найдено {len(accounts_list)} аккаунтов. Отправка на сервер...")

    payload = {"campaign_name": campaign_name, "snapshot_type": "after_immediate", "accountsList": accounts_list}

    try:
        response = requests.post(f"{SERVER_URL}/api/snapshot", headers=HEADERS, json=payload, timeout=30)
        if response.status_code == 200:
            print(f"✅ Успех! Снимок 'ПОСЛЕ' для '{campaign_name}' отправлен.")
            last_campaign_name = campaign_name
            save_campaign_locally(campaign_name, accounts_list)
        else:
            try:
                err = response.json().get('error', 'Неизвестная ошибка')
            except Exception:
                err = response.text
            print(f"❌ Ошибка сервера ({response.status_code}): {err}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка сети: {e}")

def scan_after_next_day():
    """3. Снимок 'На следующий день ПОСЛЕ'"""
    print("\n--- Создание снимка 'На следующий день ПОСЛЕ' ---")
    campaign_name = input("Введите имя вчерашней рассылки для сканирования: ")
    if not campaign_name: return

    accounts_list = find_and_scan_accounts(campaign_name, "after_day_2")
    if not accounts_list:
        print("Не найдено аккаунтов для отправки.")
        return

    print(f"Найдено {len(accounts_list)} аккаунтов. Отправка на сервер...")
    payload = {"campaign_name": campaign_name, "snapshot_type": "after_day_2", "accountsList": accounts_list}

    try:
        response = requests.post(f"{SERVER_URL}/api/snapshot", headers=HEADERS, json=payload, timeout=30)
        if response.status_code == 200:
            print(f"✅ Успех! Снимок 'На следующий день' для рассылки '{campaign_name}' успешно отправлен.")
        else:
            print(f"❌ Ошибка сервера ({response.status_code}): {response.json().get('error', 'Неизвестная ошибка')}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка сети: {e}")

def update_all_accounts():
    """Сканирует все аккаунты, включая 'Мертвые', и фиксирует баны в последних кампаниях."""
    print("\n--- Запуск полного сканирования всех аккаунтов ---")
    
    search_paths = ['accounts', 'clients']
    unique_files = set()

    for path in search_paths:
        if not os.path.isdir(path):
            continue
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.json'):
                    unique_files.add(os.path.join(root, file))

    if not unique_files:
        print("Не найдено ни одного .json файла для сканирования.")
        return

    print(f"Найдено {len(unique_files)} уникальных аккаунтов. Чтение данных...")
    
    all_accounts_data = []
    dead_accounts = []
    for filepath in unique_files:
        is_dead = is_path_inside_folder(filepath, DEAD_PERMANENT_FOLDER)
        account_data = read_account_file(filepath, is_dead=is_dead)
        if account_data:
            all_accounts_data.append(account_data)
            if is_dead:
                dead_accounts.append(account_data)

    if not all_accounts_data:
        print("Не удалось прочитать данные ни одного аккаунта.")
        return

    print("Отправка данных на сервер для массового обновления...")
    try:
        response = requests.post(f"{SERVER_URL}/api/accounts/update_all", headers=HEADERS, json=all_accounts_data, timeout=60)
        if response.status_code == 200:
            print(f"✅ Успех! {response.json().get('updated_count', 0)} аккаунтов были обновлены на сервере.")
        else:
            print(f"❌ Ошибка сервера ({response.status_code}): {response.json().get('error', 'Неизвестная ошибка')}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка сети: {e}")

    if dead_accounts:
        print(f"ℹ️ Обработка {len(dead_accounts)} мертвых аккаунтов из '{DEAD_PERMANENT_FOLDER}'.")
        for acc in dead_accounts:
            phone = acc['phone']
            last_campaign = get_last_campaign_for_account(phone)
            if last_campaign:
                print(f"   Аккаунт {phone}: Последняя кампания '{last_campaign}'. Отправка status_update.")
                payload = {"campaign_name": last_campaign, "snapshot_type": "status_update", "accountsList": [acc]}
                try:
                    response = requests.post(f"{SERVER_URL}/api/snapshot", headers=HEADERS, json=payload, timeout=30)
                    if response.status_code == 200:
                        print(f"     ✅ Фиксация бана для {phone} в '{last_campaign}' успешна.")
                    else:
                        print(f"     ❌ Ошибка: {response.json().get('error')}")
                except requests.exceptions.RequestException as e:
                    print(f"     ❌ Ошибка сети: {e}")
            else:
                print(f"   ⚠️ Аккаунт {phone}: Не найдена последняя кампания. Пропускаем фиксацию.")

def main_menu():
    while True:
        print("\n===== Меню анализатора (Клиент) =====")
        print("--- Работа с кампаниями ---")
        print("1. Связать аккаунты с рассылкой (Снимок 'ДО')")
        print("2. Сканирование сразу после выполнения")
        print("3. Сканирование на следующий день после рассылки")
        print("--- Обслуживание ---")
        print("4. Обновить информацию по ВСЕМ аккаунтам")
        print("---")
        print("0. Выход")
        
        choice = input("Выберите действие: ")
        
        if choice == '1': link_accounts_to_campaign()
        elif choice == '2': scan_after_immediate()
        elif choice == '3': scan_after_next_day()
        elif choice == '4': update_all_accounts()
        elif choice == '0':
            print("Выход из программы."); break
        else:
            print("Неверный выбор.")
        
        input("\nНажмите Enter для продолжения...")

if __name__ == '__main__':
    if not os.path.isdir('accounts'):
        os.mkdir('accounts')
        print("Создана папка 'accounts' для хранения всех аккаунтов.")
    if not os.path.isdir('clients'):
        os.mkdir('clients')
        print("Создана папка 'clients' для сортировки аккаунтов по кампаниям.")
    if not os.path.isdir(DEAD_AFTER_CAMPAIGN_FOLDER):
        os.makedirs(DEAD_AFTER_CAMPAIGN_FOLDER)
        print(f"Создана папка '{DEAD_AFTER_CAMPAIGN_FOLDER}'.")
    if not os.path.isdir(DEAD_PERMANENT_FOLDER):
        os.makedirs(DEAD_PERMANENT_FOLDER)
        print(f"Создана папка '{DEAD_PERMANENT_FOLDER}'.")
    init_local_db()
    main_menu()
