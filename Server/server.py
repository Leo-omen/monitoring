from flask import Flask, request, jsonify, render_template, redirect, url_for
import sqlite3
import os
from datetime import datetime, timedelta

# --- Настройки ---
DATABASE_FILE = 'database.db'
API_KEY = "qwertyuiop"

app = Flask(__name__)
app.config['SECRET_KEY'] = 'a_very_secret_key_for_sessions_and_forms'

# ===============================================================
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (БД и РАСЧЕТЫ)
# ===============================================================

def get_db_connection():
    """Устанавливает соединение с БД."""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Инициализирует базу данных и создает таблицы, если они не существуют."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS accounts (
        phone TEXT PRIMARY KEY, registration_date TEXT, first_campaign_date TEXT,
        current_status TEXT NOT NULL, total_messages INTEGER DEFAULT 0, total_invites INTEGER DEFAULT 0,
        total_revenue REAL DEFAULT 0.0, last_updated TEXT NOT NULL ) ''')
    
    # --- ОБНОВЛЕННАЯ ТАБЛИЦА CAMPAIGNS (ПУНКТ 4) ---
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        name TEXT NOT NULL UNIQUE, 
        campaign_date TEXT NOT NULL,
        cost_per_message REAL NOT NULL, 
        cost_per_invite REAL NOT NULL,
        message_type TEXT,
        base_type TEXT,
        link_type TEXT,
        offer TEXT
    ) ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS campaign_log (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT, campaign_id INTEGER NOT NULL, account_phone TEXT NOT NULL,
        snapshot_type TEXT NOT NULL, messages_count INTEGER NOT NULL, invites_count INTEGER NOT NULL,
        status TEXT NOT NULL, timestamp TEXT NOT NULL,
        FOREIGN KEY (campaign_id) REFERENCES campaigns (id),
        FOREIGN KEY (account_phone) REFERENCES accounts (phone) ) ''')
    conn.commit()
    conn.close()

def calculate_campaign_stats(campaign_id, conn):
    """Рассчитывает всю статистику с учетом новой логики фильтрации."""
    cursor = conn.cursor()
    cursor.execute('SELECT account_phone, snapshot_type, messages_count, invites_count, status FROM campaign_log WHERE campaign_id = ?', (campaign_id,))
    logs = cursor.fetchall()
    cursor.execute('SELECT cost_per_message, cost_per_invite FROM campaigns WHERE id = ?', (campaign_id,))
    costs = cursor.fetchone()

    stats = {}
    for log in logs:
        phone = log['account_phone']
        if phone not in stats: stats[phone] = {}
        stats[phone][log['snapshot_type']] = {'messages': log['messages_count'], 'invites': log['invites_count'], 'status': log['status']}
    
    results = []

    summary = {
        'total_revenue': 0, 'total_messages': 0, 'total_invites': 0, 'accounts_restricted': 0,
        'frozen_count': 0, 'frozen_messages': 0,
        'temp_spam_count': 0, 'temp_spam_messages': 0,
        'perm_spam_count': 0, 'perm_spam_messages': 0,
        # ДОБАВЛЯЕМ НОВЫЕ ПОЛЯ
        'temp_spam_resolved_count': 0, 
        'temp_spam_resolved_messages': 0
    }

    for phone, snapshots in stats.items():
        if 'before' in snapshots and ('after_immediate' in snapshots or 'after_day_2' in snapshots or 'status_update' in snapshots):
            after_snapshot = snapshots.get('status_update', snapshots.get('after_day_2', snapshots.get('after_immediate')))
            
            msg_delta = after_snapshot['messages'] - snapshots['before']['messages']
            inv_delta = after_snapshot['invites'] - snapshots['before']['invites']
            status_after = after_snapshot['status'] # Это финальный статус

            if msg_delta == 0 and inv_delta == 0 and status_after == 'Working':
                continue
            
            revenue = (msg_delta * costs['cost_per_message']) + (inv_delta * costs['cost_per_invite'])
            
            # --- НОВАЯ ЛОГИКА ---
            report_status = status_after # Статус, который пойдет в итоговый отчет
            was_temp_blocked = False
            
            # Проверяем, был ли он 'Working', но ДО этого имел 'Temporary Spamblock'
            if status_after == 'Working':
                for snapshot_type, snapshot_data in snapshots.items():
                    if snapshot_type != 'before' and snapshot_data['status'] == 'Temporary Spamblock':
                        was_temp_blocked = True
                        break
            
            if status_after == 'Working' and was_temp_blocked:
                # Это наш случай "Снятый спамблок"
                report_status = 'Temporary Spamblock (Resolved)' # Особый статус для отчета
                summary['temp_spam_resolved_count'] += 1
                summary['temp_spam_resolved_messages'] += msg_delta
                # ВАЖНО: 'accounts_restricted' НЕ увеличиваем
            
            elif status_after != 'Working':
                # Это все еще активные ограничения
                summary['accounts_restricted'] += 1
                if status_after == 'Frozen':
                    summary['frozen_count'] += 1
                    summary['frozen_messages'] += msg_delta
                elif status_after == 'Temporary Spamblock':
                    summary['temp_spam_count'] += 1
                    summary['temp_spam_messages'] += msg_delta
                elif status_after == 'Permanent Spamblock' or status_after == 'Banned':  # Объединяем Banned с Permanent
                    summary['perm_spam_count'] += 1
                    summary['perm_spam_messages'] += msg_delta
            # --- КОНЕЦ НОВОЙ ЛОГИКИ ---

            summary['total_revenue'] += revenue
            summary['total_messages'] += msg_delta
            summary['total_invites'] += inv_delta

            # Используем report_status вместо status_after
            results.append({'phone': phone, 'status_before': snapshots['before']['status'], 'status_after': report_status,
                            'msg_sent': msg_delta, 'inv_sent': inv_delta, 'revenue': revenue})
    
    total_accounts = len(results)
    summary['percentage_restricted'] = (summary['accounts_restricted'] / total_accounts * 100) if total_accounts > 0 else 0
    summary['avg_msg_all'] = summary['total_messages'] / total_accounts if total_accounts > 0 else 0
    summary['avg_msg_frozen'] = summary['frozen_messages'] / summary['frozen_count'] if summary['frozen_count'] > 0 else 0
    summary['avg_msg_temp_spam'] = summary['temp_spam_messages'] / summary['temp_spam_count'] if summary['temp_spam_count'] > 0 else 0
    summary['avg_msg_perm_spam'] = summary['perm_spam_messages'] / summary['perm_spam_count'] if summary['perm_spam_count'] > 0 else 0
    summary['avg_msg_temp_spam_resolved'] = summary['temp_spam_resolved_messages'] / summary['temp_spam_resolved_count'] if summary['temp_spam_resolved_count'] > 0 else 0
    summary['avg_revenue_per_account'] = summary['total_revenue'] / total_accounts if total_accounts > 0 else 0

    return summary, results

# ===============================================================
# API МАРШРУТЫ (ДЛЯ КЛИЕНТА)
# ===============================================================
@app.route('/api/campaigns', methods=['POST'])
def create_campaign():
    if request.headers.get('Authorization') != f'Bearer {API_KEY}':
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.json
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''INSERT INTO campaigns (name, campaign_date, cost_per_message, cost_per_invite, message_type, base_type, link_type, offer)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (data['campaign_name'], datetime.now().strftime("%Y-%m-%d"),
             data['cost_per_message'], data['cost_per_invite'],
             data.get('message_type'), data.get('base_type'), data.get('link_type'), data.get('offer'))
        )
        conn.commit()
        return jsonify({'message': 'Campaign created successfully'}), 200
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Campaign with this name already exists'}), 409
    finally:
        conn.close()

@app.route('/api/snapshot', methods=['POST'])
def add_snapshot():
    if request.headers.get('Authorization') != f'Bearer {API_KEY}':
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.json
    campaign_name = data['campaign_name']
    snapshot_type = data['snapshot_type']
    accounts_list = data['accountsList']

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM campaigns WHERE name = ?", (campaign_name,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': f'Campaign {campaign_name} not found'}), 404

    campaign_id = row['id']

    # Получаем costs для кампании
    cursor.execute("SELECT cost_per_message, cost_per_invite FROM campaigns WHERE id = ?", (campaign_id,))
    costs = cursor.fetchone()

    timestamp = datetime.now().isoformat()
    updated_count = 0

    for acc in accounts_list:
        phone = acc['phone']
        status = acc['status']
        messages = acc['messages_sent']
        invites = acc['invites_sent']

        # Обновляем глобальную информацию об аккаунте
        cursor.execute("SELECT * FROM accounts WHERE phone = ?", (phone,))
        acc_row = cursor.fetchone()
        delta_revenue = 0.0
        if acc_row:
            if snapshot_type.startswith('after') or snapshot_type == 'status_update':
                delta_msg = messages - acc_row['total_messages']
                delta_inv = invites - acc_row['total_invites']
                delta_revenue = (delta_msg * costs['cost_per_message']) + (delta_inv * costs['cost_per_invite'])
                new_revenue = acc_row['total_revenue'] + delta_revenue
                cursor.execute('''
                    UPDATE accounts SET current_status = ?, total_messages = ?, total_invites = ?, total_revenue = ?, last_updated = ?
                    WHERE phone = ?
                ''', (status, messages, invites, new_revenue, timestamp, phone))
            else:
                cursor.execute('''
                    UPDATE accounts SET current_status = ?, total_messages = ?, total_invites = ?, last_updated = ?
                    WHERE phone = ?
                ''', (status, messages, invites, timestamp, phone))
        else:
            cursor.execute('''
                INSERT INTO accounts (phone, registration_date, current_status, total_messages, total_invites, total_revenue, last_updated)
                VALUES (?, ?, ?, ?, ?, 0.0, ?)
            ''', (phone, acc.get('registration_date', 'N/A'), status, messages, invites, timestamp))

        # Для first_campaign_date
        if snapshot_type == 'before' and (not acc_row or not acc_row['first_campaign_date']):
            cursor.execute("UPDATE accounts SET first_campaign_date = ? WHERE phone = ?", (datetime.now().strftime("%Y-%m-%d"), phone))

        # Добавляем лог для кампании
        cursor.execute('''
            INSERT INTO campaign_log (campaign_id, account_phone, snapshot_type, messages_count, invites_count, status, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (campaign_id, phone, snapshot_type, messages, invites, status, timestamp))
        updated_count += 1

    conn.commit()
    conn.close()
    return jsonify({'message': f'Snapshot added successfully, {updated_count} accounts processed'}), 200

@app.route('/api/accounts/update_all', methods=['POST'])
def update_all_accounts():
    if request.headers.get('Authorization') != f'Bearer {API_KEY}':
        return jsonify({'error': 'Unauthorized'}), 401

    accounts_list = request.json
    conn = get_db_connection()
    cursor = conn.cursor()
    updated_count = 0

    for acc in accounts_list:
        phone = acc['phone']
        status = acc['status']
        messages = acc['messages_sent']
        invites = acc['invites_sent']
        reg_date = acc.get('registration_date', 'N/A')
        timestamp = datetime.now().isoformat()

        cursor.execute("SELECT * FROM accounts WHERE phone = ?", (phone,))
        row = cursor.fetchone()
        if row:
            cursor.execute('''
                UPDATE accounts SET registration_date = ?, current_status = ?, total_messages = ?, total_invites = ?, last_updated = ?
                WHERE phone = ?
            ''', (reg_date, status, messages, invites, timestamp, phone))
            updated_count += 1
        else:
            cursor.execute('''
                INSERT INTO accounts (phone, registration_date, current_status, total_messages, total_invites, total_revenue, last_updated)
                VALUES (?, ?, ?, ?, ?, 0.0, ?)
            ''', (phone, reg_date, status, messages, invites, timestamp))
            updated_count += 1

    conn.commit()
    conn.close()
    return jsonify({'updated_count': updated_count}), 200

@app.route('/api/campaigns/edit/<int:campaign_id>', methods=['POST'])
def api_edit_campaign(campaign_id):
    if request.headers.get('Authorization') != f'Bearer {API_KEY}':
        return jsonify({'error': 'Unauthorized'}), 401

    data = request.json
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE campaigns SET name = ?, cost_per_message = ?, cost_per_invite = ?, message_type = ?, base_type = ?, link_type = ?, offer = ?
            WHERE id = ?
        ''', (data['name'], data['cost_per_message'], data['cost_per_invite'], data['message_type'], data['base_type'], data['link_type'], data['offer'], campaign_id))
        conn.commit()
        return jsonify({'message': 'Campaign updated successfully'}), 200
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Campaign name already exists'}), 409
    finally:
        conn.close()

# ===============================================================
# WEB ИНТЕРФЕЙС (ДЛЯ АДМИНА)
# ===============================================================
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/campaigns', methods=['GET', 'POST'])
def manage_campaigns():
    conn = get_db_connection()
    cursor = conn.cursor()

    if request.method == 'POST':
        data = request.form
        try:
            cursor.execute(
                '''INSERT INTO campaigns (name, campaign_date, cost_per_message, cost_per_invite, message_type, base_type, link_type, offer)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                (data['name'], datetime.now().strftime("%Y-%m-%d"),
                 float(data['cost_per_message']), float(data['cost_per_invite']),
                 data.get('message_type'), data.get('base_type'), data.get('link_type'), data.get('offer'))
            )
            conn.commit()
        except sqlite3.IntegrityError:
            return "Campaign with this name already exists", 409

    search_query = request.args.get('search')
    if search_query:
        cursor.execute("SELECT * FROM campaigns WHERE name LIKE ? ORDER BY id DESC", (f'%{search_query}%',))
    else:
        cursor.execute("SELECT * FROM campaigns ORDER BY id DESC")
    campaigns = cursor.fetchall()

    cursor.execute("SELECT DISTINCT offer FROM campaigns WHERE offer IS NOT NULL")
    existing_offers = [row['offer'] for row in cursor.fetchall()]

    conn.close()
    return render_template('campaigns.html', campaigns=campaigns, existing_offers=existing_offers, search_query=search_query)

@app.route('/edit_campaign/<int:campaign_id>', methods=['GET', 'POST'])
def edit_campaign(campaign_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,))
    campaign = cursor.fetchone()
    if not campaign:
        conn.close()
        return "Campaign not found", 404

    if request.method == 'POST':
        data = request.form
        try:
            cursor.execute('''
                UPDATE campaigns SET name = ?, cost_per_message = ?, cost_per_invite = ?, message_type = ?, base_type = ?, link_type = ?, offer = ?
                WHERE id = ?
            ''', (data['name'], float(data['cost_per_message']), float(data['cost_per_invite']), data['message_type'], data['base_type'], data['link_type'], data['offer'], campaign_id))
            conn.commit()
            conn.close()
            return redirect(url_for('manage_campaigns'))
        except sqlite3.IntegrityError:
            return "Campaign name already exists", 409

    cursor.execute("SELECT DISTINCT offer FROM campaigns WHERE offer IS NOT NULL")
    existing_offers = [row['offer'] for row in cursor.fetchall()]

    conn.close()
    return render_template('campaign_edit.html', campaign=campaign, existing_offers=existing_offers)

@app.route('/report/campaign', methods=['GET'])
def report_campaign():
    conn = get_db_connection()
    cursor = conn.cursor()

    clients = sorted(set([c['name'].split('_')[0] for c in cursor.execute("SELECT name FROM campaigns").fetchall()]))
    selected_client = request.args.get('client_code')
    selected_campaign_name = request.args.get('campaign_name')

    if selected_client:
        cursor.execute("SELECT name FROM campaigns WHERE name LIKE ?", (f"{selected_client}%",))
        campaigns_for_client = sorted([row['name'] for row in cursor.fetchall()])

        if selected_campaign_name:
            cursor.execute("SELECT id FROM campaigns WHERE name = ?", (selected_campaign_name,))
            campaign_id = cursor.fetchone()['id']
            summary, results = calculate_campaign_stats(campaign_id, conn)
            report_data = {'summary': summary, 'results': results}
        else:
            report_data = None
    else:
        campaigns_for_client = None
        report_data = None

    conn.close()
    return render_template('report_campaign.html', clients=clients, selected_client=selected_client,
                           campaigns_for_client=campaigns_for_client, selected_campaign_name=selected_campaign_name,
                           report_data=report_data)

@app.route('/report/period', methods=['GET'])
def report_period():
    period_param = request.args.get('period')
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')

    if period_param == 'today':
        start_date = datetime.now().date()
        end_date = start_date
    elif period_param == 'yesterday':
        start_date = datetime.now().date() - timedelta(days=1)
        end_date = start_date
    elif period_param == 'week':
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
    elif period_param == 'month':
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=29)
    elif start_date_str and end_date_str:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    else:
        return render_template('report_period.html')

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM campaigns WHERE campaign_date BETWEEN ? AND ? ORDER BY campaign_date DESC",
                   (start_date.isoformat(), end_date.isoformat()))
    campaigns_in_period = cursor.fetchall()

    if not campaigns_in_period:
        conn.close()
        return render_template('report_period.html', period=period_param, start_date_str=start_date.isoformat(), end_date_str=end_date.isoformat())

    total_campaigns = len(campaigns_in_period)
    unique_accounts = set()
    total_revenue = 0
    total_messages = 0
    total_invites = 0
    total_restricted_accounts = 0
    total_messages_restricted = 0
    total_revenue_restricted = 0

    for campaign in campaigns_in_period:
        summary, results = calculate_campaign_stats(campaign['id'], conn)
        total_revenue += summary['total_revenue']
        total_messages += summary['total_messages']
        total_invites += summary['total_invites']
        unique_accounts.update([res['phone'] for res in results])
        restricted_phones = {res['phone'] for res in results if res['status_after'] not in ('Working', 'Temporary Spamblock (Resolved)')}
        total_restricted_accounts += len(restricted_phones)
        for res in results:
            if res['phone'] in restricted_phones:
                total_messages_restricted += res['msg_sent']
                total_revenue_restricted += res['revenue']

    total_unique_accounts = len(unique_accounts)
    percentage_restricted = (total_restricted_accounts / total_unique_accounts * 100) if total_unique_accounts > 0 else 0
    avg_revenue_per_account = total_revenue / total_unique_accounts if total_unique_accounts > 0 else 0
    avg_messages_all_accounts = total_messages / total_unique_accounts if total_unique_accounts > 0 else 0
    avg_messages_restricted_accounts = total_messages_restricted / total_restricted_accounts if total_restricted_accounts > 0 else 0
    avg_revenue_per_restricted_account = total_revenue_restricted / total_restricted_accounts if total_restricted_accounts > 0 else 0

    period_summary = {
        'total_campaigns': total_campaigns,
        'total_unique_accounts': total_unique_accounts,
        'total_revenue': total_revenue,
        'total_messages': total_messages,
        'total_invites': total_invites,
        'avg_revenue_per_account': avg_revenue_per_account,
        'avg_messages_all_accounts': avg_messages_all_accounts,
        'total_restricted_accounts': total_restricted_accounts,
        'percentage_restricted': percentage_restricted,
        'avg_revenue_per_restricted_account': avg_revenue_per_restricted_account,
        'avg_messages_restricted_accounts': avg_messages_restricted_accounts
    }

    # Для совместимости с шаблоном, добавляем 'date' в каждый campaign
    campaigns_in_period_with_date = []
    for camp in campaigns_in_period:
        camp_dict = dict(camp)
        camp_dict['date'] = camp['campaign_date']
        camp_dict['summary'] = calculate_campaign_stats(camp['id'], conn)[0]
        campaigns_in_period_with_date.append(camp_dict)

    conn.close()
    return render_template('report_period.html', period_summary=period_summary, campaigns_in_period=campaigns_in_period_with_date,
                           start_date_str=start_date.isoformat(), end_date_str=end_date.isoformat())

@app.route('/report/client', methods=['GET'])
def report_client():
    client_code = request.args.get('client_code')
    period_param = request.args.get('period')
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')

    if not client_code:
        return render_template('report_client.html')

    if start_date_str and end_date_str:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    elif period_param == 'today':
        start_date = end_date = datetime.now().date()
    elif period_param == 'yesterday':
        start_date = end_date = datetime.now().date() - timedelta(days=1)
    elif period_param == 'week':
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=6)
    elif period_param == 'month':
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=29)
    else:
        start_date = end_date = None

    conn = get_db_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM campaigns WHERE name LIKE ?"
    params = (f"{client_code}%",)
    if start_date and end_date:
        query += " AND campaign_date BETWEEN ? AND ?"
        params += (start_date.isoformat(), end_date.isoformat())

    cursor.execute(query, params)
    campaigns = cursor.fetchall()

    if not campaigns:
        conn.close()
        return render_template('report_client.html', client_code=client_code, start_date_str=start_date_str, end_date_str=end_date_str)

    unique_accounts = set()
    total_revenue = 0
    total_messages = 0
    total_invites = 0
    unique_restricted_accounts = set()
    total_revenue_from_restricted = 0
    total_messages_from_restricted = 0

    client_campaigns = []
    for camp in campaigns:
        summary, results = calculate_campaign_stats(camp['id'], conn)
        client_campaigns.append({
            'name': camp['name'],
            'date': camp['campaign_date'],
            'total_revenue': summary['total_revenue'],
            'total_messages': summary['total_messages'],
            'total_invites': summary['total_invites'],
            'accounts_in_report': len(results),
            'percentage_restricted': summary['percentage_restricted'],
            'avg_msg_all': summary['avg_msg_all'],
            'avg_revenue_per_account': summary['avg_revenue_per_account']
        })

        for res in results:
            unique_accounts.add(res['phone'])

        restricted_in_camp = {res['phone'] for res in results if res['status_after'] not in ('Working', 'Temporary Spamblock (Resolved)')}
        unique_restricted_accounts.update(restricted_in_camp)

        for res in results:
            if res['phone'] in restricted_in_camp:
                total_revenue_from_restricted += res['revenue']
                total_messages_from_restricted += res['msg_sent']

        total_revenue += summary['total_revenue']
        total_messages += summary['total_messages']
        total_invites += summary['total_invites']

    total_unique_accounts = len(unique_accounts)
    total_restricted_accounts = len(unique_restricted_accounts)
    percentage_restricted = (total_restricted_accounts / total_unique_accounts * 100) if total_unique_accounts > 0 else 0
    avg_revenue_per_account = total_revenue / total_unique_accounts if total_unique_accounts > 0 else 0
    avg_messages_all_accounts = total_messages / total_unique_accounts if total_unique_accounts > 0 else 0
    avg_messages_restricted_accounts = total_messages_from_restricted / total_restricted_accounts if total_restricted_accounts > 0 else 0
    avg_revenue_per_restricted_account = total_revenue_from_restricted / total_restricted_accounts if total_restricted_accounts > 0 else 0

    client_summary = {
        'client_code': client_code,
        'total_campaigns': len(campaigns),
        'total_unique_accounts': total_unique_accounts,
        'total_messages': total_messages,
        'total_invites': total_invites,
        'total_revenue': total_revenue,
        'avg_revenue_per_account': avg_revenue_per_account,
        'total_restricted_accounts': total_restricted_accounts,
        'percentage_restricted': percentage_restricted,
        'avg_messages_all_accounts': avg_messages_all_accounts,
        'avg_messages_restricted_accounts': avg_messages_restricted_accounts,
        'avg_revenue_per_restricted_account': avg_revenue_per_restricted_account
    }

    conn.close()
    return render_template('report_client.html', client_summary=client_summary, client_campaigns=client_campaigns,
                           client_code=client_code, start_date_str=start_date_str, end_date_str=end_date_str)

@app.route('/report/warmup')
def report_warmup():
    conn = get_db_connection()
    query = """
    SELECT 
        phone,
        total_revenue,
        total_messages,
        current_status,
        (JULIANDAY(first_campaign_date) - JULIANDAY(registration_date)) as rest_days
    FROM 
        accounts
    WHERE 
        registration_date IS NOT NULL 
        AND registration_date != 'N/A'
        AND first_campaign_date IS NOT NULL
    """
    accounts = conn.execute(query).fetchall()
    conn.close()

    brackets = {
        '0 - 7': {'total_revenue': 0, 'total_messages': 0, 'working_count': 0, 'perm_spam_count': 0, 'frozen_count': 0, 'accounts': []},
        '08 - 14': {'total_revenue': 0, 'total_messages': 0, 'working_count': 0, 'perm_spam_count': 0, 'frozen_count': 0, 'accounts': []},
        '15 - 30': {'total_revenue': 0, 'total_messages': 0, 'working_count': 0, 'perm_spam_count': 0, 'frozen_count': 0, 'accounts': []},
        '31 - 60': {'total_revenue': 0, 'total_messages': 0, 'working_count': 0, 'perm_spam_count': 0, 'frozen_count': 0, 'accounts': []},
        '61+': {'total_revenue': 0, 'total_messages': 0, 'working_count': 0, 'perm_spam_count': 0, 'frozen_count': 0, 'accounts': []},
        'N/A': {'total_revenue': 0, 'total_messages': 0, 'working_count': 0, 'perm_spam_count': 0, 'frozen_count': 0, 'accounts': []},
    }

    for acc in accounts:
        bracket_name = _get_warmup_bracket(acc['rest_days'])
        
        if bracket_name == 'N/A' and acc['rest_days'] is not None and acc['rest_days'] < 0:
            continue

        b = brackets[bracket_name]
        b['accounts'].append(acc['phone'])
        b['total_revenue'] += acc['total_revenue']
        b['total_messages'] += acc['total_messages']
        
        if acc['current_status'] == 'Working':
            b['working_count'] += 1
        elif acc['current_status'] == 'Permanent Spamblock' or acc['current_status'] == 'Banned':
            b['perm_spam_count'] += 1
        elif acc['current_status'] == 'Frozen':
            b['frozen_count'] += 1

    report_data = []
    for name, data in brackets.items():
        count = len(data['accounts'])
        if count == 0:
            continue

        report_data.append({
            'bracket': name,
            'account_count': count,
            'avg_ltv': data['total_revenue'] / count,
            'avg_messages': data['total_messages'] / count,
            'percent_working': (data['working_count'] / count) * 100,
            'percent_perm_spam': (data['perm_spam_count'] / count) * 100,
            'percent_frozen': (data['frozen_count'] / count) * 100
        })

    return render_template('report_warmup.html', report_data=report_data)

def _get_warmup_bracket(rest_days):
    if rest_days is None or rest_days < 0:
        return 'N/A'
    if rest_days <= 7:
        return '0 - 7'
    if rest_days <= 14:
        return '08 - 14'
    if rest_days <= 30:
        return '15 - 30'
    if rest_days <= 60:
        return '31 - 60'
    return '61+'

# ===============================================================
# ЗАПУСК ПРИЛОЖЕНИЯ
# ===============================================================
if __name__ == '__main__':
    init_db() 
    app.run(host='0.0.0.0', port=5000, debug=False)
