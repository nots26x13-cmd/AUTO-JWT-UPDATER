import os
import json
import random
import pytz
import uuid
import traceback
import threading
import eventlet

# Patch eventlet FIRST before importing requests or flask
eventlet.monkey_patch() 

import requests
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from flask_socketio import SocketIO
from github import Github
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from werkzeug.utils import secure_filename

# --- DIRECTORY CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__, template_folder=BASE_DIR)
socketio = SocketIO(app, async_mode='eventlet', cors_allowed_origins="*")

# --- SECURITY & CONFIGURATION ---
app.secret_key = "devil_super_secret_session_key_do_not_share"
ADMIN_KEY = "XDEVIL" 

USERS_FILE = os.path.join(BASE_DIR, 'users.json')
JWT_SETTING_FILE = os.path.join(BASE_DIR, 'Jwt_api_setting.txt')

BD_TZ = pytz.timezone('Asia/Dhaka')

scheduler = BackgroundScheduler(timezone=BD_TZ)
scheduler.start()

# --- Dynamic API Loader ---
def get_token_api_urls():
    if not os.path.exists(JWT_SETTING_FILE):
        default_settings = (
            "Api1:  https://arcsdc.vercel.app/api/token\n"
            "Api2:  https://sdvetbsc.vercel.app/api/token\n"
            "Api3:  https://ergtregwf.vercel.app/api/token\n"
            "Api4:  https://evrgrt.vercel.app/api/token\n"
            "Api5:  none\n"
            "Api6:  none\n"
            "Api7:  none\n"
            "Api8:  none\n"
            "Api9:  https://sdgtrb.vercel.app/api/token\n"
            "Api10:  none\n"
        )
        with open(JWT_SETTING_FILE, 'w', encoding='utf-8') as f:
            f.write(default_settings)

    urls = []
    with open(JWT_SETTING_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or ':' not in line:
                continue
            _, endpoint = line.split(':', 1)
            endpoint = endpoint.strip()
            if endpoint and endpoint.lower() != 'none' and endpoint.startswith('http'):
                urls.append(endpoint)
    return urls

# --- Security Middleware ---
@app.before_request
def check_authentication():
    allowed_routes = ['lock', 'verify_password', 'static']
    if request.endpoint not in allowed_routes and not session.get('is_authenticated'):
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.path.startswith('/api/') or request.path.startswith('/delete_file/'):
            return jsonify({"status": "error", "message": "Unauthorized."}), 401
        return redirect(url_for('lock'))

# --- Isolated Directory Management ---
def get_client_dir(username):
    safe_name = secure_filename(username)
    path = os.path.join(BASE_DIR, safe_name, 'tasks')
    os.makedirs(path, exist_ok=True)
    return path

def get_user_tasks_file(username):
    return os.path.join(get_client_dir(username), 'task.json')

def get_json_file(filepath):
    if not os.path.exists(filepath): return {}
    with open(filepath, 'r', encoding='utf-8') as f:
        try: return json.load(f)
        except: return {}

def save_json_file(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4)

def get_tasks(username):
    return get_json_file(get_user_tasks_file(username))

def save_tasks(username, tasks):
    save_json_file(get_user_tasks_file(username), tasks)

def get_users(): return get_json_file(USERS_FILE)
def save_users(users): save_json_file(USERS_FILE, users)

# --- Smooth WebSocket Emitters ---
def emit_stats(username):
    tasks = get_tasks(username)
    total_tasks = len(tasks)
    active_tasks = sum(1 for t in tasks.values() if t.get('status') != 'Stopped')
    total_tokens = sum(t.get('last_count', 0) for t in tasks.values())
    total_bans = sum(t.get('last_ban_count', 0) for t in tasks.values())
    
    socketio.emit('stats_update', {
        'client_name': username,
        'total_tasks': total_tasks,
        'active_tasks': active_tasks,
        'total_tokens': total_tokens,
        'total_bans': total_bans
    })

# --- Eventlet Concurrent Task Logic ---
def fetch_single_account(uid, password, api_urls):
    url = random.choice(api_urls)
    last_token = None
    for _ in range(3): 
        try:
            resp = requests.get(url, params={'uid': uid, 'password': password}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                token = str(data.get('token', '')).strip()
                last_token = token
                if token == "0":
                    eventlet.sleep(1)
                    continue
                if token and token.lower() != "none":
                    return {"status": "SUCCESS", "token": token}
        except Exception:
            pass
        eventlet.sleep(1)
            
    if last_token == "0":
        return {"status": "BAN"}
    return {"status": "FAILED"}

def run_token_process(task_id, username):
    try:
        tasks = get_tasks(username)
        if task_id not in tasks: return
        if tasks[task_id].get('status') == 'Stopped': return
            
        task = tasks[task_id]
        client_dir = get_client_dir(username)
        acc_path = os.path.join(client_dir, task['account_file'])
        
        with open(acc_path, 'r', encoding='utf-8') as f: 
            accounts = json.load(f)
            
        api_urls = get_token_api_urls()
        if not api_urls:
            print(f"[!] No endpoints configured for user {username}. Check Jwt_api_setting.txt")
            raise Exception("No active endpoints configured.")

        final_tokens = []
        banned_count = 0

        # Create a GreenPool to process exactly 10 concurrent connections
        pool = eventlet.GreenPool(10)

        # Process accounts in batches of 10
        for i in range(0, len(accounts), 10):
            batch = accounts[i:i+10]
            
            # Spawn green threads for this batch
            results = []
            for a in batch:
                results.append(pool.spawn(fetch_single_account, a.get('uid'), a.get('password'), api_urls))
            
            # Collect results
            for thread_job in results:
                res = thread_job.wait()
                if res['status'] == 'SUCCESS':
                    final_tokens.append({"token": res['token']})
                elif res['status'] == 'BAN':
                    banned_count += 1
            
            # Wait 3 seconds before processing the next batch of 10
            if i + 10 < len(accounts):
                eventlet.sleep(3)

        # Upload strictly valid tokens to GitHub (excluding banned nodes)
        g = Github(task['github_token'])
        repo = g.get_repo(task['repo_name'])
        content_str = json.dumps(final_tokens, indent=4)
        
        try:
            contents = repo.get_contents(task['file_path'])
            repo.update_file(task['file_path'], "Auto-update DEVIL 100K", content_str, contents.sha)
        except:
            repo.create_file(task['file_path'], "Initial build DEVIL 100K", content_str)

        tasks = get_tasks(username)
        if task_id in tasks:
            tasks[task_id]['last_run'] = datetime.now(BD_TZ).strftime("%Y-%m-%d %I:%M %p")
            tasks[task_id]['last_count'] = len(final_tokens)
            tasks[task_id]['last_ban_count'] = banned_count
            tasks[task_id]['status'] = "Active"
            save_tasks(username, tasks)
        
        emit_stats(username)
        socketio.emit('task_update', {
            'client_name': username,
            'task_id': task_id, 
            'status': 'Active',
            'last_count': len(final_tokens), 
            'last_ban_count': banned_count,
            'last_run': tasks.get(task_id, {}).get('last_run', 'Now')
        })
        
    except Exception as e:
        traceback.print_exc()
        tasks = get_tasks(username)
        if task_id in tasks:
            tasks[task_id]['status'] = "Error"
            save_tasks(username, tasks)
            
        emit_stats(username)
        socketio.emit('task_update', {
            'client_name': username,
            'task_id': task_id, 'status': 'Error',
            'last_count': tasks.get(task_id, {}).get('last_count', 0),
            'last_ban_count': tasks.get(task_id, {}).get('last_ban_count', 0),
            'last_run': tasks.get(task_id, {}).get('last_run', 'Never')
        })

def task_wrapper(task_id, username):
    try: 
        run_token_process(task_id, username)
    except Exception as e: 
        print(f"CRITICAL ERROR in task {task_id}: {e}")

def restore_scheduled_tasks():
    users = get_users()
    for token, details in users.items():
        username = details['name']
        tasks = get_tasks(username)
        for task_id, task in tasks.items():
            for i, t_str in enumerate(task.get('times', [])):
                try:
                    h, m = map(int, t_str.split(':'))
                    scheduler.add_job(
                        func=task_wrapper, trigger=CronTrigger(hour=h, minute=m, timezone=BD_TZ),
                        args=[task_id, username], id=f"{task_id}_{i}", replace_existing=True
                    )
                except: pass
restore_scheduled_tasks()

# --- Authentication Routes ---
@app.route('/')
def root(): return redirect(url_for('lock'))

@app.route('/lock')
def lock():
    if session.get('is_authenticated'):
        return redirect(url_for('admin') if session.get('role') == 'admin' else url_for('main'))
    return render_template('lock.html')

@app.route('/verify_password', methods=['POST'])
def verify_password():
    token = request.json.get('password')
    if token == ADMIN_KEY:
        session.clear()
        session['is_authenticated'] = True
        session['role'] = 'admin'
        return jsonify({"status": "success", "redirect": "/admin"})
    users = get_users()
    if token in users:
        session.clear()
        session['is_authenticated'] = True
        session['role'] = 'user'
        session['token'] = token
        session['client_name'] = users[token]['name']
        get_client_dir(users[token]['name'])
        return jsonify({"status": "success", "redirect": "/main"})
    return jsonify({"status": "error", "message": "Invalid Access Token"}), 401

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('lock'))

# --- Admin Routes ---
@app.route('/admin')
def admin():
    if session.get('role') != 'admin': return redirect(url_for('main'))
    users = get_users()
    enriched_users = {}
    for token, details in users.items():
        username = details['name']
        client_dir = get_client_dir(username)
        tasks = get_tasks(username)
        total_tasks = len(tasks)
        active_tasks = sum(1 for t in tasks.values() if t.get('status') != 'Stopped')
        total_tokens = sum(t.get('last_count', 0) for t in tasks.values())
        total_bans = sum(t.get('last_ban_count', 0) for t in tasks.values())
        files_count = 0
        if os.path.exists(client_dir):
            files_count = len([f for f in os.listdir(client_dir) if f.endswith('.json') and f != 'task.json'])
        enriched_users[token] = {
            **details, 'total_tasks': total_tasks, 'active_tasks': active_tasks,
            'total_tokens': total_tokens, 'total_bans': total_bans, 'files_count': files_count
        }
    return render_template('admin.html', users=enriched_users)

@app.route('/admin_login_as/<token>')
def admin_login_as(token):
    if session.get('role') != 'admin': 
        return redirect(url_for('lock'))
    users = get_users()
    if token in users:
        session.clear()
        session['is_authenticated'] = True
        session['role'] = 'user'
        session['token'] = token
        session['client_name'] = users[token]['name']
        get_client_dir(users[token]['name'])
        return redirect(url_for('main'))
    return redirect(url_for('admin'))

@app.route('/api/create_client', methods=['POST'])
def create_client():
    if session.get('role') != 'admin': return jsonify({"error": "Unauthorized"}), 403
    data = request.json
    name = data.get('name', 'Unknown Client')
    custom_token = data.get('token', '').strip()
    new_token = custom_token if custom_token else f"DK-{uuid.uuid4().hex[:8].upper()}"
    users = get_users()
    if new_token in users: return jsonify({"status": "error", "message": "This access token already exists!"}), 400
    users[new_token] = {"name": name, "created": datetime.now(BD_TZ).strftime("%Y-%m-%d %H:%M")}
    save_users(users)
    return jsonify({"status": "success", "token": new_token})

@app.route('/api/delete_client/<token>', methods=['DELETE'])
def delete_client(token):
    if session.get('role') != 'admin': return jsonify({"error": "Unauthorized"}), 403
    users = get_users()
    if token in users:
        del users[token]
        save_users(users)
    return jsonify({"status": "success"})

# --- Client Dashboard Routes ---
@app.route('/main')
def main():
    if session.get('role') == 'admin': return redirect(url_for('admin'))
    username = session.get('client_name')
    client_dir = get_client_dir(username)
    
    account_files = []
    if os.path.exists(client_dir):
        for f in os.listdir(client_dir):
            if f.endswith('.json') and f != 'task.json':
                count = 0
                try:
                    with open(os.path.join(client_dir, f), 'r', encoding='utf-8') as fp:
                        d = json.load(fp)
                        if isinstance(d, list): count = len(d)
                except: pass
                account_files.append({"name": f, "count": count})
                
    user_tasks = get_tasks(username)
    
    total_tasks = len(user_tasks)
    active_tasks = sum(1 for t in user_tasks.values() if t.get('status') != 'Stopped')
    total_tokens = sum(t.get('last_count', 0) for t in user_tasks.values())
    total_bans = sum(t.get('last_ban_count', 0) for t in user_tasks.values())
    
    return render_template('index.html', account_files=account_files, tasks=user_tasks, client_name=username, 
                           total_tasks=total_tasks, active_tasks=active_tasks, total_tokens=total_tokens, total_bans=total_bans)

@app.route('/add_task', methods=['POST'])
def add_task():
    data = request.form
    username = session.get('client_name')
    times = [t for t in data.getlist('times[]') if t]
    task_id = str(uuid.uuid4())[:8]
    tasks = get_tasks(username)
    
    task_data = {
        "id": task_id, "github_token": data.get('github_token'), "repo_name": data.get('repo_name'),
        "file_path": data.get('file_path'), "account_file": data.get('account_file'),
        "times": times, "status": "Active", "last_run": "Never", "last_count": 0, "last_ban_count": 0
    }
    
    tasks[task_id] = task_data
    save_tasks(username, tasks)
    
    for i, t_str in enumerate(times):
        h, m = map(int, t_str.split(':'))
        scheduler.add_job(func=task_wrapper, trigger=CronTrigger(hour=h, minute=m, timezone=BD_TZ), args=[task_id, username], id=f"{task_id}_{i}", replace_existing=True)
    
    emit_stats(username)
    socketio.emit('task_added', {'client_name': username, 'task': task_data})
    return jsonify({"status": "success"})

@app.route('/toggle_task/<task_id>', methods=['POST'])
def toggle_task(task_id):
    username = session.get('client_name')
    tasks = get_tasks(username)
    if task_id in tasks:
        current_status = tasks[task_id].get('status', 'Active')
        tasks[task_id]['status'] = 'Stopped' if current_status != 'Stopped' else 'Active'
        save_tasks(username, tasks)
        
        emit_stats(username)
        socketio.emit('task_toggled', {'client_name': username, 'task_id': task_id, 'new_state': tasks[task_id]['status']})
        return jsonify({"status": "success", "new_state": tasks[task_id]['status']})
    return jsonify({"status": "error"}), 404

@app.route('/delete_task/<task_id>')
def delete_task(task_id):
    username = session.get('client_name')
    tasks = get_tasks(username)
    if task_id in tasks:
        for i in range(len(tasks[task_id]['times'])):
            try: scheduler.remove_job(f"{task_id}_{i}")
            except: pass
        del tasks[task_id]
        save_tasks(username, tasks)
        
        emit_stats(username)
        socketio.emit('task_deleted', {'client_name': username, 'task_id': task_id})
    return jsonify({"status": "success"})

@app.route('/force_run/<task_id>', methods=['POST'])
def force_run(task_id):
    username = session.get('client_name')
    tasks = get_tasks(username)
    if task_id in tasks:
        if tasks[task_id].get('status') == 'Stopped':
            return jsonify({"status": "error", "message": "Task is stopped."}), 400
        
        # Offload safely using Eventlet's background task instead of threading
        socketio.start_background_task(task_wrapper, task_id, username)
        return jsonify({"status": "success"})
    
    return jsonify({"status": "error"}), 404

@app.route('/delete_file/<filename>', methods=['DELETE'])
def delete_file(filename):
    username = session.get('client_name')
    safe_name = secure_filename(filename)
    if safe_name == 'task.json': 
        return jsonify({"status": "error", "message": "Cannot delete core config."}), 400
        
    filepath = os.path.join(get_client_dir(username), safe_name)
    if os.path.exists(filepath):
        os.remove(filepath)
        socketio.emit('file_deleted', {'client_name': username, 'filename': safe_name})
        return jsonify({"status": "success", "message": f"Account file {safe_name} is deleted success."})
        
    return jsonify({"status": "error", "message": "File not found."}), 404

@app.route('/upload', methods=['POST'])
def upload():
    if file := request.files.get('file'):
        username = session.get('client_name')
        filename = secure_filename(file.filename)
        if filename == 'task.json': return jsonify({"status": "error", "message": "Cannot overwrite task.json"}), 400
        save_path = os.path.join(get_client_dir(username), filename)
        file.save(save_path)
        
        account_count = 0
        try:
            with open(save_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    account_count = len(data)
        except:
            pass 
        
        socketio.emit('file_uploaded', {'client_name': username, 'filename': filename, 'count': account_count})
        return jsonify({"status": "success", "filename": filename, "account_count": account_count})
    return jsonify({"status": "error"}), 400

if __name__ == '__main__':
    assigned_port = int(os.environ.get('SERVER_PORT', os.environ.get('PORT', 22468)))
    print(f"[*] Production Storage Server running on http://0.0.0.0:{assigned_port}")
    socketio.run(app, host='0.0.0.0', port=assigned_port)