#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZEN X HOST BOT v4.1
Advanced Telegram Bot Hosting Platform
Author: @zerox6t9
"""

import os
import sys
import subprocess
import sqlite3
import telebot
import threading
import time
import uuid
import signal
import random
import platform
import zipfile
import json
import logging
import shutil
import hashlib
import re
from pathlib import Path
from telebot import types
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
from flask import Flask, request, jsonify, render_template_string
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from collections import defaultdict

# ---------- Optional dependencies (auto‑fallback) ----------
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    from cryptography.fernet import Fernet
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

# ---------- Python 3.10+ stdlib module names fallback ----------
if not hasattr(sys, 'stdlib_module_names'):
    sys.stdlib_module_names = set()

# ---------- Configuration ----------
class Config:
    # Telegram
    TOKEN = os.environ.get('BOT_TOKEN', '8494225623:AAFOFfM2kZFyfW04SkNfGtRs_cK-1bHuEC8')
    ADMIN_ID = int(os.environ.get('ADMIN_ID', '7832264582'))
    ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME', 'zerox6t9')
    BOT_USERNAME = os.environ.get('BOT_USERNAME', 'zen_xbot')

    # Paths
    PROJECT_DIR = 'projects'
    DB_NAME = 'zenx_host.db'
    BACKUP_DIR = 'backups'
    LOGS_DIR = 'logs'
    EXPORTS_DIR = 'exports'
    TEMP_DIR = 'temp'

    # Server
    PORT = int(os.environ.get('PORT', 10000))
    MAINTENANCE = False
    AUTO_RESTART_BOTS = True

    # Limits
    MAX_BOTS_PER_USER = 10
    MAX_CONCURRENT_DEPLOYMENTS = 5
    MAX_FILE_SIZE_MB = 5.5
    BOT_TIMEOUT = 300
    MAX_LOG_LINES = 5000

    # Backup
    BACKUP_INTERVAL = 3600  # 1 hour
    AUTO_BACKUP_THRESHOLDS = {
        'cpu_percent': 80,
        'ram_percent': 85,
        'disk_percent': 90,
        'crash_rate': 5
    }

    # Hosting nodes (300 capacity each)
    HOSTING_NODES = [
        {"name": "Node-1", "region": "Asia", "capacity": 300},
        {"name": "Node-2", "region": "Asia", "capacity": 300},
        {"name": "Node-3", "region": "Europe", "capacity": 300}
    ]

    # Rate limiting (actions per minute)
    RATE_LIMIT = {
        'deploy': (2, 60),
        'restart': (3, 60),
        'export': (5, 60)
    }

    # Session timeout (seconds)
    SESSION_TIMEOUT = 600

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('zenx_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- Globals ----------
bot = telebot.TeleBot(Config.TOKEN, parse_mode="Markdown")
app = Flask(__name__)
executor = ThreadPoolExecutor(max_workers=10)
db_lock = threading.RLock()
user_sessions = {}
user_message_history = {}
bot_monitors = {}
rate_limiter = defaultdict(list)
system_health = {
    'last_emergency_backup': None,
    'crash_counter': defaultdict(int),
    'crash_timestamps': defaultdict(list)
}

# Create required directories
for d in [Config.PROJECT_DIR, Config.BACKUP_DIR, Config.LOGS_DIR, Config.EXPORTS_DIR, Config.TEMP_DIR]:
    Path(d).mkdir(exist_ok=True)

# ---------- Database Helpers (Thread‑Safe) ----------
def get_db():
    with db_lock:
        conn = sqlite3.connect(Config.DB_NAME, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

def execute_db(query, params=(), fetchone=False, fetchall=False, commit=False):
    with db_lock:
        conn = sqlite3.connect(Config.DB_NAME, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        try:
            c.execute(query, params)
            if commit:
                conn.commit()
            if fetchone:
                res = c.fetchone()
            elif fetchall:
                res = c.fetchall()
            else:
                res = None
            return res
        except Exception as e:
            logger.error(f"DB error: {e}")
            return None
        finally:
            conn.close()

# ---------- Database Initialization ----------
def init_db():
    with get_db() as conn:
        c = conn.cursor()
        # Users
        c.execute('''CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            expiry TEXT,
            file_limit INTEGER DEFAULT 1,
            is_prime INTEGER DEFAULT 0,
            join_date TEXT,
            last_renewal TEXT,
            total_bots_deployed INTEGER DEFAULT 0,
            total_deployments INTEGER DEFAULT 0,
            last_active TEXT,
            banned INTEGER DEFAULT 0,
            language TEXT DEFAULT 'en'
        )''')
        # Prime keys
        c.execute('''CREATE TABLE IF NOT EXISTS keys (
            key TEXT PRIMARY KEY,
            duration_days INTEGER,
            file_limit INTEGER,
            created_date TEXT,
            used_by INTEGER,
            used_date TEXT,
            is_used INTEGER DEFAULT 0
        )''')
        # Deployments (bots)
        c.execute('''CREATE TABLE IF NOT EXISTS deployments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            bot_name TEXT,
            filename TEXT,
            pid INTEGER,
            start_time TEXT,
            status TEXT,
            cpu_usage REAL DEFAULT 0,
            ram_usage REAL DEFAULT 0,
            last_active TEXT,
            node_id INTEGER,
            logs TEXT,
            restart_count INTEGER DEFAULT 0,
            auto_restart INTEGER DEFAULT 1,
            public_bot INTEGER DEFAULT 0,
            description TEXT,
            tags TEXT,
            created_at TEXT,
            updated_at TEXT
        )''')
        # Hosting nodes
        c.execute('''CREATE TABLE IF NOT EXISTS nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            status TEXT DEFAULT 'active',
            capacity INTEGER DEFAULT 300,
            current_load INTEGER DEFAULT 0,
            region TEXT DEFAULT 'Global',
            total_deployed INTEGER DEFAULT 0,
            last_check TEXT
        )''')
        # Server logs
        c.execute('''CREATE TABLE IF NOT EXISTS server_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            event TEXT,
            details TEXT,
            user_id INTEGER
        )''')
        # Bot logs
        c.execute('''CREATE TABLE IF NOT EXISTS bot_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id INTEGER,
            timestamp TEXT,
            log_type TEXT,
            message TEXT
        )''')
        # Notifications
        c.execute('''CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            message TEXT,
            is_read INTEGER DEFAULT 0,
            created_at TEXT
        )''')
        # Bot statistics (views/downloads)
        c.execute('''CREATE TABLE IF NOT EXISTS bot_stats (
            bot_id INTEGER PRIMARY KEY,
            views INTEGER DEFAULT 0,
            downloads INTEGER DEFAULT 0,
            last_viewed TEXT
        )''')
        conn.commit()

    # Insert default nodes if missing
    for node in Config.HOSTING_NODES:
        exist = execute_db("SELECT id FROM nodes WHERE name=?", (node['name'],), fetchone=True)
        if not exist:
            execute_db("INSERT INTO nodes (name, status, capacity, region, last_check) VALUES (?,?,?,?,?)",
                       (node['name'], 'active', node['capacity'], node['region'], datetime.now().isoformat()),
                       commit=True)

    # Ensure admin user exists
    admin = execute_db("SELECT id FROM users WHERE id=?", (Config.ADMIN_ID,), fetchone=True)
    if not admin:
        now = datetime.now().isoformat()
        execute_db("INSERT INTO users (id, username, expiry, file_limit, is_prime, join_date, last_active) VALUES (?,?,?,?,?,?,?)",
                   (Config.ADMIN_ID, Config.ADMIN_USERNAME,
                    (datetime.now() + timedelta(days=3650)).isoformat(),
                    999, 1, now, now), commit=True)
    logger.info("Database initialized.")

# ---------- Utility Functions ----------
def rate_limit(action, user_id):
    if user_id == Config.ADMIN_ID:
        return True
    limit, period = Config.RATE_LIMIT.get(action, (5, 60))
    now = time.time()
    key = f"{user_id}:{action}"
    timestamps = rate_limiter[key]
    timestamps = [t for t in timestamps if now - t < period]
    rate_limiter[key] = timestamps
    if len(timestamps) >= limit:
        return False
    timestamps.append(now)
    return True

def get_process_stats(pid):
    if not pid or pid <= 0:
        return None
    try:
        if PSUTIL_AVAILABLE:
            proc = psutil.Process(pid)
            with proc.oneshot():
                cpu = proc.cpu_percent(interval=0.1)
                mem = proc.memory_percent()
                uptime_seconds = time.time() - proc.create_time()
                return {
                    'pid': pid,
                    'cpu': cpu,
                    'memory': mem,
                    'uptime': str(timedelta(seconds=int(uptime_seconds))),
                    'status': 'running'
                }
        else:
            if platform.system() == "Windows":
                cmd = f'tasklist /FI "PID eq {pid}" /FO CSV'
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if str(pid) in result.stdout:
                    return {'pid': pid, 'cpu': 0.0, 'memory': 0.0, 'status': 'running'}
            else:
                cmd = f'ps -p {pid} -o pcpu,pmem,etime --no-headers'
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if result.returncode == 0 and result.stdout.strip():
                    parts = result.stdout.strip().split()
                    if len(parts) >= 3:
                        return {
                            'pid': pid,
                            'cpu': float(parts[0]),
                            'memory': float(parts[1]),
                            'uptime': parts[2],
                            'status': 'running'
                        }
    except Exception:
        pass
    return None

def calculate_uptime(start_time_str):
    if not start_time_str:
        return "N/A"
    try:
        start = datetime.fromisoformat(start_time_str)
        delta = datetime.now() - start
        days = delta.days
        hours, rem = divmod(delta.seconds, 3600)
        minutes, sec = divmod(rem, 60)
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        if hours > 0:
            return f"{hours}h {minutes}m {sec}s"
        if minutes > 0:
            return f"{minutes}m {sec}s"
        return f"{sec}s"
    except:
        return "N/A"

def create_progress_bar(percent, width=10):
    filled = int(percent * width / 100)
    return "█" * filled + "░" * (width - filled)

def log_event(event, details, user_id=None):
    try:
        ts = datetime.now().isoformat()
        execute_db("INSERT INTO server_logs (timestamp, event, details, user_id) VALUES (?,?,?,?)",
                   (ts, event, details[:500], user_id), commit=True)
    except:
        pass

def send_notification(user_id, message):
    try:
        execute_db("INSERT INTO notifications (user_id, message, created_at) VALUES (?,?,?)",
                   (user_id, message, datetime.now().isoformat()), commit=True)
        bot.send_message(user_id, f"📢 **Notification:** {message}")
    except:
        pass

def log_bot_event(bot_id, log_type, message):
    try:
        execute_db("INSERT INTO bot_logs (bot_id, timestamp, log_type, message) VALUES (?,?,?,?)",
                   (bot_id, datetime.now().isoformat(), log_type, message[:500]), commit=True)
    except:
        pass

def check_prime_status(user_id):
    user = execute_db("SELECT expiry FROM users WHERE id=?", (user_id,), fetchone=True)
    if not user or not user['expiry']:
        return {'expired': True, 'message': 'No Prime subscription'}
    try:
        expiry = datetime.fromisoformat(user['expiry'])
        now = datetime.now()
        if expiry > now:
            delta = expiry - now
            return {'expired': False, 'days_left': delta.days, 'hours_left': delta.seconds//3600,
                    'expiry_date': expiry.isoformat()}
        else:
            return {'expired': True, 'days_expired': (now - expiry).days, 'expiry_date': expiry.isoformat()}
    except:
        return {'expired': True, 'message': 'Invalid expiry format'}

# ---------- Session Management ----------
def get_session(user_id):
    sess = user_sessions.get(user_id, {})
    if 'expires' in sess and sess['expires'] < time.time():
        del user_sessions[user_id]
        return {}
    return sess

def set_session(user_id, data):
    data['expires'] = time.time() + Config.SESSION_TIMEOUT
    user_sessions[user_id] = data

def clear_session(user_id):
    user_sessions.pop(user_id, None)

def update_message_history(chat_id, msg_id):
    if chat_id not in user_message_history:
        user_message_history[chat_id] = []
    user_message_history[chat_id].append(msg_id)
    if len(user_message_history[chat_id]) > 10:
        user_message_history[chat_id] = user_message_history[chat_id][-10:]

def edit_or_send(chat_id, message_id, text, reply_markup=None, parse_mode="Markdown"):
    try:
        if message_id:
            try:
                return bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup, parse_mode=parse_mode)
            except telebot.apihelper.ApiException as e:
                if "message can't be edited" in str(e):
                    msg = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
                    update_message_history(chat_id, msg.message_id)
                    return msg
                else:
                    raise
        else:
            msg = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
            update_message_history(chat_id, msg.message_id)
            return msg
    except Exception:
        msg = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
        update_message_history(chat_id, msg.message_id)
        return msg

# ---------- System Health & Auto‑Backup ----------
def get_system_stats():
    stats = {
        'cpu_percent': 0.0,
        'ram_percent': 0.0,
        'disk_percent': 0.0,
        'total_users': 0,
        'prime_users': 0,
        'total_bots': 0,
        'running_bots': 0,
        'deployed_today': 0,
        'uptime_days': 0,
        'total_capacity': 0,
        'used_capacity': 0,
        'available_capacity': 0,
        'last_backup': 'Never',
        'backup_count': 0,
        'platform': platform.system(),
        'python_version': platform.python_version(),
        'crash_rate': 0.0
    }
    try:
        if PSUTIL_AVAILABLE:
            stats['cpu_percent'] = psutil.cpu_percent(interval=0.5)
            stats['ram_percent'] = psutil.virtual_memory().percent
            stats['disk_percent'] = psutil.disk_usage('/').percent
        total_users = execute_db("SELECT COUNT(*) FROM users", fetchone=True)
        stats['total_users'] = total_users[0] if total_users else 0
        prime = execute_db("SELECT COUNT(*) FROM users WHERE is_prime=1", fetchone=True)
        stats['prime_users'] = prime[0] if prime else 0
        total_bots = execute_db("SELECT COUNT(*) FROM deployments", fetchone=True)
        stats['total_bots'] = total_bots[0] if total_bots else 0
        running = execute_db("SELECT COUNT(*) FROM deployments WHERE status='Running'", fetchone=True)
        stats['running_bots'] = running[0] if running else 0
        today = datetime.now().date().isoformat()
        deployed = execute_db("SELECT COUNT(*) FROM deployments WHERE DATE(created_at)=?", (today,), fetchone=True)
        stats['deployed_today'] = deployed[0] if deployed else 0
        nodes = execute_db("SELECT * FROM nodes", fetchall=True) or []
        stats['total_capacity'] = sum(n['capacity'] for n in nodes)
        stats['used_capacity'] = sum(n['current_load'] for n in nodes)
        stats['available_capacity'] = stats['total_capacity'] - stats['used_capacity']
        backup_files = list(Path(Config.BACKUP_DIR).glob("*.zip"))
        stats['backup_count'] = len(backup_files)
        if backup_files:
            latest = max(backup_files, key=lambda f: f.stat().st_mtime)
            stats['last_backup'] = datetime.fromtimestamp(latest.stat().st_mtime).isoformat()
        now = time.time()
        all_crashes = [t for ts_list in system_health['crash_timestamps'].values() for t in ts_list if now - t < 300]
        stats['crash_rate'] = len(all_crashes) / 5.0
    except Exception as e:
        logger.error(f"Stats error: {e}")
    return stats

def full_system_backup(reason="manual"):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_name = f"full_backup_{reason}_{timestamp}"
    backup_dir = Path(Config.BACKUP_DIR) / backup_name
    backup_dir.mkdir(parents=True, exist_ok=True)
    try:
        with db_lock:
            shutil.copy2(Config.DB_NAME, backup_dir / Config.DB_NAME)
        shutil.copytree(Config.PROJECT_DIR, backup_dir / Config.PROJECT_DIR, dirs_exist_ok=True)
        shutil.copytree(Config.LOGS_DIR, backup_dir / Config.LOGS_DIR, dirs_exist_ok=True)
        config_dict = {k: getattr(Config, k) for k in dir(Config) if not k.startswith('_') and not callable(getattr(Config, k))}
        with open(backup_dir / 'config.json', 'w') as f:
            json.dump(config_dict, f, indent=2, default=str)
        with open(backup_dir / 'system_stats.json', 'w') as f:
            json.dump(get_system_stats(), f, indent=2, default=str)
        zip_path = Path(Config.BACKUP_DIR) / f"{backup_name}.zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(backup_dir):
                for file in files:
                    full = os.path.join(root, file)
                    rel = os.path.relpath(full, backup_dir)
                    zf.write(full, rel)
        shutil.rmtree(backup_dir)
        logger.info(f"Full backup created: {zip_path}")
        return zip_path
    except Exception as e:
        logger.error(f"Full backup failed: {e}")
        return None

def health_check_thread():
    while True:
        time.sleep(60)
        try:
            stats = get_system_stats()
            thresholds = Config.AUTO_BACKUP_THRESHOLDS
            alerts = []
            if stats['cpu_percent'] > thresholds['cpu_percent']:
                alerts.append(f"CPU: {stats['cpu_percent']:.1f}% > {thresholds['cpu_percent']}%")
            if stats['ram_percent'] > thresholds['ram_percent']:
                alerts.append(f"RAM: {stats['ram_percent']:.1f}% > {thresholds['ram_percent']}%")
            if stats['disk_percent'] > thresholds['disk_percent']:
                alerts.append(f"Disk: {stats['disk_percent']:.1f}% > {thresholds['disk_percent']}%")
            if stats['crash_rate'] > thresholds['crash_rate']:
                alerts.append(f"Crash rate: {stats['crash_rate']:.1f}/min > {thresholds['crash_rate']}/min")
            if alerts and Config.ADMIN_ID:
                reason = "auto_" + "_".join(a.split()[0].lower() for a in alerts[:2])
                zip_path = full_system_backup(reason=reason)
                if zip_path:
                    msg = "🚨 **Emergency Auto‑Backup Triggered**\nReason: " + ", ".join(alerts)
                    with open(zip_path, 'rb') as f:
                        bot.send_document(Config.ADMIN_ID, f, caption=msg)
                    system_health['last_emergency_backup'] = datetime.now().isoformat()
        except Exception as e:
            logger.error(f"Health check error: {e}")

# ---------- Bot Process Management ----------
def assign_bot_to_node():
    nodes = execute_db("SELECT * FROM nodes WHERE status='active'", fetchall=True)
    if not nodes:
        return None
    return min(nodes, key=lambda n: n['current_load'] / n['capacity'] if n['capacity'] > 0 else float('inf'))

def start_bot_process(bot_id, user_id, filename, node_id=None):
    file_path = Path(Config.PROJECT_DIR) / filename
    if not file_path.exists():
        return False, "File not found"
    if not node_id:
        node = assign_bot_to_node()
        if not node:
            return False, "No available node"
        node_id = node['id']
    else:
        node = execute_db("SELECT * FROM nodes WHERE id=?", (node_id,), fetchone=True)
    try:
        log_file = open(f'{Config.LOGS_DIR}/bot_{bot_id}.log', 'a')
        log_file.write(f"\n{'='*50}\nStart at {datetime.now().isoformat()}\n{'='*50}\n")
        proc = subprocess.Popen(
            ['python', str(file_path)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
        time.sleep(2)
        if proc.poll() is not None:
            log_file.close()
            return False, "Bot process exited immediately"
        now = datetime.now().isoformat()
        execute_db("""
            UPDATE deployments SET pid=?, start_time=?, status='Running',
            node_id=?, last_active=?, updated_at=?
            WHERE id=?
        """, (proc.pid, now, node_id, now, now, bot_id), commit=True)
        execute_db("UPDATE nodes SET current_load=current_load+1, total_deployed=total_deployed+1 WHERE id=?",
                   (node_id,), commit=True)
        log_file.close()
        start_monitor(bot_id, proc.pid, user_id)
        return True, "Bot started"
    except Exception as e:
        logger.error(f"Start process error: {e}")
        return False, str(e)

def start_monitor(bot_id, pid, user_id):
    def monitor():
        while True:
            try:
                os.kill(pid, 0)
            except OSError:
                bot_info = execute_db("SELECT bot_name, auto_restart, restart_count FROM deployments WHERE id=?", (bot_id,), fetchone=True)
                if not bot_info:
                    break
                now = datetime.now().isoformat()
                system_health['crash_counter'][bot_id] += 1
                system_health['crash_timestamps'][bot_id].append(time.time())
                if bot_info['auto_restart'] == 1:
                    recent_crashes = [t for t in system_health['crash_timestamps'][bot_id] if time.time() - t < 600]
                    if len(recent_crashes) >= 3:
                        execute_db("UPDATE deployments SET auto_restart=0 WHERE id=?", (bot_id,), commit=True)
                        send_notification(user_id, f"⚠️ Bot '{bot_info['bot_name']}' crashed 3 times in 10 minutes. Auto‑restart disabled.")
                        execute_db("UPDATE deployments SET status='Crashed', pid=0, updated_at=? WHERE id=?", (now, bot_id), commit=True)
                        break
                    else:
                        execute_db("UPDATE deployments SET status='Restarting', restart_count=restart_count+1, updated_at=? WHERE id=?", (now, bot_id), commit=True)
                        time.sleep(5)
                        bot_data = execute_db("SELECT filename, node_id FROM deployments WHERE id=?", (bot_id,), fetchone=True)
                        if bot_data:
                            success, msg = start_bot_process(bot_id, user_id, bot_data['filename'], bot_data['node_id'])
                            if success:
                                log_bot_event(bot_id, "AUTO_RESTART", "Bot auto‑restarted")
                                send_notification(user_id, f"🔄 Bot '{bot_info['bot_name']}' auto‑restarted")
                                break
                execute_db("UPDATE deployments SET status='Stopped', pid=0, last_active=?, updated_at=? WHERE id=?",
                           (now, now, bot_id), commit=True)
                execute_db("UPDATE nodes SET current_load=current_load-1 WHERE id=(SELECT node_id FROM deployments WHERE id=?)", (bot_id,), commit=True)
                log_bot_event(bot_id, "STOPPED", "Bot process terminated")
                break
            time.sleep(30)
            if PSUTIL_AVAILABLE:
                try:
                    proc = psutil.Process(pid)
                    cpu = proc.cpu_percent()
                    mem = proc.memory_percent()
                    execute_db("UPDATE deployments SET cpu_usage=?, ram_usage=?, last_active=? WHERE id=?",
                               (cpu, mem, datetime.now().isoformat(), bot_id), commit=True)
                except:
                    pass
    thr = threading.Thread(target=monitor, daemon=True)
    thr.start()
    bot_monitors[bot_id] = thr

def recover_deployments():
    bots = execute_db("SELECT id, user_id, filename, node_id FROM deployments WHERE auto_restart=1 AND (status='Running' OR status='Restarting')", fetchall=True)
    for b in bots:
        start_bot_process(b['id'], b['user_id'], b['filename'], b['node_id'])
    logger.info(f"Recovered {len(bots)} bots.")

# ---------- Keyboard Layouts ----------
def main_keyboard(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    prime = check_prime_status(user_id)
    if not prime['expired']:
        btns = ["📤 Upload Bot", "🤖 My Bots", "🚀 Deploy Bot", "📊 Dashboard",
                "⚙️ Settings", "👑 Prime Info", "🔔 Notifications", "🌍 Explore Bots"]
    else:
        btns = ["🔑 Activate Prime", "👑 Prime Info", "📞 Contact Admin", "ℹ️ Help"]
    markup.add(*[types.KeyboardButton(b) for b in btns])
    if user_id == Config.ADMIN_ID:
        markup.add(types.KeyboardButton("👑 Admin Panel"))
    return markup

def admin_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    btns = ["🎫 Generate Key", "👥 All Users", "🤖 All Bots", "📈 Statistics",
            "🗄️ Database", "💾 Backup", "⚙️ Maintenance", "🌐 Nodes",
            "🔧 Logs", "📊 System", "🔔 Broadcast", "🔄 Cleanup", "🏠 Main Menu"]
    for i in range(0, len(btns), 2):
        markup.add(*[types.KeyboardButton(b) for b in btns[i:i+2]])
    return markup

def bot_actions_keyboard(bot_id):
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🛑 Stop", callback_data=f"stop_{bot_id}"),
        types.InlineKeyboardButton("🔄 Restart", callback_data=f"restart_{bot_id}"),
        types.InlineKeyboardButton("📥 Export", callback_data=f"export_{bot_id}"),
        types.InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{bot_id}"),
        types.InlineKeyboardButton("📜 Logs", callback_data=f"logs_{bot_id}"),
        types.InlineKeyboardButton("🔁 Auto-Restart", callback_data=f"autorestart_{bot_id}")
    )
    markup.add(
        types.InlineKeyboardButton("📊 Stats", callback_data=f"stats_{bot_id}"),
        types.InlineKeyboardButton("🔙 Back", callback_data="my_bots")
    )
    return markup

# ---------- Command Handlers ----------
@bot.message_handler(commands=['start', 'menu', 'help'])
def cmd_start(message):
    uid = message.from_user.id
    username = message.from_user.username or f"User_{uid}"
    if Config.MAINTENANCE and uid != Config.ADMIN_ID:
        bot.reply_to(message, "🛠 **System Maintenance**\nPlease try later.")
        return
    user = execute_db("SELECT * FROM users WHERE id=?", (uid,), fetchone=True)
    if not user:
        now = datetime.now().isoformat()
        execute_db("INSERT INTO users (id, username, join_date, last_active) VALUES (?,?,?,?)",
                   (uid, username, now, now), commit=True)
    clear_session(uid)
    prime = check_prime_status(uid)
    text = f"""
🤖 **ZEN X HOST BOT v4.1**
*Auto‑Recovery & Emergency Backup Active*
━━━━━━━━━━━━━━━━━━━━
👤 **User:** @{username}
💎 **Status:** {'PRIME 👑' if not prime['expired'] else 'FREE'}
📅 **Expiry:** {prime.get('expiry_date', 'N/A') if not prime['expired'] else 'Not Activated'}
━━━━━━━━━━━━━━━━━━━━
Use buttons below or type commands:
/upload, /mybots, /deploy <id>, /explore
    """
    bot.send_message(uid, text, reply_markup=main_keyboard(uid))

@bot.message_handler(commands=['admin'])
def cmd_admin(message):
    uid = message.from_user.id
    if uid == Config.ADMIN_ID:
        bot.send_message(uid, "👑 **Admin Panel**", reply_markup=admin_keyboard())
    else:
        bot.reply_to(message, "⛔ Access Denied.")

@bot.message_handler(commands=['upload'])
def cmd_upload(message):
    uid = message.from_user.id
    prime = check_prime_status(uid)
    if prime['expired']:
        bot.reply_to(message, "⚠️ Prime required to upload files.")
        return
    count = execute_db("SELECT COUNT(*) FROM deployments WHERE user_id=?", (uid,), fetchone=True)[0]
    if count >= Config.MAX_BOTS_PER_USER:
        bot.reply_to(message, f"❌ Max {Config.MAX_BOTS_PER_USER} bots allowed.")
        return
    set_session(uid, {'state': 'waiting_for_file'})
    bot.reply_to(message, "📤 **Send your Python (.py) or ZIP file** (max 5.5MB)")

@bot.message_handler(commands=['mybots'])
def cmd_mybots(message):
    uid = message.from_user.id
    bots = execute_db("SELECT id, bot_name, status, auto_restart, created_at FROM deployments WHERE user_id=? ORDER BY id DESC", (uid,), fetchall=True)
    if not bots:
        bot.send_message(uid, "🤖 **No bots found.** Use /upload to add one.")
        return
    text = f"**Your Bots** ({len(bots)})\n━━━━━━━━━━━━━━━━━━━━\n"
    markup = types.InlineKeyboardMarkup(row_width=1)
    for b in bots:
        status_icon = "🟢" if b['status'] == 'Running' else "🔴"
        auto = "🔁" if b['auto_restart'] else "⏸️"
        text += f"{status_icon}{auto} **{b['bot_name']}** (ID: {b['id']})\n"
        markup.add(types.InlineKeyboardButton(f"{status_icon}{auto} {b['bot_name']}", callback_data=f"bot_{b['id']}"))
    bot.send_message(uid, text, reply_markup=markup)

@bot.message_handler(commands=['deploy'])
def cmd_deploy(message):
    uid = message.from_user.id
    prime = check_prime_status(uid)
    if prime['expired']:
        bot.reply_to(message, "⚠️ Prime required to deploy.")
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /deploy <bot_id>")
        return
    try:
        bot_id = int(parts[1])
    except:
        bot.reply_to(message, "Invalid bot ID.")
        return
    bot_info = execute_db("SELECT * FROM deployments WHERE id=? AND user_id=?", (bot_id, uid), fetchone=True)
    if not bot_info:
        bot.reply_to(message, "Bot not found or not yours.")
        return
    if not rate_limit('deploy', uid):
        bot.reply_to(message, "⏳ Too many deployments. Try later.")
        return
    success, msg = start_bot_process(bot_id, uid, bot_info['filename'])
    if success:
        bot.reply_to(message, f"✅ Bot **{bot_info['bot_name']}** deployed!")
    else:
        bot.reply_to(message, f"❌ Deployment failed: {msg}")

@bot.message_handler(commands=['stop', 'restart', 'delete', 'export', 'logs'])
def cmd_bot_action(message):
    uid = message.from_user.id
    cmd = message.text.split()[0][1:]
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, f"Usage: /{cmd} <bot_id>")
        return
    try:
        bot_id = int(parts[1])
    except:
        bot.reply_to(message, "Invalid bot ID.")
        return
    bot_info = execute_db("SELECT * FROM deployments WHERE id=? AND (user_id=? OR ?=?)", 
                          (bot_id, uid, uid, Config.ADMIN_ID), fetchone=True)
    if not bot_info:
        bot.reply_to(message, "Bot not found or access denied.")
        return
    if cmd == 'stop':
        stop_bot_logic(bot_id, uid)
        bot.reply_to(message, f"🛑 Bot **{bot_info['bot_name']}** stopped.")
    elif cmd == 'restart':
        restart_bot_logic(bot_id, uid)
        bot.reply_to(message, f"🔄 Restarting **{bot_info['bot_name']}**...")
    elif cmd == 'delete':
        delete_bot_logic(bot_id, uid)
        bot.reply_to(message, f"🗑️ Bot **{bot_info['bot_name']}** deleted.")
    elif cmd == 'export':
        export_bot_logic(bot_id, uid, message.chat.id)
    elif cmd == 'logs':
        show_bot_logs_logic(bot_id, message.chat.id)

@bot.message_handler(commands=['dashboard', 'stats'])
def cmd_dashboard(message):
    uid = message.from_user.id
    stats = get_system_stats()
    user_stats = execute_db("SELECT total_bots_deployed, total_deployments FROM users WHERE id=?", (uid,), fetchone=True)
    bots = execute_db("SELECT COUNT(*) as total, SUM(CASE WHEN status='Running' THEN 1 ELSE 0 END) as running FROM deployments WHERE user_id=?", (uid,), fetchone=True)
    prime = check_prime_status(uid)
    text = f"""
📊 **Dashboard**
━━━━━━━━━━━━━━━━━━━━
👤 **User ID:** `{uid}`
💎 **Prime:** {'Active' if not prime['expired'] else 'Inactive'}
📦 **Bots:** {bots['total'] or 0} total, {bots['running'] or 0} running
📈 **Deployments:** {user_stats['total_deployments'] if user_stats else 0}
━━━━━━━━━━━━━━━━━━━━
🖥️ **System**
• CPU: {create_progress_bar(stats['cpu_percent'])} {stats['cpu_percent']:.1f}%
• RAM: {create_progress_bar(stats['ram_percent'])} {stats['ram_percent']:.1f}%
• Disk: {create_progress_bar(stats['disk_percent'])} {stats['disk_percent']:.1f}%
• Capacity: {stats['used_capacity']}/{stats['total_capacity']}
━━━━━━━━━━━━━━━━━━━━
    """
    bot.send_message(uid, text)

@bot.message_handler(commands=['explore', 'public'])
def cmd_explore(message):
    uid = message.from_user.id
    page = 1
    per_page = 5
    bots = execute_db("""
        SELECT d.id, d.bot_name, d.description, d.tags, u.username, d.created_at
        FROM deployments d
        LEFT JOIN users u ON d.user_id = u.id
        WHERE d.public_bot = 1
        ORDER BY d.id DESC LIMIT ? OFFSET ?
    """, (per_page, (page-1)*per_page), fetchall=True)
    if not bots:
        bot.send_message(uid, "🌍 No public bots available.")
        return
    text = "🌍 **Public Bot Store**\n━━━━━━━━━━━━━━━━━━━━\n"
    markup = types.InlineKeyboardMarkup()
    for b in bots:
        text += f"🤖 **{b['bot_name']}** by @{b['username']}\nID: `{b['id']}`\n{b['description'] or 'No description'}\n━━━━━━━━━━━━━━━━━━━━\n"
        markup.add(types.InlineKeyboardButton(f"View {b['bot_name']}", callback_data=f"viewpublic_{b['id']}"))
    bot.send_message(uid, text, reply_markup=markup)

@bot.message_handler(commands=['activate'])
def cmd_activate(message):
    uid = message.from_user.id
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /activate <key>")
        return
    key = parts[1].strip().upper()
    process_key(uid, key, message.chat.id)

@bot.message_handler(commands=['backup', 'fullbackup'])
def cmd_backup(message):
    uid = message.from_user.id
    if uid != Config.ADMIN_ID:
        bot.reply_to(message, "⛔ Admin only.")
        return
    bot.reply_to(message, "💾 Creating full system backup...")
    zip_path = full_system_backup(reason="manual")
    if zip_path:
        with open(zip_path, 'rb') as f:
            bot.send_document(uid, f, caption=f"✅ Full backup completed\n{datetime.now().isoformat()}")
    else:
        bot.reply_to(message, "❌ Backup failed.")

@bot.message_handler(commands=['broadcast'])
def cmd_broadcast(message):
    uid = message.from_user.id
    if uid != Config.ADMIN_ID:
        return
    text = message.text.replace('/broadcast', '', 1).strip()
    if not text:
        bot.reply_to(message, "Usage: /broadcast <message>")
        return
    users = execute_db("SELECT id FROM users", fetchall=True) or []
    success = 0
    for u in users:
        try:
            bot.send_message(u['id'], f"📢 **Broadcast**\n\n{text}")
            success += 1
        except:
            pass
    bot.reply_to(message, f"✅ Broadcast sent to {success}/{len(users)} users.")

# ---------- Document Upload Handler ----------
@bot.message_handler(content_types=['document'])
def handle_docs(message):
    uid = message.from_user.id
    sess = get_session(uid)
    if sess.get('state') != 'waiting_for_file':
        return
    file_name = message.document.file_name.lower()
    if not (file_name.endswith('.py') or file_name.endswith('.zip')):
        bot.reply_to(message, "❌ Only .py or .zip files allowed.")
        return
    if message.document.file_size > Config.MAX_FILE_SIZE_MB * 1024 * 1024:
        bot.reply_to(message, f"❌ File too large. Max {Config.MAX_FILE_SIZE_MB}MB.")
        return
    file_info = bot.get_file(message.document.file_id)
    downloaded = bot.download_file(file_info.file_path)
    safe_name = secure_filename(message.document.file_name)
    counter = 1
    orig = safe_name
    while (Path(Config.PROJECT_DIR) / safe_name).exists():
        name, ext = os.path.splitext(orig)
        safe_name = f"{name}_{counter}{ext}"
        counter += 1
    file_path = Path(Config.PROJECT_DIR) / safe_name
    file_path.write_bytes(downloaded)
    if file_name.endswith('.zip'):
        extract_dir = Path(Config.TEMP_DIR) / f"extract_{uid}_{int(time.time())}"
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(file_path, 'r') as zf:
            zf.extractall(extract_dir)
        py_files = list(extract_dir.rglob('*.py'))
        if not py_files:
            shutil.rmtree(extract_dir)
            file_path.unlink()
            bot.reply_to(message, "❌ No Python file found in ZIP.")
            return
        main_py = py_files[0]
        new_name = secure_filename(main_py.name)
        counter = 1
        while (Path(Config.PROJECT_DIR) / new_name).exists():
            name, ext = os.path.splitext(main_py.name)
            new_name = f"{name}_{counter}{ext}"
            counter += 1
        shutil.copy2(main_py, Path(Config.PROJECT_DIR) / new_name)
        file_path.unlink()
        shutil.rmtree(extract_dir)
        safe_name = new_name
    set_session(uid, {'state': 'waiting_for_bot_name', 'filename': safe_name})
    bot.reply_to(message, "✅ File uploaded.\nNow send a **name** for your bot (max 30 chars):")

@bot.message_handler(func=lambda m: get_session(m.from_user.id).get('state') == 'waiting_for_bot_name')
def process_bot_name(message):
    uid = message.from_user.id
    sess = get_session(uid)
    if message.text.lower() == 'cancel':
        clear_session(uid)
        bot.reply_to(message, "❌ Cancelled.", reply_markup=main_keyboard(uid))
        return
    bot_name = message.text.strip()[:50]
    filename = sess['filename']
    now = datetime.now().isoformat()
    execute_db("""
        INSERT INTO deployments (user_id, bot_name, filename, status, auto_restart, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?)
    """, (uid, bot_name, filename, 'Uploaded', 1, now, now), commit=True)
    execute_db("UPDATE users SET total_bots_deployed = total_bots_deployed + 1 WHERE id=?", (uid,), commit=True)
    clear_session(uid)
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("📚 Install Libraries", callback_data="install_libs"),
        types.InlineKeyboardButton("🚀 Deploy Now", callback_data="deploy_new"),
        types.InlineKeyboardButton("🌍 Make Public", callback_data="make_public")
    )
    bot.reply_to(message, f"✅ Bot **{bot_name}** saved.\nYou can now install libraries or deploy.", reply_markup=markup)

# ---------- Callback Query Handler ----------
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    uid = call.from_user.id
    data = call.data
    try:
        if data == "my_bots":
            cmd_mybots(call.message)
        elif data == "upload":
            cmd_upload(call.message)
        elif data == "deploy_new":
            files = execute_db("SELECT id, bot_name, filename FROM deployments WHERE user_id=? AND (status='Uploaded' OR status='Stopped')", (uid,), fetchall=True)
            if not files:
                bot.answer_callback_query(call.id, "No bots available to deploy.")
                return
            markup = types.InlineKeyboardMarkup()
            for f in files:
                markup.add(types.InlineKeyboardButton(f"📁 {f['bot_name']}", callback_data=f"select_{f['id']}"))
            bot.edit_message_text("🚀 **Select bot to deploy:**", call.message.chat.id, call.message.message_id, reply_markup=markup)
        elif data.startswith("select_"):
            bot_id = int(data.split("_")[1])
            bot_info = execute_db("SELECT * FROM deployments WHERE id=? AND user_id=?", (bot_id, uid), fetchone=True)
            if not bot_info:
                bot.answer_callback_query(call.id, "Bot not found.")
                return
            success, msg = start_bot_process(bot_id, uid, bot_info['filename'])
            if success:
                bot.edit_message_text(f"✅ Bot **{bot_info['bot_name']}** deployed!", call.message.chat.id, call.message.message_id)
            else:
                bot.edit_message_text(f"❌ Deployment failed: {msg}", call.message.chat.id, call.message.message_id)
        elif data.startswith("bot_"):
            bot_id = int(data.split("_")[1])
            show_bot_details(call, bot_id)
        elif data.startswith("stop_"):
            bot_id = int(data.split("_")[1])
            stop_bot_logic(bot_id, uid)
            bot.answer_callback_query(call.id, "Bot stopped")
            show_bot_details(call, bot_id)
        elif data.startswith("restart_"):
            bot_id = int(data.split("_")[1])
            restart_bot_logic(bot_id, uid)
            bot.answer_callback_query(call.id, "Restarting...")
            show_bot_details(call, bot_id)
        elif data.startswith("delete_"):
            bot_id = int(data.split("_")[1])
            confirm = types.InlineKeyboardMarkup()
            confirm.add(
                types.InlineKeyboardButton("✅ Yes", callback_data=f"confirm_delete_{bot_id}"),
                types.InlineKeyboardButton("❌ No", callback_data=f"bot_{bot_id}")
            )
            bot.edit_message_text("⚠️ **Delete this bot permanently?**", call.message.chat.id, call.message.message_id, reply_markup=confirm)
        elif data.startswith("confirm_delete_"):
            bot_id = int(data.split("_")[2])
            delete_bot_logic(bot_id, uid)
            bot.edit_message_text("✅ Bot deleted.", call.message.chat.id, call.message.message_id)
            cmd_mybots(call.message)
        elif data.startswith("export_"):
            bot_id = int(data.split("_")[1])
            export_bot_logic(bot_id, uid, call.message.chat.id)
        elif data.startswith("logs_"):
            bot_id = int(data.split("_")[1])
            show_bot_logs_logic(bot_id, call.message.chat.id)
        elif data.startswith("autorestart_"):
            bot_id = int(data.split("_")[1])
            bot_info = execute_db("SELECT auto_restart FROM deployments WHERE id=?", (bot_id,), fetchone=True)
            new = 0 if bot_info['auto_restart'] == 1 else 1
            execute_db("UPDATE deployments SET auto_restart=? WHERE id=?", (new, bot_id), commit=True)
            bot.answer_callback_query(call.id, f"Auto‑restart {'enabled' if new else 'disabled'}")
            show_bot_details(call, bot_id)
        elif data.startswith("stats_"):
            bot_id = int(data.split("_")[1])
            show_bot_stats_logic(bot_id, call.message.chat.id)
        elif data.startswith("viewpublic_"):
            bot_id = int(data.split("_")[1])
            show_public_bot_details(call, bot_id)
        elif data == "install_libs":
            set_session(uid, {'state': 'waiting_for_libs'})
            bot.send_message(uid, "📚 **Send pip install commands** (one per line):")
        elif data == "make_public":
            set_session(uid, {'state': 'waiting_for_public_bot_id'})
            bot.send_message(uid, "Enter the **ID** of the bot you want to make public:")
        elif data == "admin_panel":
            if uid == Config.ADMIN_ID:
                bot.send_message(uid, "👑 **Admin Panel**", reply_markup=admin_keyboard())
        else:
            bot.answer_callback_query(call.id, "Unknown action")
    except Exception as e:
        logger.error(f"Callback error: {e}")
        bot.answer_callback_query(call.id, "⚠️ Error occurred")

# ---------- Bot Action Implementations ----------
def stop_bot_logic(bot_id, user_id):
    bot_info = execute_db("SELECT pid, node_id FROM deployments WHERE id=?", (bot_id,), fetchone=True)
    if bot_info and bot_info['pid']:
        try:
            os.kill(bot_info['pid'], signal.SIGTERM)
            time.sleep(1)
        except:
            pass
    now = datetime.now().isoformat()
    execute_db("UPDATE deployments SET status='Stopped', pid=0, last_active=?, updated_at=? WHERE id=?", (now, now, bot_id), commit=True)
    if bot_info and bot_info['node_id']:
        execute_db("UPDATE nodes SET current_load=current_load-1 WHERE id=?", (bot_info['node_id'],), commit=True)
    log_event("STOP", f"Bot {bot_id} stopped", user_id)

def restart_bot_logic(bot_id, user_id):
    stop_bot_logic(bot_id, user_id)
    time.sleep(2)
    bot_info = execute_db("SELECT filename, node_id FROM deployments WHERE id=?", (bot_id,), fetchone=True)
    if bot_info:
        start_bot_process(bot_id, user_id, bot_info['filename'], bot_info['node_id'])

def delete_bot_logic(bot_id, user_id):
    bot_info = execute_db("SELECT filename, pid, node_id FROM deployments WHERE id=?", (bot_id,), fetchone=True)
    if bot_info:
        if bot_info['pid']:
            try:
                os.kill(bot_info['pid'], signal.SIGTERM)
            except:
                pass
        file_path = Path(Config.PROJECT_DIR) / bot_info['filename']
        if file_path.exists():
            file_path.unlink()
        if bot_info['node_id']:
            execute_db("UPDATE nodes SET current_load=current_load-1 WHERE id=?", (bot_info['node_id'],), commit=True)
        execute_db("DELETE FROM deployments WHERE id=?", (bot_id,), commit=True)
        execute_db("DELETE FROM bot_logs WHERE bot_id=?", (bot_id,), commit=True)
        execute_db("UPDATE users SET total_bots_deployed = total_bots_deployed - 1 WHERE id=?", (user_id,), commit=True)
        log_event("DELETE", f"Bot {bot_id} deleted", user_id)

def export_bot_logic(bot_id, user_id, chat_id):
    bot_info = execute_db("SELECT bot_name, filename FROM deployments WHERE id=?", (bot_id,), fetchone=True)
    if not bot_info:
        bot.send_message(chat_id, "Bot not found.")
        return
    zip_path = create_bot_export(bot_id, bot_info['bot_name'], bot_info['filename'], user_id)
    if zip_path:
        with open(zip_path, 'rb') as f:
            bot.send_document(chat_id, f, caption=f"📦 **{bot_info['bot_name']}** export\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        zip_path.unlink()
    else:
        bot.send_message(chat_id, "❌ Export failed.")

def create_bot_export(bot_id, bot_name, filename, user_id):
    export_dir = Path(Config.EXPORTS_DIR) / f"export_{bot_id}_{int(time.time())}"
    export_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy2(Path(Config.PROJECT_DIR) / filename, export_dir / filename)
        log_src = Path(Config.LOGS_DIR) / f"bot_{bot_id}.log"
        if log_src.exists():
            shutil.copy2(log_src, export_dir / f"{bot_name}.log")
        meta = {
            'bot_id': bot_id,
            'bot_name': bot_name,
            'filename': filename,
            'user_id': user_id,
            'export_date': datetime.now().isoformat(),
            'version': 'ZEN X v4.1'
        }
        with open(export_dir / 'metadata.json', 'w') as f:
            json.dump(meta, f, indent=2)
        reqs = extract_requirements(Path(Config.PROJECT_DIR) / filename)
        if reqs:
            with open(export_dir / 'requirements.txt', 'w') as f:
                f.write("\n".join(reqs))
        zip_name = f"bot_{bot_name}_{bot_id}.zip"
        zip_path = Path(Config.EXPORTS_DIR) / zip_name
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(export_dir):
                for file in files:
                    full = os.path.join(root, file)
                    rel = os.path.relpath(full, export_dir)
                    zf.write(full, rel)
        return zip_path
    finally:
        shutil.rmtree(export_dir, ignore_errors=True)

def extract_requirements(py_file):
    reqs = set()
    try:
        with open(py_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('import ') or line.startswith('from '):
                    parts = line.split()
                    if len(parts) > 1:
                        mod = parts[1].split('.')[0]
                        if mod not in sys.stdlib_module_names and mod not in reqs:
                            reqs.add(mod)
    except:
        pass
    return list(reqs)

def show_bot_logs_logic(bot_id, chat_id):
    log_file = Path(Config.LOGS_DIR) / f"bot_{bot_id}.log"
    if not log_file.exists():
        bot.send_message(chat_id, "📜 No logs available.")
        return
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            logs = f.read()[-2000:]
        bot_name = execute_db("SELECT bot_name FROM deployments WHERE id=?", (bot_id,), fetchone=True)
        name = bot_name['bot_name'] if bot_name else f"Bot_{bot_id}"
        bot.send_message(chat_id, f"📜 **Logs for {name}**\n```\n{logs}\n```", parse_mode="Markdown")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Error reading logs: {e}")

def show_bot_stats_logic(bot_id, chat_id):
    bot_info = execute_db("SELECT * FROM deployments WHERE id=?", (bot_id,), fetchone=True)
    if not bot_info:
        bot.send_message(chat_id, "Bot not found.")
        return
    logs = execute_db("SELECT * FROM bot_logs WHERE bot_id=? ORDER BY id DESC LIMIT 10", (bot_id,), fetchall=True)
    text = f"""
📊 **Bot Statistics**
━━━━━━━━━━━━━━━━━━━━
🤖 **{bot_info['bot_name']}** (ID: {bot_id})
📁 File: `{bot_info['filename']}`
📊 Status: {bot_info['status']}
🔄 Restarts: {bot_info['restart_count']}
🔁 Auto‑restart: {'Yes' if bot_info['auto_restart'] else 'No'}
📅 Created: {bot_info['created_at']}
🕒 Last active: {bot_info['last_active']}
━━━━━━━━━━━━━━━━━━━━
📜 Recent events:
"""
    for l in logs:
        text += f"\n• {l['timestamp'][:19]} - {l['log_type']}: {l['message'][:50]}"
    bot.send_message(chat_id, text)

def show_bot_details(call, bot_id):
    bot_info = execute_db("SELECT * FROM deployments WHERE id=?", (bot_id,), fetchone=True)
    if not bot_info:
        bot.answer_callback_query(call.id, "Bot not found.")
        return
    stats = get_process_stats(bot_info['pid']) if bot_info['pid'] else None
    status = bot_info['status']
    if stats:
        status = "Running"
    cpu = stats['cpu'] if stats else 0
    mem = stats['memory'] if stats else 0
    uptime = stats['uptime'] if stats else "N/A"
    node = execute_db("SELECT name FROM nodes WHERE id=?", (bot_info['node_id'],), fetchone=True)
    node_name = node['name'] if node else "N/A"
    text = f"""
🤖 **Bot Details**
━━━━━━━━━━━━━━━━━━━━
**Name:** {bot_info['bot_name']}
**ID:** `{bot_id}`
**File:** `{bot_info['filename']}`
**Status:** {status}
**Node:** {node_name}
**CPU:** {cpu:.1f}%  **RAM:** {mem:.1f}%
**Uptime:** {uptime}
**Restarts:** {bot_info['restart_count']}
**Auto‑restart:** {'✅' if bot_info['auto_restart'] else '❌'}
**Public:** {'🌍 Yes' if bot_info['public_bot'] else '🔒 No'}
━━━━━━━━━━━━━━━━━━━━
"""
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                          reply_markup=bot_actions_keyboard(bot_id))

def show_public_bot_details(call, bot_id):
    bot_info = execute_db("""
        SELECT d.*, u.username 
        FROM deployments d
        LEFT JOIN users u ON d.user_id = u.id
        WHERE d.id=? AND d.public_bot=1
    """, (bot_id,), fetchone=True)
    if not bot_info:
        bot.answer_callback_query(call.id, "Bot not public or not found.")
        return
    execute_db("INSERT INTO bot_stats (bot_id, views, last_viewed) VALUES (?,1,?) ON CONFLICT(bot_id) DO UPDATE SET views=views+1, last_viewed=?",
               (bot_id, datetime.now().isoformat(), datetime.now().isoformat()), commit=True)
    text = f"""
🌍 **Public Bot**
━━━━━━━━━━━━━━━━━━━━
🤖 **{bot_info['bot_name']}**
👤 **Owner:** @{bot_info['username']}
📝 **Description:** {bot_info['description'] or 'No description'}
🏷️ **Tags:** {bot_info['tags'] or 'None'}
📅 **Created:** {bot_info['created_at']}
━━━━━━━━━━━━━━━━━━━━
"""
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📥 Export", callback_data=f"export_{bot_id}"))
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup)

def process_key(uid, key, chat_id):
    res = execute_db("SELECT * FROM keys WHERE key=?", (key,), fetchone=True)
    if not res or res['is_used'] == 1:
        bot.send_message(chat_id, "❌ Invalid or already used key.")
        return
    days, limit = res['duration_days'], res['file_limit']
    user = execute_db("SELECT expiry FROM users WHERE id=?", (uid,), fetchone=True)
    current = user['expiry']
    if current:
        try:
            new_expiry = max(datetime.now(), datetime.fromisoformat(current)) + timedelta(days=days)
        except:
            new_expiry = datetime.now() + timedelta(days=days)
    else:
        new_expiry = datetime.now() + timedelta(days=days)
    expiry_str = new_expiry.isoformat()
    now = datetime.now().isoformat()
    execute_db("UPDATE users SET expiry=?, file_limit=?, is_prime=1, last_renewal=?, last_active=? WHERE id=?",
               (expiry_str, limit, now, now, uid), commit=True)
    execute_db("UPDATE keys SET used_by=?, used_date=?, is_used=1 WHERE key=?", (uid, now, key), commit=True)
    bot.send_message(chat_id, f"✅ **Prime Activated!**\nExpires: {expiry_str}\nFile limit: {limit}")

# ---------- Admin Message Handlers ----------
@bot.message_handler(func=lambda m: m.from_user.id == Config.ADMIN_ID and m.text in admin_keyboard_buttons())
def admin_text_handler(message):
    uid = message.from_user.id
    text = message.text
    if text == "🎫 Generate Key":
        set_session(uid, {'state': 'admin_gen_days'})
        bot.reply_to(message, "Enter duration in days:")
    elif text == "👥 All Users":
        show_all_users_admin(message)
    elif text == "🤖 All Bots":
        show_all_bots_admin(message)
    elif text == "📈 Statistics":
        show_admin_stats(message)
    elif text == "🗄️ Database":
        view_database_admin(message)
    elif text == "💾 Backup":
        cmd_backup(message)
    elif text == "⚙️ Maintenance":
        toggle_maintenance_admin(message)
    elif text == "🌐 Nodes":
        show_nodes_admin(message)
    elif text == "🔧 Logs":
        show_server_logs_admin(message)
    elif text == "📊 System":
        show_system_info_admin(message)
    elif text == "🔔 Broadcast":
        set_session(uid, {'state': 'admin_broadcast'})
        bot.reply_to(message, "Enter broadcast message:")
    elif text == "🔄 Cleanup":
        cleanup_system_admin(message)
    elif text == "🏠 Main Menu":
        cmd_start(message)

def admin_keyboard_buttons():
    return ["🎫 Generate Key", "👥 All Users", "🤖 All Bots", "📈 Statistics",
            "🗄️ Database", "💾 Backup", "⚙️ Maintenance", "🌐 Nodes",
            "🔧 Logs", "📊 System", "🔔 Broadcast", "🔄 Cleanup", "🏠 Main Menu"]

def show_all_users_admin(message):
    users = execute_db("SELECT id, username, is_prime, expiry, total_bots_deployed, join_date FROM users ORDER BY id DESC LIMIT 20", fetchall=True)
    text = "👥 **Recent Users**\n━━━━━━━━━━━━━━━━━━━━\n"
    for u in users:
        prime = "👑" if u['is_prime'] else "🆓"
        text += f"{prime} **{u['username'] or u['id']}** (ID: {u['id']})\nBots: {u['total_bots_deployed']} | Exp: {u['expiry'][:10] if u['expiry'] else 'None'}\n━━━━━━━━━━━━━━━━━━━━\n"
    bot.send_message(message.chat.id, text)

def show_all_bots_admin(message):
    bots = execute_db("SELECT d.id, d.bot_name, d.status, u.username, d.auto_restart FROM deployments d LEFT JOIN users u ON d.user_id=u.id ORDER BY d.id DESC LIMIT 20", fetchall=True)
    text = "🤖 **All Bots**\n━━━━━━━━━━━━━━━━━━━━\n"
    for b in bots:
        icon = "🟢" if b['status'] == 'Running' else "🔴"
        auto = "🔁" if b['auto_restart'] else "⏸️"
        text += f"{icon}{auto} **{b['bot_name']}** (ID: {b['id']})\n👤 @{b['username'] or 'Unknown'}\n━━━━━━━━━━━━━━━━━━━━\n"
    bot.send_message(message.chat.id, text)

def show_admin_stats(message):
    stats = get_system_stats()
    text = f"""
📈 **Admin Statistics**
━━━━━━━━━━━━━━━━━━━━
👥 Users: {stats['total_users']} (👑 {stats['prime_users']})
🤖 Bots: {stats['total_bots']} (🟢 {stats['running_bots']})
📦 Deployed today: {stats['deployed_today']}
💾 Backups: {stats['backup_count']} (last: {stats['last_backup'][:19]})
━━━━━━━━━━━━━━━━━━━━
🖥️ System:
CPU: {create_progress_bar(stats['cpu_percent'])} {stats['cpu_percent']:.1f}%
RAM: {create_progress_bar(stats['ram_percent'])} {stats['ram_percent']:.1f}%
Disk: {create_progress_bar(stats['disk_percent'])} {stats['disk_percent']:.1f}%
Capacity: {stats['used_capacity']}/{stats['total_capacity']}
Crash rate: {stats['crash_rate']:.1f}/min
━━━━━━━━━━━━━━━━━━━━
    """
    bot.send_message(message.chat.id, text)

def view_database_admin(message):
    page = 1
    per_page = 5
    total = execute_db("SELECT COUNT(*) FROM deployments", fetchone=True)[0]
    pages = (total + per_page - 1) // per_page
    deployments = execute_db("""
        SELECT d.id, d.bot_name, d.status, u.username, d.created_at
        FROM deployments d
        LEFT JOIN users u ON d.user_id = u.id
        ORDER BY d.id DESC
        LIMIT ? OFFSET ?
    """, (per_page, (page-1)*per_page), fetchall=True)
    text = f"🗄️ **Database** (Page {page}/{pages})\n━━━━━━━━━━━━━━━━━━━━\n"
    for d in deployments:
        text += f"ID: {d['id']} | {d['bot_name']} | {d['status']} | @{d['username']}\n{d['created_at'][:19]}\n━━━━━━━━━━━━━━━━━━━━\n"
    markup = types.InlineKeyboardMarkup()
    if page < pages:
        markup.add(types.InlineKeyboardButton("Next ➡️", callback_data="db_page_2"))
    bot.send_message(message.chat.id, text, reply_markup=markup)

def toggle_maintenance_admin(message):
    Config.MAINTENANCE = not Config.MAINTENANCE
    status = "ENABLED" if Config.MAINTENANCE else "DISABLED"
    bot.reply_to(message, f"⚙️ Maintenance mode {status}.")
    log_event("MAINTENANCE", f"Mode {status}", Config.ADMIN_ID)

def show_nodes_admin(message):
    nodes = execute_db("SELECT * FROM nodes", fetchall=True)
    text = "🌐 **Node Status**\n━━━━━━━━━━━━━━━━━━━━\n"
    for n in nodes:
        load = (n['current_load'] / n['capacity']) * 100 if n['capacity'] > 0 else 0
        bar = create_progress_bar(load)
        status = "🟢" if n['status'] == 'active' else "🔴"
        text += f"{status} **{n['name']}** ({n['region']})\nLoad: {bar} {load:.1f}% ({n['current_load']}/{n['capacity']})\nDeployed: {n['total_deployed']}\n━━━━━━━━━━━━━━━━━━━━\n"
    bot.send_message(message.chat.id, text)

def show_server_logs_admin(message):
    logs = execute_db("SELECT * FROM server_logs ORDER BY id DESC LIMIT 15", fetchall=True)
    text = "🔧 **Server Logs**\n━━━━━━━━━━━━━━━━━━━━\n"
    for l in logs:
        text += f"{l['timestamp'][:19]} - {l['event']}\n`{l['details'][:100]}`\n━━━━━━━━━━━━━━━━━━━━\n"
    bot.send_message(message.chat.id, text)

def show_system_info_admin(message):
    stats = get_system_stats()
    text = f"""
📊 **System Information**
━━━━━━━━━━━━━━━━━━━━
🖥️ Platform: {stats['platform']} / Python {stats['python_version']}
⏱️ Uptime: {stats['uptime_days']} days
📦 Bot version: 4.1
━━━━━━━━━━━━━━━━━━━━
💾 Storage:
• Projects: {sum(f.stat().st_size for f in Path(Config.PROJECT_DIR).glob('*')) / 1024 / 1024:.1f} MB
• Logs: {sum(f.stat().st_size for f in Path(Config.LOGS_DIR).glob('*')) / 1024 / 1024:.1f} MB
• Backups: {stats['backup_count']} files
━━━━━━━━━━━━━━━━━━━━
⚙️ Configuration:
• Max bots/user: {Config.MAX_BOTS_PER_USER}
• Max concurrent: {Config.MAX_CONCURRENT_DEPLOYMENTS}
• Auto‑restart: {Config.AUTO_RESTART_BOTS}
• Auto‑backup thresholds: {Config.AUTO_BACKUP_THRESHOLDS}
━━━━━━━━━━━━━━━━━━━━
    """
    bot.send_message(message.chat.id, text)

def cleanup_system_admin(message):
    count = 0
    for f in Path(Config.EXPORTS_DIR).glob("*.zip"):
        if (datetime.now() - datetime.fromtimestamp(f.stat().st_mtime)).days > 1:
            f.unlink()
            count += 1
    for f in Path(Config.TEMP_DIR).glob("*"):
        if f.is_dir():
            shutil.rmtree(f, ignore_errors=True)
        else:
            f.unlink()
    for f in Path(Config.LOGS_DIR).glob("*.log"):
        if (datetime.now() - datetime.fromtimestamp(f.stat().st_mtime)).days > 14:
            f.unlink()
    bot.reply_to(message, f"✅ Cleanup completed. Removed {count} export files.")

# ---------- Background Threads ----------
def start_background_threads():
    threading.Thread(target=health_check_thread, daemon=True).start()
    threading.Thread(target=auto_backup_scheduler, daemon=True).start()
    threading.Thread(target=cleanup_old_sessions, daemon=True).start()
    logger.info("Background threads started.")

def auto_backup_scheduler():
    while True:
        time.sleep(Config.BACKUP_INTERVAL)
        zip_path = full_system_backup(reason="scheduled")
        if zip_path:
            logger.info(f"Scheduled backup created: {zip_path.name}")
            backups = sorted(Path(Config.BACKUP_DIR).glob("full_backup_*.zip"), key=lambda f: f.stat().st_mtime)
            while len(backups) > 7:
                backups[0].unlink()
                backups.pop(0)

def cleanup_old_sessions():
    while True:
        time.sleep(600)
        now = time.time()
        to_delete = [uid for uid, sess in user_sessions.items() if sess.get('expires', 0) < now]
        for uid in to_delete:
            del user_sessions[uid]

# ---------- Flask Web Endpoints ----------
@app.route('/')
def home():
    return "ZEN X HOST BOT v4.1 - Online"

@app.route('/health')
def health():
    stats = get_system_stats()
    return jsonify({'status': 'ok', 'stats': stats})

# ---------- Main ----------
if __name__ == "__main__":
    logger.info("Starting ZEN X HOST BOT v4.1...")
    init_db()
    recover_deployments()
    start_background_threads()
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=Config.PORT, debug=False, use_reloader=False), daemon=True).start()
    try:
        bot.send_message(Config.ADMIN_ID, f"🚀 **ZEN X HOST BOT v4.1 started**\n{datetime.now().isoformat()}")
    except:
        pass
    while True:
        try:
            bot.polling(none_stop=True, timeout=60)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(10)
