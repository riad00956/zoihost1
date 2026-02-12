
import asyncio
import json
import logging
import os
import platform
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Any

import psutil
import telebot
from flask import Flask, jsonify, request
from telebot import types
from werkzeug.utils import secure_filename

# =============================================================================
#  CONFIGURATION – ALL SETTINGS IN ONE PLACE
# =============================================================================

class Config:
    """
    Central configuration for the entire bot.
    All values can be overridden via environment variables.
    """
    
    # ---------- Telegram Bot Credentials ----------
    TOKEN: str = os.environ.get('BOT_TOKEN', '8494225623:AAFOFfM2kZFyfW04SkNfGtRs_cK-1bHuEC8')
    ADMIN_ID: int = int(os.environ.get('ADMIN_ID', '7832264582'))
    ADMIN_USERNAME: str = os.environ.get('ADMIN_USERNAME', 'zerox6t9')
    BOT_USERNAME: str = os.environ.get('BOT_USERNAME', 'zen_xbot')
    
    # ---------- Directory Structure ----------
    PROJECT_DIR: str = 'projects'
    DB_NAME: str = 'zenx_host.db'
    BACKUP_DIR: str = 'backups'
    LOGS_DIR: str = 'logs'
    EXPORTS_DIR: str = 'exports'
    TEMP_DIR: str = 'temp'
    
    # ---------- Server Settings ----------
    PORT: int = int(os.environ.get('PORT', 10000))
    MAINTENANCE: bool = False
    AUTO_RESTART_BOTS: bool = True
    
    # ---------- User & Bot Limits ----------
    MAX_BOTS_PER_USER: int = 10
    MAX_CONCURRENT_DEPLOYMENTS: int = 5
    MAX_FILE_SIZE_MB: float = 5.5
    BOT_TIMEOUT: int = 300
    MAX_LOG_LINES: int = 5000
    
    # ---------- Backup & Health ----------
    BACKUP_INTERVAL: int = 3600  # seconds (1 hour)
    AUTO_BACKUP_THRESHOLDS: Dict[str, float] = {
        'cpu_percent': 80.0,
        'ram_percent': 85.0,
        'disk_percent': 90.0,
        'crash_rate': 5.0       # crashes per minute
    }
    
    # ---------- Hosting Nodes (300 capacity each, fully adjustable) ----------
    HOSTING_NODES: List[Dict[str, Any]] = [
        {"name": "Node-1", "region": "Asia", "capacity": 300},
        {"name": "Node-2", "region": "Asia", "capacity": 300},
        {"name": "Node-3", "region": "Europe", "capacity": 300}
    ]
    
    # ---------- Rate Limiting (actions / period in seconds) ----------
    RATE_LIMIT: Dict[str, Tuple[int, int]] = {
        'deploy': (2, 60),
        'restart': (3, 60),
        'export': (5, 60),
        'delete': (5, 60),
        'broadcast': (1, 3600)
    }
    
    # ---------- Session Timeout ----------
    SESSION_TIMEOUT: int = 600  # seconds (10 minutes)
    
    # ---------- Message Cleanup ----------
    MAX_MESSAGE_HISTORY: int = 10
    AUTO_DELETE_DELAY: int = 5  # seconds for temporary notifications
    
    # ---------- Public Bot Store ----------
    PUBLIC_BOT_PAGE_SIZE: int = 5
    MAX_DESCRIPTION_LENGTH: int = 200
    MAX_TAGS_LENGTH: int = 100
    
    # ---------- Circuit Breaker ----------
    MAX_CRASHES_BEFORE_DISABLE: int = 3
    CRASH_WINDOW_SECONDS: int = 600  # 10 minutes


# =============================================================================
#  ADVANCED LOGGING SETUP
# =============================================================================

def setup_logging() -> logging.Logger:
    """Configure comprehensive logging to file and console."""
    log_dir = Path(Config.LOGS_DIR)
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / 'zenx_host.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Suppress overly verbose libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('telebot').setLevel(logging.WARNING)
    
    logger = logging.getLogger('ZenXHost')
    logger.info('=' * 60)
    logger.info(' ZEN X HOST BOT v5.0 – STARTING UP')
    logger.info('=' * 60)
    return logger


LOGGER = setup_logging()


# =============================================================================
#  GLOBAL OBJECTS & STATE
# =============================================================================

# Telegram Bot instance
bot = telebot.TeleBot(Config.TOKEN, parse_mode='Markdown')

# Flask web server for health checks
app = Flask(__name__)

# Thread pool for background tasks
executor = ThreadPoolExecutor(max_workers=10)

# Thread‑safe database lock
db_lock = threading.RLock()

# User session storage (state, expires)
user_sessions: Dict[int, Dict[str, Any]] = {}

# Message history for editing (avoid "message not modified")
user_message_history: Dict[int, List[int]] = {}

# Bot process monitor threads
bot_monitors: Dict[int, threading.Thread] = {}

# Rate limiter: {user_id:action: [timestamps]}
rate_limiter: Dict[str, List[float]] = defaultdict(list)

# System health tracking
system_health: Dict[str, Any] = {
    'last_emergency_backup': None,
    'crash_counter': defaultdict(int),
    'crash_timestamps': defaultdict(list),
    'bot_start_time': datetime.now()
}

# Create all required directories
for directory in [
    Config.PROJECT_DIR,
    Config.BACKUP_DIR,
    Config.LOGS_DIR,
    Config.EXPORTS_DIR,
    Config.TEMP_DIR
]:
    Path(directory).mkdir(parents=True, exist_ok=True)
    LOGGER.info(f'Directory ensured: {directory}')


# =============================================================================
#  DATABASE CORE – THREAD‑SAFE, AUTO‑RECOVERY
# =============================================================================

def get_db_connection() -> sqlite3.Connection:
    """Return a thread‑safe SQLite connection with row factory."""
    with db_lock:
        conn = sqlite3.connect(Config.DB_NAME, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn


def execute_query(
    query: str,
    params: tuple = (),
    fetchone: bool = False,
    fetchall: bool = False,
    commit: bool = False
) -> Optional[Union[sqlite3.Row, List[sqlite3.Row], Any]]:
    """
    Execute a database query with automatic connection management.
    Returns None on failure.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        if commit:
            conn.commit()
        
        if fetchone:
            return cursor.fetchone()
        elif fetchall:
            return cursor.fetchall()
        else:
            return None
            
    except sqlite3.Error as e:
        LOGGER.error(f'Database error: {e} | Query: {query[:100]}...')
        return None
    finally:
        if conn:
            conn.close()


def init_database() -> None:
    """
    Create all tables and insert default records.
    This function is idempotent and safe to run multiple times.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # ---------- Users table ----------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                language_code TEXT DEFAULT 'en',
                expiry TEXT,
                file_limit INTEGER DEFAULT 1,
                is_prime INTEGER DEFAULT 0,
                join_date TEXT,
                last_renewal TEXT,
                total_bots_deployed INTEGER DEFAULT 0,
                total_deployments INTEGER DEFAULT 0,
                last_active TEXT,
                banned INTEGER DEFAULT 0,
                notification_settings TEXT DEFAULT '{"broadcast":true,"bot_status":true}'
            )
        ''')
        
        # ---------- Prime keys table ----------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS keys (
                key TEXT PRIMARY KEY,
                duration_days INTEGER,
                file_limit INTEGER,
                created_date TEXT,
                created_by INTEGER,
                used_by INTEGER,
                used_date TEXT,
                is_used INTEGER DEFAULT 0
            )
        ''')
        
        # ---------- Deployments (bots) table ----------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS deployments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                bot_name TEXT NOT NULL,
                filename TEXT NOT NULL,
                pid INTEGER,
                start_time TEXT,
                status TEXT DEFAULT 'Uploaded',
                cpu_usage REAL DEFAULT 0,
                ram_usage REAL DEFAULT 0,
                last_active TEXT,
                node_id INTEGER,
                logs TEXT,
                restart_count INTEGER DEFAULT 0,
                auto_restart INTEGER DEFAULT 1,
                public_bot INTEGER DEFAULT 0,
                description TEXT DEFAULT '',
                tags TEXT DEFAULT '',
                created_at TEXT,
                updated_at TEXT,
                last_start_attempt TEXT,
                error_log TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(node_id) REFERENCES nodes(id)
            )
        ''')
        
        # ---------- Hosting nodes table ----------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'active',
                capacity INTEGER DEFAULT 300,
                current_load INTEGER DEFAULT 0,
                region TEXT DEFAULT 'Global',
                total_deployed INTEGER DEFAULT 0,
                last_check TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        ''')
        
        # ---------- Server logs table ----------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS server_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                event TEXT,
                details TEXT,
                user_id INTEGER,
                severity TEXT DEFAULT 'INFO'
            )
        ''')
        
        # ---------- Bot logs table ----------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id INTEGER,
                timestamp TEXT,
                log_type TEXT,
                message TEXT,
                FOREIGN KEY(bot_id) REFERENCES deployments(id) ON DELETE CASCADE
            )
        ''')
        
        # ---------- Notifications table ----------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                is_read INTEGER DEFAULT 0,
                created_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        ''')
        
        # ---------- Bot statistics table (views/downloads) ----------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_stats (
                bot_id INTEGER PRIMARY KEY,
                views INTEGER DEFAULT 0,
                downloads INTEGER DEFAULT 0,
                last_viewed TEXT,
                FOREIGN KEY(bot_id) REFERENCES deployments(id) ON DELETE CASCADE
            )
        ''')
        
        # ---------- Message tracking for deletion feature ----------
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                bot_message_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                command TEXT,
                created_at TEXT
            )
        ''')
        
        conn.commit()
    
    # ---------- Insert default nodes if not exist ----------
    for node in Config.HOSTING_NODES:
        exists = execute_query(
            'SELECT id FROM nodes WHERE name = ?',
            (node['name'],),
            fetchone=True
        )
        if not exists:
            now = datetime.now().isoformat()
            execute_query(
                '''
                INSERT INTO nodes 
                (name, status, capacity, region, created_at, updated_at, last_check)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''',
                (node['name'], 'active', node['capacity'], node['region'], now, now, now),
                commit=True
            )
            LOGGER.info(f'Default node created: {node["name"]}')
    
    # ---------- Ensure admin user exists ----------
    admin = execute_query(
        'SELECT id FROM users WHERE id = ?',
        (Config.ADMIN_ID,),
        fetchone=True
    )
    if not admin:
        now = datetime.now().isoformat()
        expiry = (datetime.now() + timedelta(days=3650)).isoformat()
        execute_query(
            '''
            INSERT INTO users 
            (id, username, expiry, file_limit, is_prime, join_date, last_active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (Config.ADMIN_ID, Config.ADMIN_USERNAME, expiry, 999, 1, now, now),
            commit=True
        )
        LOGGER.info(f'Admin user created: {Config.ADMIN_ID}')
    
    LOGGER.info('Database initialization completed.')


# =============================================================================
#  UTILITY FUNCTIONS – REUSABLE, CLEAN, TESTED
# =============================================================================

def safe_int(value: Any, default: int = 0) -> int:
    """Convert value to int safely; return default on failure."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """Convert value to float safely."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def format_size(bytes: int) -> str:
    """Convert bytes to human‑readable string."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes < 1024:
            return f'{bytes:.1f} {unit}'
        bytes /= 1024
    return f'{bytes:.1f} TB'


def format_uptime(seconds: float) -> str:
    """Convert seconds to human‑readable uptime string."""
    days, remainder = divmod(int(seconds), 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    parts = []
    if days > 0:
        parts.append(f'{days}d')
    if hours > 0:
        parts.append(f'{hours}h')
    if minutes > 0:
        parts.append(f'{minutes}m')
    if seconds > 0 or not parts:
        parts.append(f'{seconds}s')
    
    return ' '.join(parts)


def create_progress_bar(percent: float, width: int = 10) -> str:
    """Generate a visual progress bar."""
    filled = int(min(percent, 100) * width / 100)
    return '█' * filled + '░' * (width - filled)


def generate_key() -> str:
    """Generate a unique prime activation key."""
    import secrets
    prefix = 'ZENX-'
    random_part = secrets.token_hex(6).upper()
    return f'{prefix}{random_part}'


def is_valid_key_format(key: str) -> bool:
    """Validate prime key format."""
    return bool(re.match(r'^ZENX-[A-F0-9]{12}$', key))


def get_client_ip() -> str:
    """Retrieve client IP from Flask request context."""
    if request.headers.get('X-Forwarded-For'):
        return request.headers.get('X-Forwarded-For').split(',')[0]
    return request.remote_addr or '0.0.0.0'


def escape_markdown(text: str) -> str:
    """Escape Telegram MarkdownV2 special characters."""
    special_chars = '_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{c}' if c in special_chars else c for c in text)


def truncate(text: str, max_length: int = 100) -> str:
    """Truncate text with ellipsis."""
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + '...'


def sanitize_filename(filename: str) -> str:
    """Ensure filename is safe for filesystem."""
    return secure_filename(filename)


def is_python_file(filename: str) -> bool:
    """Check if file is a Python script."""
    return filename.lower().endswith('.py')


def is_zip_file(filename: str) -> bool:
    """Check if file is a ZIP archive."""
    return filename.lower().endswith('.zip')


def extract_zip_and_get_main_py(zip_path: Path, extract_to: Path) -> Optional[Path]:
    """
    Extract a ZIP file and return the path to the first .py file found.
    Returns None if no Python file is found.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_to)
        
        py_files = list(extract_to.rglob('*.py'))
        if not py_files:
            return None
        
        # Heuristic: prefer a file named main.py or bot.py
        for name in ['main.py', 'bot.py', 'app.py']:
            for py in py_files:
                if py.name == name:
                    return py
        
        # Otherwise return the first .py file
        return py_files[0]
    except Exception as e:
        LOGGER.error(f'ZIP extraction failed: {e}')
        return None


def extract_requirements(py_file: Path) -> List[str]:
    """
    Simple scanner to detect imported non‑stdlib modules.
    Returns a list of likely required packages.
    """
    imports = set()
    try:
        with open(py_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('import ') or line.startswith('from '):
                    parts = line.split()
                    if len(parts) > 1:
                        module = parts[1].split('.')[0]
                        if module not in sys.stdlib_module_names and module not in imports:
                            imports.add(module)
    except Exception as e:
        LOGGER.warning(f'Requirements extraction failed: {e}')
    
    return sorted(imports)


def delete_messages(chat_id: int, message_ids: List[int]) -> None:
    """Attempt to delete multiple messages, ignoring failures."""
    for msg_id in message_ids:
        try:
            bot.delete_message(chat_id, msg_id)
            time.sleep(0.2)  # avoid hitting rate limits
        except Exception as e:
            LOGGER.debug(f'Could not delete message {msg_id}: {e}')


def store_bot_message(chat_id: int, user_id: int, bot_message_id: int, command: str = '') -> None:
    """Store a bot message for later deletion via reply."""
    now = datetime.now().isoformat()
    execute_query(
        '''
        INSERT INTO bot_messages (chat_id, message_id, bot_message_id, user_id, command, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (chat_id, 0, bot_message_id, user_id, command, now),
        commit=True
    )


def get_bot_message_by_reply(reply_to_message: types.Message) -> Optional[Dict[str, Any]]:
    """Retrieve stored bot message that the user replied to."""
    if not reply_to_message:
        return None
    
    result = execute_query(
        '''
        SELECT * FROM bot_messages
        WHERE chat_id = ? AND bot_message_id = ?
        ORDER BY id DESC LIMIT 1
        ''',
        (reply_to_message.chat.id, reply_to_message.message_id),
        fetchone=True
    )
    return dict(result) if result else None


# =============================================================================
#  RATE LIMITING DECORATOR
# =============================================================================

def rate_limit(action: str):
    """Decorator to enforce rate limits on commands."""
    def decorator(func):
        @wraps(func)
        def wrapper(message: types.Message, *args, **kwargs):
            user_id = message.from_user.id
            if user_id == Config.ADMIN_ID:
                return func(message, *args, **kwargs)
            
            limit, period = Config.RATE_LIMIT.get(action, (5, 60))
            key = f'{user_id}:{action}'
            now = time.time()
            
            # Clean old timestamps
            timestamps = rate_limiter[key]
            timestamps = [t for t in timestamps if now - t < period]
            rate_limiter[key] = timestamps
            
            if len(timestamps) >= limit:
                bot.reply_to(
                    message,
                    f'⏳ **Rate limit exceeded.**\nYou can use this command '
                    f'{limit} times per {period} seconds.\nPlease wait.'
                )
                return
            
            timestamps.append(now)
            return func(message, *args, **kwargs)
        return wrapper
    return decorator


# =============================================================================
#  USER & PRIME MANAGEMENT
# =============================================================================

def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    """Fetch user record from database."""
    row = execute_query(
        'SELECT * FROM users WHERE id = ?',
        (user_id,),
        fetchone=True
    )
    return dict(row) if row else None


def create_user(user_id: int, username: str = '', first_name: str = '') -> None:
    """Insert a new user if not already exists."""
    user = get_user(user_id)
    if not user:
        now = datetime.now().isoformat()
        execute_query(
            '''
            INSERT INTO users (id, username, first_name, join_date, last_active)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (user_id, username[:32], first_name[:64], now, now),
            commit=True
        )
        LOGGER.info(f'New user created: {user_id} (@{username})')


def update_user_activity(user_id: int) -> None:
    """Update last_active timestamp for user."""
    now = datetime.now().isoformat()
    execute_query(
        'UPDATE users SET last_active = ? WHERE id = ?',
        (now, user_id),
        commit=True
    )


def check_prime_status(user_id: int) -> Dict[str, Any]:
    """
    Return detailed prime subscription status.
    If expired or not found, returns {'expired': True, ...}
    """
    user = get_user(user_id)
    if not user or not user['expiry']:
        return {
            'expired': True,
            'message': 'No Prime subscription found.',
            'days_left': 0,
            'hours_left': 0
        }
    
    try:
        expiry = datetime.fromisoformat(user['expiry'])
        now = datetime.now()
        
        if expiry > now:
            delta = expiry - now
            return {
                'expired': False,
                'expiry_date': expiry.isoformat(),
                'days_left': delta.days,
                'hours_left': delta.seconds // 3600,
                'file_limit': user['file_limit'],
                'is_prime': user['is_prime']
            }
        else:
            delta = now - expiry
            return {
                'expired': True,
                'expiry_date': expiry.isoformat(),
                'days_expired': delta.days,
                'message': f'Prime expired {delta.days} day(s) ago.',
                'file_limit': 1,
                'is_prime': 0
            }
    except Exception as e:
        LOGGER.error(f'Prime check error: {e}')
        return {
            'expired': True,
            'message': 'Invalid expiry date format.',
            'file_limit': 1,
            'is_prime': 0
        }


def activate_prime(user_id: int, key: str) -> Tuple[bool, str]:
    """
    Activate a prime key for a user.
    Returns (success, message).
    """
    key_data = execute_query(
        'SELECT * FROM keys WHERE key = ? AND is_used = 0',
        (key,),
        fetchone=True
    )
    
    if not key_data:
        return False, '❌ Invalid or already used key.'
    
    days = key_data['duration_days']
    file_limit = key_data['file_limit']
    
    user = get_user(user_id)
    current_expiry = None
    if user and user['expiry']:
        try:
            current_expiry = datetime.fromisoformat(user['expiry'])
        except:
            pass
    
    now = datetime.now()
    if current_expiry and current_expiry > now:
        new_expiry = current_expiry + timedelta(days=days)
    else:
        new_expiry = now + timedelta(days=days)
    
    expiry_str = new_expiry.isoformat()
    now_str = now.isoformat()
    
    # Update user
    execute_query(
        '''
        UPDATE users 
        SET expiry = ?, file_limit = ?, is_prime = 1, last_renewal = ?, last_active = ?
        WHERE id = ?
        ''',
        (expiry_str, file_limit, now_str, now_str, user_id),
        commit=True
    )
    
    # Mark key as used
    execute_query(
        'UPDATE keys SET used_by = ?, used_date = ?, is_used = 1 WHERE key = ?',
        (user_id, now_str, key),
        commit=True
    )
    
    LOGGER.info(f'Prime activated for user {user_id} with key {key}')
    return True, f'✅ **Prime Activated!**\nExpires: {expiry_str[:19]}\nFile limit: {file_limit}'


def generate_prime_key(days: int, file_limit: int, created_by: int) -> str:
    """Generate a new prime key and store in database."""
    key = generate_key()
    now = datetime.now().isoformat()
    execute_query(
        '''
        INSERT INTO keys (key, duration_days, file_limit, created_date, created_by)
        VALUES (?, ?, ?, ?, ?)
        ''',
        (key, days, file_limit, now, created_by),
        commit=True
    )
    LOGGER.info(f'Prime key generated by admin {created_by}: {key}')
    return key


# =============================================================================
#  NODE MANAGEMENT (ADMIN)
# =============================================================================

def get_all_nodes() -> List[Dict[str, Any]]:
    """Retrieve all hosting nodes."""
    rows = execute_query(
        'SELECT * FROM nodes ORDER BY id ASC',
        fetchall=True
    ) or []
    return [dict(row) for row in rows]


def get_node(node_id: int) -> Optional[Dict[str, Any]]:
    """Retrieve a specific node by ID."""
    row = execute_query(
        'SELECT * FROM nodes WHERE id = ?',
        (node_id,),
        fetchone=True
    )
    return dict(row) if row else None


def update_node_load(node_id: int, delta: int = 1) -> None:
    """Increase or decrease the current load of a node."""
    node = get_node(node_id)
    if node:
        new_load = max(0, node['current_load'] + delta)
        now = datetime.now().isoformat()
        execute_query(
            'UPDATE nodes SET current_load = ?, updated_at = ? WHERE id = ?',
            (new_load, now, node_id),
            commit=True
        )


def assign_bot_to_node() -> Optional[Dict[str, Any]]:
    """
    Select the node with the lowest relative load.
    Returns None if no active nodes.
    """
    nodes = get_all_nodes()
    active_nodes = [n for n in nodes if n['status'] == 'active']
    if not active_nodes:
        return None
    
    # Choose node with smallest (current_load / capacity)
    def load_ratio(node):
        cap = node['capacity'] or 1
        return node['current_load'] / cap
    
    return min(active_nodes, key=load_ratio)


def add_node(name: str, region: str, capacity: int) -> bool:
    """Add a new hosting node (admin only)."""
    now = datetime.now().isoformat()
    try:
        execute_query(
            '''
            INSERT INTO nodes (name, region, capacity, status, created_at, updated_at, last_check)
            VALUES (?, ?, ?, 'active', ?, ?, ?)
            ''',
            (name, region, capacity, now, now, now),
            commit=True
        )
        LOGGER.info(f'New node added: {name} (capacity {capacity})')
        return True
    except sqlite3.IntegrityError:
        return False  # name already exists


def remove_node(node_id: int) -> bool:
    """Remove a node (admin only)."""
    # Check if node has any running bots
    node = get_node(node_id)
    if node and node['current_load'] > 0:
        return False  # cannot remove node with active bots
    
    execute_query(
        'DELETE FROM nodes WHERE id = ?',
        (node_id,),
        commit=True
    )
    LOGGER.info(f'Node {node_id} removed')
    return True


def set_node_capacity(node_id: int, new_capacity: int) -> bool:
    """Update node capacity (admin only)."""
    node = get_node(node_id)
    if not node:
        return False
    
    if new_capacity < node['current_load']:
        return False  # capacity cannot be less than current load
    
    now = datetime.now().isoformat()
    execute_query(
        'UPDATE nodes SET capacity = ?, updated_at = ? WHERE id = ?',
        (new_capacity, now, node_id),
        commit=True
    )
    LOGGER.info(f'Node {node_id} capacity updated to {new_capacity}')
    return True


def set_node_status(node_id: int, status: str) -> bool:
    """Activate or deactivate a node."""
    if status not in ('active', 'maintenance', 'offline'):
        return False
    
    now = datetime.now().isoformat()
    execute_query(
        'UPDATE nodes SET status = ?, updated_at = ? WHERE id = ?',
        (status, now, node_id),
        commit=True
    )
    LOGGER.info(f'Node {node_id} status set to {status}')
    return True


# =============================================================================
#  BOT DEPLOYMENT & PROCESS MANAGEMENT
# =============================================================================

def get_process_stats(pid: int) -> Optional[Dict[str, Any]]:
    """
    Retrieve CPU%, memory%, and uptime for a given PID.
    Returns None if process does not exist.
    """
    if not pid or pid <= 0:
        return None
    
    try:
        proc = psutil.Process(pid)
        with proc.oneshot():
            cpu = proc.cpu_percent(interval=0.1)
            mem = proc.memory_percent()
            create_time = proc.create_time()
            uptime = time.time() - create_time
            return {
                'pid': pid,
                'cpu': cpu,
                'memory': mem,
                'uptime': format_uptime(uptime),
                'status': 'running',
                'create_time': datetime.fromtimestamp(create_time).isoformat()
            }
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None
    except Exception as e:
        LOGGER.debug(f'Process stats error for PID {pid}: {e}')
        return None


def start_bot_process(
    bot_id: int,
    user_id: int,
    filename: str,
    node_id: Optional[int] = None
) -> Tuple[bool, str]:
    """
    Launch a bot process, update database, and start monitoring.
    Returns (success, message).
    """
    file_path = Path(Config.PROJECT_DIR) / filename
    if not file_path.exists():
        return False, 'Bot file not found on server.'
    
    # Assign node if not provided
    if not node_id:
        node = assign_bot_to_node()
        if not node:
            return False, 'No available hosting nodes.'
        node_id = node['id']
    else:
        node = get_node(node_id)
        if not node or node['status'] != 'active':
            return False, 'Specified node is inactive or does not exist.'
    
    try:
        # Open log file in append mode
        log_path = Path(Config.LOGS_DIR) / f'bot_{bot_id}.log'
        with open(log_path, 'a', encoding='utf-8') as log_file:
            log_file.write(f'\n{"="*60}\n')
            log_file.write(f'Start attempt at {datetime.now().isoformat()}\n')
            log_file.write(f'User: {user_id} | Node: {node["name"]}\n')
            log_file.write('=' * 60 + '\n')
            
            # Start subprocess
            proc = subprocess.Popen(
                [sys.executable, str(file_path)],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                cwd=str(Path(Config.PROJECT_DIR).resolve())
            )
        
        # Give the process a moment to start
        time.sleep(2)
        
        # Check if process is still alive
        if proc.poll() is not None:
            return False, 'Bot process exited immediately. Check your code.'
        
        now = datetime.now().isoformat()
        
        # Update deployment record
        execute_query(
            '''
            UPDATE deployments 
            SET pid = ?, start_time = ?, status = 'Running',
                node_id = ?, last_active = ?, updated_at = ?, last_start_attempt = ?
            WHERE id = ?
            ''',
            (proc.pid, now, node_id, now, now, now, bot_id),
            commit=True
        )
        
        # Update node load
        execute_query(
            'UPDATE nodes SET current_load = current_load + 1, total_deployed = total_deployed + 1 WHERE id = ?',
            (node_id,),
            commit=True
        )
        
        # Start monitoring thread
        start_bot_monitor(bot_id, proc.pid, user_id)
        
        # Log event
        log_event('BOT_START', f'Bot {bot_id} started on node {node["name"]}', user_id)
        log_bot_event(bot_id, 'START', 'Bot process launched successfully')
        
        return True, f'✅ Bot started on node **{node["name"]}** (PID: {proc.pid})'
        
    except FileNotFoundError:
        return False, 'Python interpreter not found on server.'
    except Exception as e:
        LOGGER.error(f'Start bot process error: {e}')
        return False, f'Start failed: {str(e)[:100]}'


def stop_bot_process(bot_id: int, user_id: int) -> bool:
    """
    Terminate a bot process and update database.
    Returns True if process was killed or already dead.
    """
    bot_info = execute_query(
        'SELECT pid, node_id, bot_name FROM deployments WHERE id = ?',
        (bot_id,),
        fetchone=True
    )
    
    if not bot_info:
        return False
    
    pid = bot_info['pid']
    node_id = bot_info['node_id']
    
    if pid:
        try:
            # Try graceful termination first
            os.kill(pid, signal.SIGTERM)
            time.sleep(1)
            
            # Force kill if still running
            try:
                os.kill(pid, 0)  # check existence
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass  # process already dead
        except Exception as e:
            LOGGER.warning(f'Could not kill PID {pid}: {e}')
    
    now = datetime.now().isoformat()
    
    # Update deployment status
    execute_query(
        '''
        UPDATE deployments 
        SET status = 'Stopped', pid = 0, last_active = ?, updated_at = ?
        WHERE id = ?
        ''',
        (now, now, bot_id),
        commit=True
    )
    
    # Update node load
    if node_id:
        execute_query(
            'UPDATE nodes SET current_load = current_load - 1 WHERE id = ?',
            (node_id,),
            commit=True
        )
    
    log_event('BOT_STOP', f'Bot {bot_id} stopped', user_id)
    log_bot_event(bot_id, 'STOP', 'Bot terminated by user')
    
    return True


def restart_bot_process(bot_id: int, user_id: int) -> Tuple[bool, str]:
    """
    Stop and then start a bot again.
    """
    stop_bot_process(bot_id, user_id)
    time.sleep(2)
    
    bot_info = execute_query(
        'SELECT filename, node_id FROM deployments WHERE id = ?',
        (bot_id,),
        fetchone=True
    )
    
    if not bot_info:
        return False, 'Bot record not found.'
    
    return start_bot_process(bot_id, user_id, bot_info['filename'], bot_info['node_id'])


def delete_bot_completely(bot_id: int, user_id: int) -> bool:
    """
    Permanently delete a bot: kill process, remove files, delete database records.
    """
    bot_info = execute_query(
        'SELECT filename, pid, node_id FROM deployments WHERE id = ?',
        (bot_id,),
        fetchone=True
    )
    
    if not bot_info:
        return False
    
    # Stop process
    if bot_info['pid']:
        try:
            os.kill(bot_info['pid'], signal.SIGKILL)
        except:
            pass
    
    # Delete Python file
    file_path = Path(Config.PROJECT_DIR) / bot_info['filename']
    if file_path.exists():
        file_path.unlink()
    
    # Delete log file
    log_path = Path(Config.LOGS_DIR) / f'bot_{bot_id}.log'
    if log_path.exists():
        log_path.unlink()
    
    # Update node load
    if bot_info['node_id']:
        execute_query(
            'UPDATE nodes SET current_load = current_load - 1 WHERE id = ?',
            (bot_info['node_id'],),
            commit=True
        )
    
    # Delete from database
    execute_query('DELETE FROM deployments WHERE id = ?', (bot_id,), commit=True)
    execute_query('DELETE FROM bot_logs WHERE bot_id = ?', (bot_id,), commit=True)
    execute_query('DELETE FROM bot_stats WHERE bot_id = ?', (bot_id,), commit=True)
    
    # Update user bot count
    execute_query(
        'UPDATE users SET total_bots_deployed = total_bots_deployed - 1 WHERE id = ?',
        (user_id,),
        commit=True
    )
    
    log_event('BOT_DELETE', f'Bot {bot_id} permanently deleted', user_id)
    return True


def start_bot_monitor(bot_id: int, pid: int, user_id: int) -> None:
    """
    Launch a background thread to watch the bot process.
    Handles auto‑restart and circuit breaker.
    """
    def monitor():
        LOGGER.debug(f'Monitor started for bot {bot_id} (PID: {pid})')
        
        while True:
            try:
                # Check if process exists
                os.kill(pid, 0)
            except OSError:
                # Process died
                bot_info = execute_query(
                    'SELECT bot_name, auto_restart, restart_count FROM deployments WHERE id = ?',
                    (bot_id,),
                    fetchone=True
                )
                
                if not bot_info:
                    break
                
                now = datetime.now().isoformat()
                
                # Record crash
                system_health['crash_counter'][bot_id] += 1
                system_health['crash_timestamps'][bot_id].append(time.time())
                
                log_bot_event(bot_id, 'CRASH', 'Bot process terminated unexpectedly')
                
                # Auto‑restart logic with circuit breaker
                if bot_info['auto_restart'] == 1:
                    # Clean old crash timestamps
                    window = Config.CRASH_WINDOW_SECONDS
                    recent = [
                        t for t in system_health['crash_timestamps'][bot_id]
                        if time.time() - t < window
                    ]
                    system_health['crash_timestamps'][bot_id] = recent
                    
                    if len(recent) >= Config.MAX_CRASHES_BEFORE_DISABLE:
                        # Disable auto‑restart temporarily
                        execute_query(
                            'UPDATE deployments SET auto_restart = 0 WHERE id = ?',
                            (bot_id,),
                            commit=True
                        )
                        
                        send_notification(
                            user_id,
                            f'⚠️ **Circuit breaker activated**\n'
                            f'Bot "{bot_info["bot_name"]}" crashed {len(recent)} times '
                            f'in {window//60} minutes. Auto‑restart disabled.'
                        )
                        
                        execute_query(
                            'UPDATE deployments SET status = ?, updated_at = ? WHERE id = ?',
                            ('Crashed', now, bot_id),
                            commit=True
                        )
                        break
                    else:
                        # Attempt restart
                        execute_query(
                            '''
                            UPDATE deployments 
                            SET status = 'Restarting', restart_count = restart_count + 1, updated_at = ?
                            WHERE id = ?
                            ''',
                            (now, bot_id),
                            commit=True
                        )
                        
                        time.sleep(5)
                        
                        bot_data = execute_query(
                            'SELECT filename, node_id FROM deployments WHERE id = ?',
                            (bot_id,),
                            fetchone=True
                        )
                        
                        if bot_data:
                            success, msg = start_bot_process(
                                bot_id,
                                user_id,
                                bot_data['filename'],
                                bot_data['node_id']
                            )
                            if success:
                                log_bot_event(bot_id, 'AUTO_RESTART', 'Bot auto‑restarted')
                                send_notification(
                                    user_id,
                                    f'🔄 Bot **{bot_info["bot_name"]}** auto‑restarted.'
                                )
                                break
                
                # If we reach here, bot is permanently stopped
                execute_query(
                    '''
                    UPDATE deployments 
                    SET status = 'Stopped', pid = 0, last_active = ?, updated_at = ?
                    WHERE id = ?
                    ''',
                    (now, now, bot_id),
                    commit=True
                )
                
                # Update node load
                bot_node = execute_query(
                    'SELECT node_id FROM deployments WHERE id = ?',
                    (bot_id,),
                    fetchone=True
                )
                if bot_node and bot_node['node_id']:
                    execute_query(
                        'UPDATE nodes SET current_load = current_load - 1 WHERE id = ?',
                        (bot_node['node_id'],),
                        commit=True
                    )
                
                break
            
            # Update CPU/RAM usage every 30 seconds
            time.sleep(30)
            try:
                if psutil.pid_exists(pid):
                    proc = psutil.Process(pid)
                    cpu = proc.cpu_percent()
                    mem = proc.memory_percent()
                    execute_query(
                        '''
                        UPDATE deployments 
                        SET cpu_usage = ?, ram_usage = ?, last_active = ?
                        WHERE id = ?
                        ''',
                        (cpu, mem, datetime.now().isoformat(), bot_id),
                        commit=True
                    )
            except:
                pass
        
        LOGGER.debug(f'Monitor stopped for bot {bot_id}')
    
    monitor_thread = threading.Thread(target=monitor, daemon=True)
    monitor_thread.start()
    bot_monitors[bot_id] = monitor_thread


def recover_deployments() -> None:
    """
    On bot startup, restart all bots that had auto_restart enabled
    and were in Running or Restarting state.
    """
    bots = execute_query(
        '''
        SELECT id, user_id, filename, node_id FROM deployments
        WHERE auto_restart = 1 AND (status = 'Running' OR status = 'Restarting')
        ''',
        fetchall=True
    ) or []
    
    for bot_record in bots:
        success, msg = start_bot_process(
            bot_record['id'],
            bot_record['user_id'],
            bot_record['filename'],
            bot_record['node_id']
        )
        if success:
            LOGGER.info(f'Recovered bot {bot_record["id"]}')
        else:
            LOGGER.warning(f'Failed to recover bot {bot_record["id"]}: {msg}')
    
    LOGGER.info(f'Deployment recovery completed. Attempted: {len(bots)}')


# =============================================================================
#  BOT EXPORT FUNCTIONALITY
# =============================================================================

def create_bot_export(bot_id: int, user_id: int) -> Optional[Path]:
    """
    Package a bot: main file, log, metadata, requirements.txt.
    Returns path to the created ZIP file, or None on failure.
    """
    bot_info = execute_query(
        'SELECT bot_name, filename FROM deployments WHERE id = ?',
        (bot_id,),
        fetchone=True
    )
    
    if not bot_info:
        return None
    
    bot_name = bot_info['bot_name']
    filename = bot_info['filename']
    
    export_root = Path(Config.EXPORTS_DIR) / f'export_{bot_id}_{int(time.time())}'
    export_root.mkdir(parents=True, exist_ok=True)
    
    try:
        # 1. Copy main Python file
        src_file = Path(Config.PROJECT_DIR) / filename
        if src_file.exists():
            shutil.copy2(src_file, export_root / filename)
        else:
            LOGGER.error(f'Bot file missing: {src_file}')
            return None
        
        # 2. Copy log file
        log_src = Path(Config.LOGS_DIR) / f'bot_{bot_id}.log'
        if log_src.exists():
            shutil.copy2(log_src, export_root / f'{bot_name}.log')
        
        # 3. Generate metadata.json
        metadata = {
            'bot_id': bot_id,
            'bot_name': bot_name,
            'filename': filename,
            'user_id': user_id,
            'export_date': datetime.now().isoformat(),
            'version': 'ZEN X HOST v5.0',
            'exported_by': f'@{Config.BOT_USERNAME}'
        }
        with open(export_root / 'metadata.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        # 4. Generate requirements.txt (simple scan)
        reqs = extract_requirements(src_file)
        if reqs:
            with open(export_root / 'requirements.txt', 'w', encoding='utf-8') as f:
                f.write('\n'.join(reqs))
        
        # 5. Create ZIP
        zip_name = f'bot_{bot_name}_{bot_id}.zip'
        zip_path = Path(Config.EXPORTS_DIR) / zip_name
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(export_root):
                for file in files:
                    full_path = os.path.join(root, file)
                    arcname = os.path.relpath(full_path, export_root)
                    zf.write(full_path, arcname)
        
        return zip_path
        
    except Exception as e:
        LOGGER.error(f'Bot export failed: {e}')
        return None
    finally:
        # Cleanup temporary export folder
        shutil.rmtree(export_root, ignore_errors=True)


# =============================================================================
#  PUBLIC BOT STORE
# =============================================================================

def get_public_bots(page: int = 1, page_size: int = Config.PUBLIC_BOT_PAGE_SIZE) -> List[Dict[str, Any]]:
    """Fetch paginated list of public bots."""
    offset = (page - 1) * page_size
    rows = execute_query(
        '''
        SELECT d.id, d.bot_name, d.description, d.tags, d.created_at,
               u.username, u.id as owner_id
        FROM deployments d
        LEFT JOIN users u ON d.user_id = u.id
        WHERE d.public_bot = 1 AND d.status != 'Deleted'
        ORDER BY d.id DESC
        LIMIT ? OFFSET ?
        ''',
        (page_size, offset),
        fetchall=True
    ) or []
    
    return [dict(row) for row in rows]


def set_bot_public(bot_id: int, user_id: int, public: bool = True, description: str = '', tags: str = '') -> bool:
    """Mark a bot as public or private."""
    # Verify ownership
    bot = execute_query(
        'SELECT user_id FROM deployments WHERE id = ?',
        (bot_id,),
        fetchone=True
    )
    if not bot or bot['user_id'] != user_id:
        return False
    
    execute_query(
        '''
        UPDATE deployments
        SET public_bot = ?, description = ?, tags = ?, updated_at = ?
        WHERE id = ?
        ''',
        (1 if public else 0, description[:Config.MAX_DESCRIPTION_LENGTH],
         tags[:Config.MAX_TAGS_LENGTH], datetime.now().isoformat(), bot_id),
        commit=True
    )
    return True


def increment_bot_view(bot_id: int) -> None:
    """Increment view counter for a public bot."""
    now = datetime.now().isoformat()
    execute_query(
        '''
        INSERT INTO bot_stats (bot_id, views, last_viewed)
        VALUES (?, 1, ?)
        ON CONFLICT(bot_id) DO UPDATE SET
            views = views + 1,
            last_viewed = excluded.last_viewed
        ''',
        (bot_id, now),
        commit=True
    )


def increment_bot_download(bot_id: int) -> None:
    """Increment download counter for a public bot."""
    now = datetime.now().isoformat()
    execute_query(
        '''
        INSERT INTO bot_stats (bot_id, downloads, last_viewed)
        VALUES (?, 1, ?)
        ON CONFLICT(bot_id) DO UPDATE SET
            downloads = downloads + 1,
            last_viewed = excluded.last_viewed
        ''',
        (bot_id, now),
        commit=True
    )


# =============================================================================
#  NOTIFICATION & LOGGING
# =============================================================================

def log_event(event: str, details: str, user_id: int = None, severity: str = 'INFO') -> None:
    """Insert a record into server_logs."""
    try:
        timestamp = datetime.now().isoformat()
        execute_query(
            '''
            INSERT INTO server_logs (timestamp, event, details, user_id, severity)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (timestamp, event, details[:1000], user_id, severity),
            commit=True
        )
    except Exception as e:
        LOGGER.error(f'Failed to log event: {e}')


def log_bot_event(bot_id: int, log_type: str, message: str) -> None:
    """Insert a record into bot_logs."""
    try:
        timestamp = datetime.now().isoformat()
        execute_query(
            '''
            INSERT INTO bot_logs (bot_id, timestamp, log_type, message)
            VALUES (?, ?, ?, ?)
            ''',
            (bot_id, timestamp, log_type, message[:500]),
            commit=True
        )
    except Exception as e:
        LOGGER.error(f'Failed to log bot event: {e}')


def send_notification(user_id: int, message: str, store: bool = True) -> bool:
    """
    Send a notification to a user and optionally store in database.
    Returns True if sent successfully.
    """
    if store:
        try:
            timestamp = datetime.now().isoformat()
            execute_query(
                '''
                INSERT INTO notifications (user_id, message, created_at)
                VALUES (?, ?, ?)
                ''',
                (user_id, message, timestamp),
                commit=True
            )
        except Exception as e:
            LOGGER.error(f'Failed to store notification: {e}')
    
    try:
        bot.send_message(user_id, f'📢 **Notification**\n\n{message}')
        return True
    except Exception as e:
        LOGGER.warning(f'Failed to send notification to {user_id}: {e}')
        return False


def get_unread_notifications(user_id: int) -> List[Dict[str, Any]]:
    """Fetch unread notifications for a user."""
    rows = execute_query(
        '''
        SELECT id, message, created_at FROM notifications
        WHERE user_id = ? AND is_read = 0
        ORDER BY id DESC
        LIMIT 20
        ''',
        (user_id,),
        fetchall=True
    ) or []
    
    # Mark as read
    execute_query(
        'UPDATE notifications SET is_read = 1 WHERE user_id = ?',
        (user_id,),
        commit=True
    )
    
    return [dict(row) for row in rows]


# =============================================================================
#  SYSTEM HEALTH & AUTO‑BACKUP
# =============================================================================

def get_system_stats() -> Dict[str, Any]:
    """
    Gather comprehensive system health metrics.
    """
    stats = {
        'cpu_percent': 0.0,
        'ram_percent': 0.0,
        'disk_percent': 0.0,
        'total_users': 0,
        'prime_users': 0,
        'total_bots': 0,
        'running_bots': 0,
        'deployed_today': 0,
        'uptime_seconds': 0,
        'total_capacity': 0,
        'used_capacity': 0,
        'available_capacity': 0,
        'last_backup': 'Never',
        'backup_count': 0,
        'platform': platform.system(),
        'python_version': platform.python_version(),
        'crash_rate': 0.0,
        'active_nodes': 0,
        'total_nodes': 0
    }
    
    try:
        # System resources
        stats['cpu_percent'] = psutil.cpu_percent(interval=0.5)
        stats['ram_percent'] = psutil.virtual_memory().percent
        stats['disk_percent'] = psutil.disk_usage('/').percent
        stats['uptime_seconds'] = (datetime.now() - system_health['bot_start_time']).total_seconds()
        
        # Database counts
        total_users = execute_query('SELECT COUNT(*) FROM users', fetchone=True)
        stats['total_users'] = total_users[0] if total_users else 0
        
        prime = execute_query('SELECT COUNT(*) FROM users WHERE is_prime = 1', fetchone=True)
        stats['prime_users'] = prime[0] if prime else 0
        
        total_bots = execute_query('SELECT COUNT(*) FROM deployments', fetchone=True)
        stats['total_bots'] = total_bots[0] if total_bots else 0
        
        running = execute_query(
            'SELECT COUNT(*) FROM deployments WHERE status = ?',
            ('Running',),
            fetchone=True
        )
        stats['running_bots'] = running[0] if running else 0
        
        today = datetime.now().date().isoformat()
        deployed = execute_query(
            'SELECT COUNT(*) FROM deployments WHERE DATE(created_at) = ?',
            (today,),
            fetchone=True
        )
        stats['deployed_today'] = deployed[0] if deployed else 0
        
        # Nodes
        nodes = get_all_nodes()
        stats['total_nodes'] = len(nodes)
        stats['active_nodes'] = sum(1 for n in nodes if n['status'] == 'active')
        stats['total_capacity'] = sum(n['capacity'] for n in nodes)
        stats['used_capacity'] = sum(n['current_load'] for n in nodes)
        stats['available_capacity'] = stats['total_capacity'] - stats['used_capacity']
        
        # Backups
        backup_files = list(Path(Config.BACKUP_DIR).glob('*.zip'))
        stats['backup_count'] = len(backup_files)
        if backup_files:
            latest = max(backup_files, key=lambda f: f.stat().st_mtime)
            stats['last_backup'] = datetime.fromtimestamp(latest.stat().st_mtime).isoformat()
        
        # Crash rate (last 5 minutes)
        now = time.time()
        all_crashes = [
            t for ts_list in system_health['crash_timestamps'].values()
            for t in ts_list if now - t < 300
        ]
        stats['crash_rate'] = len(all_crashes) / 5.0
        
    except Exception as e:
        LOGGER.error(f'Stats collection error: {e}')
    
    return stats


def full_system_backup(reason: str = 'manual') -> Optional[Path]:
    """
    Create a complete system backup including database, projects, logs, config.
    Returns path to the created ZIP file.
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_name = f'full_backup_{reason}_{timestamp}'
    backup_dir = Path(Config.BACKUP_DIR) / backup_name
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # 1. Database
        with db_lock:
            shutil.copy2(Config.DB_NAME, backup_dir / Config.DB_NAME)
        
        # 2. Projects
        if Path(Config.PROJECT_DIR).exists():
            shutil.copytree(
                Config.PROJECT_DIR,
                backup_dir / Config.PROJECT_DIR,
                dirs_exist_ok=True
            )
        
        # 3. Logs
        if Path(Config.LOGS_DIR).exists():
            shutil.copytree(
                Config.LOGS_DIR,
                backup_dir / Config.LOGS_DIR,
                dirs_exist_ok=True
            )
        
        # 4. Configuration as JSON
        config_dict = {
            k: getattr(Config, k) for k in dir(Config)
            if not k.startswith('_') and not callable(getattr(Config, k))
        }
        with open(backup_dir / 'config.json', 'w', encoding='utf-8') as f:
            json.dump(config_dict, f, indent=2, default=str)
        
        # 5. System stats snapshot
        with open(backup_dir / 'system_stats.json', 'w', encoding='utf-8') as f:
            json.dump(get_system_stats(), f, indent=2, default=str)
        
        # 6. Create ZIP
        zip_path = Path(Config.BACKUP_DIR) / f'{backup_name}.zip'
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(backup_dir):
                for file in files:
                    full = os.path.join(root, file)
                    rel = os.path.relpath(full, backup_dir)
                    zf.write(full, rel)
        
        LOGGER.info(f'Full system backup created: {zip_path.name}')
        return zip_path
        
    except Exception as e:
        LOGGER.error(f'Full system backup failed: {e}')
        return None
    finally:
        shutil.rmtree(backup_dir, ignore_errors=True)


def health_check_worker() -> None:
    """
    Background thread that monitors system health and triggers
    emergency backups when thresholds are exceeded.
    """
    while True:
        time.sleep(60)  # check every minute
        
        try:
            stats = get_system_stats()
            thresholds = Config.AUTO_BACKUP_THRESHOLDS
            alerts = []
            
            if stats['cpu_percent'] > thresholds['cpu_percent']:
                alerts.append(f'CPU: {stats["cpu_percent"]:.1f}%')
            if stats['ram_percent'] > thresholds['ram_percent']:
                alerts.append(f'RAM: {stats["ram_percent"]:.1f}%')
            if stats['disk_percent'] > thresholds['disk_percent']:
                alerts.append(f'Disk: {stats["disk_percent"]:.1f}%')
            if stats['crash_rate'] > thresholds['crash_rate']:
                alerts.append(f'Crash rate: {stats["crash_rate"]:.1f}/min')
            
            if alerts and Config.ADMIN_ID:
                reason = 'auto_' + '_'.join(a.split()[0].lower() for a in alerts[:2])
                zip_path = full_system_backup(reason=reason)
                
                if zip_path:
                    msg = (
                        '🚨 **Emergency Auto‑Backup Triggered**\n\n'
                        '**Thresholds exceeded:**\n' + '\n'.join(f'• {a}' for a in alerts)
                    )
                    try:
                        with open(zip_path, 'rb') as f:
                            bot.send_document(Config.ADMIN_ID, f, caption=msg)
                        system_health['last_emergency_backup'] = datetime.now().isoformat()
                    except Exception as e:
                        LOGGER.error(f'Failed to send emergency backup: {e}')
        
        except Exception as e:
            LOGGER.error(f'Health check error: {e}')


def auto_backup_scheduler() -> None:
    """
    Background thread that creates scheduled backups every BACKUP_INTERVAL.
    Keeps only the last 7 backups.
    """
    while True:
        time.sleep(Config.BACKUP_INTERVAL)
        zip_path = full_system_backup(reason='scheduled')
        if zip_path:
            # Keep only the 7 most recent backups
            backups = sorted(
                Path(Config.BACKUP_DIR).glob('full_backup_*.zip'),
                key=lambda f: f.stat().st_mtime
            )
            while len(backups) > 7:
                oldest = backups.pop(0)
                oldest.unlink()
                LOGGER.info(f'Removed old backup: {oldest.name}')


def cleanup_old_sessions() -> None:
    """Remove expired user sessions every 10 minutes."""
    while True:
        time.sleep(600)
        now = time.time()
        expired = [
            uid for uid, sess in user_sessions.items()
            if sess.get('expires', 0) < now
        ]
        for uid in expired:
            del user_sessions[uid]
        if expired:
            LOGGER.debug(f'Cleaned {len(expired)} expired sessions')


# =============================================================================
#  SESSION MANAGEMENT
# =============================================================================

def get_session(user_id: int) -> Dict[str, Any]:
    """Retrieve user session; returns empty dict if expired or not found."""
    sess = user_sessions.get(user_id, {})
    if 'expires' in sess and sess['expires'] < time.time():
        del user_sessions[user_id]
        return {}
    return sess


def set_session(user_id: int, data: Dict[str, Any]) -> None:
    """Store user session with expiration."""
    data['expires'] = time.time() + Config.SESSION_TIMEOUT
    user_sessions[user_id] = data


def clear_session(user_id: int) -> None:
    """Remove user session."""
    user_sessions.pop(user_id, None)


def update_message_history(chat_id: int, message_id: int) -> None:
    """Keep track of sent messages for later editing."""
    if chat_id not in user_message_history:
        user_message_history[chat_id] = []
    
    user_message_history[chat_id].append(message_id)
    
    # Keep only the last N messages
    max_len = Config.MAX_MESSAGE_HISTORY
    if len(user_message_history[chat_id]) > max_len:
        user_message_history[chat_id] = user_message_history[chat_id][-max_len:]


def edit_or_send(
    chat_id: int,
    message_id: Optional[int],
    text: str,
    reply_markup: Optional[types.InlineKeyboardMarkup] = None,
    parse_mode: str = 'Markdown'
) -> types.Message:
    """
    Safely edit an existing message or send a new one if editing fails.
    """
    try:
        if message_id:
            try:
                return bot.edit_message_text(
                    text,
                    chat_id,
                    message_id,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode
                )
            except telebot.apihelper.ApiException as e:
                if 'message can\'t be edited' in str(e) or 'message is not modified' in str(e):
                    # Send a new message instead
                    msg = bot.send_message(
                        chat_id,
                        text,
                        reply_markup=reply_markup,
                        parse_mode=parse_mode
                    )
                    update_message_history(chat_id, msg.message_id)
                    return msg
                else:
                    raise
        else:
            msg = bot.send_message(
                chat_id,
                text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
            update_message_history(chat_id, msg.message_id)
            return msg
    except Exception as e:
        LOGGER.warning(f'Edit/send fallback: {e}')
        msg = bot.send_message(
            chat_id,
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
        update_message_history(chat_id, msg.message_id)
        return msg


# =============================================================================
#  KEYBOARD LAYOUTS
# =============================================================================

def main_menu_keyboard(user_id: int) -> types.ReplyKeyboardMarkup:
    """Dynamic main menu based on Prime status."""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    prime = check_prime_status(user_id)
    
    if not prime['expired']:
        buttons = [
            '📤 Upload Bot', '🤖 My Bots',
            '🚀 Deploy Bot', '📊 Dashboard',
            '⚙️ Settings', '👑 Prime Info',
            '🔔 Notifications', '🌍 Explore Bots'
        ]
    else:
        buttons = [
            '🔑 Activate Prime', '👑 Prime Info',
            '📞 Contact Admin', 'ℹ️ Help'
        ]
    
    markup.add(*[types.KeyboardButton(b) for b in buttons])
    
    if user_id == Config.ADMIN_ID:
        markup.add(types.KeyboardButton('👑 Admin Panel'))
    
    return markup


def admin_keyboard() -> types.ReplyKeyboardMarkup:
    """Admin panel keyboard."""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        '🎫 Generate Key', '👥 All Users',
        '🤖 All Bots', '📈 Statistics',
        '🗄️ Database', '💾 Backup',
        '⚙️ Maintenance', '🌐 Nodes',
        '🔧 Logs', '📊 System Info',
        '🔔 Broadcast', '🔄 Cleanup',
        '🏠 Main Menu'
    ]
    
    for i in range(0, len(buttons), 2):
        markup.add(*[types.KeyboardButton(b) for b in buttons[i:i+2]])
    
    return markup


def bot_actions_keyboard(bot_id: int) -> types.InlineKeyboardMarkup:
    """Inline keyboard for bot management."""
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    markup.add(
        types.InlineKeyboardButton('🛑 Stop', callback_data=f'stop_{bot_id}'),
        types.InlineKeyboardButton('🔄 Restart', callback_data=f'restart_{bot_id}'),
        types.InlineKeyboardButton('📥 Export', callback_data=f'export_{bot_id}'),
        types.InlineKeyboardButton('🗑️ Delete', callback_data=f'delete_{bot_id}'),
        types.InlineKeyboardButton('📜 Logs', callback_data=f'logs_{bot_id}'),
        types.InlineKeyboardButton('🔁 Auto-Restart', callback_data=f'autorestart_{bot_id}')
    )
    
    markup.add(
        types.InlineKeyboardButton('📊 Stats', callback_data=f'stats_{bot_id}'),
        types.InlineKeyboardButton('🔙 Back', callback_data='my_bots')
    )
    
    return markup


def confirm_keyboard(action: str, bot_id: int) -> types.InlineKeyboardMarkup:
    """Yes/No confirmation keyboard."""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton('✅ Yes', callback_data=f'confirm_{action}_{bot_id}'),
        types.InlineKeyboardButton('❌ No', callback_data=f'bot_{bot_id}')
    )
    return markup


def public_bot_keyboard(bot_id: int) -> types.InlineKeyboardMarkup:
    """Keyboard for public bot details."""
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton('📥 Export', callback_data=f'export_{bot_id}'),
        types.InlineKeyboardButton('🔙 Back to Store', callback_data='explore')
    )
    return markup


# =============================================================================
#  COMMAND HANDLERS (TEXT & KEYBOARD)
# =============================================================================

@bot.message_handler(commands=['start', 'menu', 'help'])
def cmd_start(message: types.Message) -> None:
    """
    /start - Welcome message and main menu.
    """
    user_id = message.from_user.id
    username = message.from_user.username or f'User_{user_id}'
    first_name = message.from_user.first_name or ''
    
    if Config.MAINTENANCE and user_id != Config.ADMIN_ID:
        bot.reply_to(
            message,
            '🛠 **System Maintenance**\n\nThe bot is currently under maintenance. '
            'Please try again later.'
        )
        return
    
    # Register user if not exists
    create_user(user_id, username, first_name)
    update_user_activity(user_id)
    clear_session(user_id)
    
    prime = check_prime_status(user_id)
    
    # Get unread notification count
    unread = execute_query(
        'SELECT COUNT(*) FROM notifications WHERE user_id = ? AND is_read = 0',
        (user_id,),
        fetchone=True
    )
    unread_count = unread[0] if unread else 0
    
    welcome_text = (
        '🤖 **ZEN X HOST BOT – ULTIMATE EDITION v5.0**\n'
        '*Auto‑Recovery • Emergency Backup • Public Store • Node Control*\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        f'👤 **User:** @{username}\n'
        f'🆔 **ID:** `{user_id}`\n'
        f'💎 **Status:** {"PRIME 👑" if not prime["expired"] else "FREE"}\n'
        f'📅 **Expiry:** {prime.get("expiry_date", "N/A")[:10] if not prime["expired"] else "Not Activated"}\n'
        f'🔔 **Notifications:** {unread_count} unread\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        'Use the buttons below or type commands:\n'
        '`/upload` `/mybots` `/deploy <id>` `/explore`'
    )
    
    bot.send_message(
        user_id,
        welcome_text,
        reply_markup=main_menu_keyboard(user_id)
    )


@bot.message_handler(commands=['admin'])
def cmd_admin(message: types.Message) -> None:
    """
    /admin - Access admin panel (admin only).
    """
    user_id = message.from_user.id
    if user_id == Config.ADMIN_ID:
        bot.send_message(
            user_id,
            '👑 **Admin Control Panel**\n\nSelect an option from the keyboard below.',
            reply_markup=admin_keyboard()
        )
    else:
        bot.reply_to(message, '⛔ **Access Denied.**')


@bot.message_handler(commands=['upload'])
@rate_limit('upload')
def cmd_upload(message: types.Message) -> None:
    """
    /upload - Upload a Python file or ZIP archive.
    """
    user_id = message.from_user.id
    prime = check_prime_status(user_id)
    
    if prime['expired']:
        bot.reply_to(
            message,
            '⚠️ **Prime Required**\n\nYou need an active Prime subscription to upload bots.'
        )
        return
    
    # Check bot limit
    bot_count = execute_query(
        'SELECT COUNT(*) FROM deployments WHERE user_id = ?',
        (user_id,),
        fetchone=True
    )[0]
    
    if bot_count >= Config.MAX_BOTS_PER_USER:
        bot.reply_to(
            message,
            f'❌ **Bot Limit Reached**\n\nYou can only have {Config.MAX_BOTS_PER_USER} bots at a time.'
        )
        return
    
    set_session(user_id, {'state': 'waiting_for_file'})
    bot.reply_to(
        message,
        '📤 **Upload Your Bot File**\n\n'
        'Send a **.py** Python file or a **.zip** archive containing a Python script.\n'
        f'Maximum size: {Config.MAX_FILE_SIZE_MB} MB\n\n'
        'Type `/cancel` to abort.'
    )


@bot.message_handler(commands=['mybots'])
def cmd_mybots(message: types.Message) -> None:
    """
    /mybots - List all bots owned by the user.
    """
    user_id = message.from_user.id
    bots = execute_query(
        '''
        SELECT id, bot_name, status, auto_restart, created_at, public_bot
        FROM deployments
        WHERE user_id = ?
        ORDER BY id DESC
        ''',
        (user_id,),
        fetchall=True
    )
    
    if not bots:
        bot.send_message(
            user_id,
            '🤖 **No Bots Found**\n\nUpload your first bot using /upload'
        )
        return
    
    text = f'🤖 **Your Bots** (Total: {len(bots)})\n━━━━━━━━━━━━━━━━━━━━\n'
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    for bot_record in bots:
        status_icon = '🟢' if bot_record['status'] == 'Running' else '🔴'
        auto_icon = '🔁' if bot_record['auto_restart'] else '⏸️'
        public_icon = '🌍' if bot_record['public_bot'] else '🔒'
        created = bot_record['created_at'][:10] if bot_record['created_at'] else 'N/A'
        
        text += f'{status_icon}{auto_icon}{public_icon} **{bot_record["bot_name"]}**\n'
        text += f'`ID: {bot_record["id"]}` | Status: {bot_record["status"]} | {created}\n━━━━━━━━━━━━━━━━━━━━\n'
        
        button_text = f'{status_icon}{auto_icon}{public_icon} {bot_record["bot_name"][:20]}'
        markup.add(
            types.InlineKeyboardButton(
                button_text,
                callback_data=f'bot_{bot_record["id"]}'
            )
        )
    
    bot.send_message(user_id, text, reply_markup=markup)


@bot.message_handler(commands=['deploy'])
@rate_limit('deploy')
def cmd_deploy(message: types.Message) -> None:
    """
    /deploy <bot_id> - Deploy a specific bot.
    """
    user_id = message.from_user.id
    prime = check_prime_status(user_id)
    
    if prime['expired']:
        bot.reply_to(
            message,
            '⚠️ **Prime Required**\n\nYou need an active Prime subscription to deploy bots.'
        )
        return
    
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(
            message,
            '❌ **Usage:** `/deploy <bot_id>`\n\nExample: `/deploy 5`'
        )
        return
    
    try:
        bot_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, '❌ Invalid bot ID. Must be a number.')
        return
    
    bot_info = execute_query(
        'SELECT * FROM deployments WHERE id = ? AND user_id = ?',
        (bot_id, user_id),
        fetchone=True
    )
    
    if not bot_info:
        bot.reply_to(message, '❌ Bot not found or you do not own it.')
        return
    
    if not rate_limit('deploy', user_id):
        return
    
    status_msg = bot.reply_to(message, '🚀 **Deploying bot...**')
    
    success, msg = start_bot_process(bot_id, user_id, bot_info['filename'])
    
    if success:
        bot.edit_message_text(
            f'✅ **Deployment Successful**\n\n{msg}',
            status_msg.chat.id,
            status_msg.message_id
        )
        send_notification(
            user_id,
            f'✅ Bot **{bot_info["bot_name"]}** has been deployed.'
        )
    else:
        bot.edit_message_text(
            f'❌ **Deployment Failed**\n\n{msg}',
            status_msg.chat.id,
            status_msg.message_id
        )


@bot.message_handler(commands=['stop', 'restart', 'delete', 'export', 'logs'])
@rate_limit('bot_action')
def cmd_bot_action(message: types.Message) -> None:
    """
    Unified handler for bot actions: stop, restart, delete, export, logs.
    """
    user_id = message.from_user.id
    command = message.text.split()[0][1:]  # remove leading '/'
    
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(
            message,
            f'❌ **Usage:** `/{command} <bot_id>`\n\nExample: `/{command} 5`'
        )
        return
    
    try:
        bot_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, '❌ Invalid bot ID. Must be a number.')
        return
    
    # Allow admin to act on any bot
    if user_id == Config.ADMIN_ID:
        bot_info = execute_query(
            'SELECT * FROM deployments WHERE id = ?',
            (bot_id,),
            fetchone=True
        )
    else:
        bot_info = execute_query(
            'SELECT * FROM deployments WHERE id = ? AND user_id = ?',
            (bot_id, user_id),
            fetchone=True
        )
    
    if not bot_info:
        bot.reply_to(message, '❌ Bot not found or access denied.')
        return
    
    if command == 'stop':
        success = stop_bot_process(bot_id, user_id)
        if success:
            bot.reply_to(message, f'🛑 Bot **{bot_info["bot_name"]}** stopped.')
        else:
            bot.reply_to(message, '❌ Failed to stop bot.')
    
    elif command == 'restart':
        success, msg = restart_bot_process(bot_id, user_id)
        if success:
            bot.reply_to(message, f'🔄 Restarting **{bot_info["bot_name"]}**...')
        else:
            bot.reply_to(message, f'❌ Restart failed: {msg}')
    
    elif command == 'delete':
        # Ask for confirmation
        markup = confirm_keyboard('delete', bot_id)
        bot.reply_to(
            message,
            f'⚠️ **Delete Bot?**\n\nAre you sure you want to permanently delete '
            f'**{bot_info["bot_name"]}**? This action cannot be undone.',
            reply_markup=markup
        )
    
    elif command == 'export':
        if not rate_limit('export', user_id):
            return
        
        status_msg = bot.reply_to(message, '📦 **Exporting bot...**')
        zip_path = create_bot_export(bot_id, user_id)
        
        if zip_path and zip_path.exists():
            with open(zip_path, 'rb') as f:
                bot.send_document(
                    user_id,
                    f,
                    caption=f'📦 **{bot_info["bot_name"]}** – Full Export\n'
                            f'Bot ID: `{bot_id}`\n'
                            f'Exported: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                )
            zip_path.unlink()  # delete temporary file
            bot.delete_message(status_msg.chat.id, status_msg.message_id)
            increment_bot_download(bot_id)
        else:
            bot.edit_message_text(
                '❌ Export failed.',
                status_msg.chat.id,
                status_msg.message_id
            )
    
    elif command == 'logs':
        show_bot_logs(bot_id, message.chat.id)


@bot.message_handler(commands=['dashboard', 'stats'])
def cmd_dashboard(message: types.Message) -> None:
    """
    /dashboard - Display user dashboard with system stats.
    """
    user_id = message.from_user.id
    stats = get_system_stats()
    
    user_bots = execute_query(
        'SELECT COUNT(*) as total, SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) as running '
        'FROM deployments WHERE user_id = ?',
        ('Running', user_id),
        fetchone=True
    )
    
    user_deployments = execute_query(
        'SELECT total_bots_deployed, total_deployments FROM users WHERE id = ?',
        (user_id,),
        fetchone=True
    )
    
    prime = check_prime_status(user_id)
    
    text = (
        '📊 **User Dashboard**\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        f'👤 **User ID:** `{user_id}`\n'
        f'💎 **Prime:** {"Active" if not prime["expired"] else "Inactive"}\n'
        f'📦 **Bots:** {user_bots["total"] or 0} total, {user_bots["running"] or 0} running\n'
        f'📈 **Deployments:** {user_deployments["total_deployments"] if user_deployments else 0}\n'
        '━━━━━━━━━━━━━━━━━━━━\n'
        '🖥️ **System Status**\n'
        f'• CPU: {create_progress_bar(stats["cpu_percent"])} {stats["cpu_percent"]:.1f}%\n'
        f'• RAM: {create_progress_bar(stats["ram_percent"])} {stats["ram_percent"]:.1f}%\n'
        f'• Disk: {create_progress_bar(stats["disk_percent"])} {stats["disk_percent"]:.1f}%\n'
        f'• Capacity: {stats["used_capacity"]}/{stats["total_capacity"]} bots\n'
        f'• Uptime: {format_uptime(stats["uptime_seconds"])}\n'
        '━━━━━━━━━━━━━━━━━━━━'
    )
    
    bot.send_message(user_id, text)


@bot.message_handler(commands=['explore', 'public'])
def cmd_explore(message: types.Message) -> None:
    """
    /explore - Browse public bots uploaded by other users.
    """
    user_id = message.from_user.id
    page = 1
    bots = get_public_bots(page)
    
    if not bots:
        bot.send_message(
            user_id,
            '🌍 **Public Bot Store**\n\nNo public bots available yet.\n'
            'Be the first to make a bot public!'
        )
        return
    
    text = '🌍 **Public Bot Store**\n━━━━━━━━━━━━━━━━━━━━\n'
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    for bot_entry in bots:
        text += (
            f'🤖 **{bot_entry["bot_name"]}**\n'
            f'👤 Owner: @{bot_entry["username"] or "Unknown"}\n'
            f'📝 {bot_entry["description"] or "No description"}\n'
            f'🆔 `{bot_entry["id"]}`\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
        )
        markup.add(
            types.InlineKeyboardButton(
                f'View {bot_entry["bot_name"]}',
                callback_data=f'viewpublic_{bot_entry["id"]}'
            )
        )
    
    # Pagination
    total_public = execute_query(
        'SELECT COUNT(*) FROM deployments WHERE public_bot = 1',
        fetchone=True
    )[0]
    
    if total_public > Config.PUBLIC_BOT_PAGE_SIZE:
        markup.add(
            types.InlineKeyboardButton('Next ➡️', callback_data='explore_page_2')
        )
    
    bot.send_message(user_id, text, reply_markup=markup)


@bot.message_handler(commands=['activate'])
def cmd_activate(message: types.Message) -> None:
    """
    /activate <key> - Activate a Prime subscription key.
    """
    user_id = message.from_user.id
    parts = message.text.split()
    
    if len(parts) != 2:
        bot.reply_to(
            message,
            '❌ **Usage:** `/activate <key>`\n\nExample: `/activate ZENX-ABC123DEF456`'
        )
        return
    
    key = parts[1].strip().upper()
    
    if not is_valid_key_format(key):
        bot.reply_to(message, '❌ Invalid key format. Should be `ZENX-XXXXXXXXXXXX`')
        return
    
    success, msg = activate_prime(user_id, key)
    bot.reply_to(message, msg)
    
    if success:
        # Refresh keyboard
        bot.send_message(
            user_id,
            '✅ Prime activated! Your main menu has been updated.',
            reply_markup=main_menu_keyboard(user_id)
        )


@bot.message_handler(commands=['backup', 'fullbackup'])
def cmd_backup(message: types.Message) -> None:
    """
    /backup - Create a full system backup (admin only).
    """
    user_id = message.from_user.id
    if user_id != Config.ADMIN_ID:
        bot.reply_to(message, '⛔ **Admin only.**')
        return
    
    status_msg = bot.reply_to(message, '💾 **Creating full system backup...**')
    zip_path = full_system_backup(reason='manual')
    
    if zip_path:
        with open(zip_path, 'rb') as f:
            bot.send_document(
                user_id,
                f,
                caption=f'✅ **Full System Backup**\n'
                        f'Date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
                        f'Size: {format_size(zip_path.stat().st_size)}'
            )
        zip_path.unlink()
        bot.delete_message(status_msg.chat.id, status_msg.message_id)
    else:
        bot.edit_message_text(
            '❌ Backup failed. Check logs.',
            status_msg.chat.id,
            status_msg.message_id
        )


@bot.message_handler(commands=['broadcast'])
def cmd_broadcast(message: types.Message) -> None:
    """
    /broadcast <message> - Send a message to all users (admin only).
    """
    user_id = message.from_user.id
    if user_id != Config.ADMIN_ID:
        return
    
    text = message.text.replace('/broadcast', '', 1).strip()
    if not text:
        bot.reply_to(message, '❌ **Usage:** `/broadcast <message>`')
        return
    
    users = execute_query('SELECT id FROM users', fetchall=True) or []
    success_count = 0
    fail_count = 0
    
    status_msg = bot.reply_to(
        message,
        f'📢 **Broadcasting...**\n\nTotal users: {len(users)}\nProgress: 0%'
    )
    
    for i, user in enumerate(users):
        try:
            bot.send_message(
                user['id'],
                f'📢 **Broadcast Message**\n\n{text}'
            )
            success_count += 1
        except Exception:
            fail_count += 1
        
        # Update progress every 10 users
        if i % 10 == 0 and i > 0:
            percent = int((i + 1) / len(users) * 100)
            try:
                bot.edit_message_text(
                    f'📢 **Broadcasting...**\n\nTotal users: {len(users)}\nProgress: {percent}%',
                    status_msg.chat.id,
                    status_msg.message_id
                )
            except:
                pass
    
    bot.edit_message_text(
        f'✅ **Broadcast Complete**\n\n'
        f'Total: {len(users)}\n'
        f'✅ Success: {success_count}\n'
        f'❌ Failed: {fail_count}',
        status_msg.chat.id,
        status_msg.message_id
    )


@bot.message_handler(commands=['node'])
def cmd_node(message: types.Message) -> None:
    """
    /node add <name> <region> <capacity> - Add a new node (admin)
    /node remove <id> - Remove a node
    /node capacity <id> <new_capacity> - Change node capacity
    /node status <id> <active|maintenance|offline> - Change node status
    """
    user_id = message.from_user.id
    if user_id != Config.ADMIN_ID:
        bot.reply_to(message, '⛔ **Admin only.**')
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(
            message,
            '❌ **Node Management Commands:**\n\n'
            '`/node add <name> <region> <capacity>`\n'
            '`/node remove <id>`\n'
            '`/node capacity <id> <new_capacity>`\n'
            '`/node status <id> <active|maintenance|offline>`'
        )
        return
    
    subcmd = parts[1].lower()
    
    if subcmd == 'add' and len(parts) == 5:
        name = parts[2]
        region = parts[3]
        try:
            capacity = int(parts[4])
        except ValueError:
            bot.reply_to(message, '❌ Capacity must be an integer.')
            return
        
        if add_node(name, region, capacity):
            bot.reply_to(message, f'✅ Node **{name}** added with capacity {capacity}.')
        else:
            bot.reply_to(message, f'❌ Node **{name}** already exists.')
    
    elif subcmd == 'remove' and len(parts) == 3:
        try:
            node_id = int(parts[2])
        except ValueError:
            bot.reply_to(message, '❌ Node ID must be an integer.')
            return
        
        if remove_node(node_id):
            bot.reply_to(message, f'✅ Node {node_id} removed.')
        else:
            bot.reply_to(
                message,
                '❌ Cannot remove node. It may have active bots or does not exist.'
            )
    
    elif subcmd == 'capacity' and len(parts) == 4:
        try:
            node_id = int(parts[2])
            new_capacity = int(parts[3])
        except ValueError:
            bot.reply_to(message, '❌ Node ID and capacity must be integers.')
            return
        
        if set_node_capacity(node_id, new_capacity):
            bot.reply_to(message, f'✅ Node {node_id} capacity updated to {new_capacity}.')
        else:
            bot.reply_to(
                message,
                '❌ Failed to update capacity. Node may not exist or capacity < current load.'
            )
    
    elif subcmd == 'status' and len(parts) == 4:
        try:
            node_id = int(parts[2])
        except ValueError:
            bot.reply_to(message, '❌ Node ID must be an integer.')
            return
        
        status = parts[3].lower()
        if status not in ('active', 'maintenance', 'offline'):
            bot.reply_to(message, '❌ Status must be `active`, `maintenance`, or `offline`.')
            return
        
        if set_node_status(node_id, status):
            bot.reply_to(message, f'✅ Node {node_id} status set to **{status}**.')
        else:
            bot.reply_to(message, '❌ Failed to update node status.')
    
    else:
        bot.reply_to(message, '❌ Invalid node command.')


@bot.message_handler(commands=['clean'])
def cmd_clean(message: types.Message) -> None:
    """
    /clean <count> - Delete the last N messages in the chat (admin only).
    """
    user_id = message.from_user.id
    if user_id != Config.ADMIN_ID:
        bot.reply_to(message, '⛔ **Admin only.**')
        return
    
    parts = message.text.split()
    count = 10  # default
    
    if len(parts) >= 2:
        try:
            count = int(parts[1])
            count = max(1, min(count, 50))  # limit to 50
        except ValueError:
            bot.reply_to(message, '❌ Invalid count. Using default 10.')
    
    chat_id = message.chat.id
    
    # We need message IDs to delete. We'll rely on our stored message history.
    if chat_id in user_message_history:
        ids_to_delete = user_message_history[chat_id][-count:]
        delete_messages(chat_id, ids_to_delete)
        bot.reply_to(message, f'✅ Deleted {len(ids_to_delete)} messages.')
    else:
        bot.reply_to(message, '❌ No message history for this chat.')


@bot.message_handler(commands=['cancel'])
def cmd_cancel(message: types.Message) -> None:
    """
    /cancel - Cancel the current operation and clear session.
    """
    user_id = message.from_user.id
    clear_session(user_id)
    bot.reply_to(
        message,
        '❌ Operation cancelled.',
        reply_markup=main_menu_keyboard(user_id)
    )


# =============================================================================
#  DOCUMENT UPLOAD HANDLER
# =============================================================================

@bot.message_handler(content_types=['document'])
def handle_document_upload(message: types.Message) -> None:
    """
    Handle uploaded Python files and ZIP archives.
    """
    user_id = message.from_user.id
    session = get_session(user_id)
    
    if session.get('state') != 'waiting_for_file':
        return
    
    # Check file extension
    file_name = message.document.file_name.lower()
    if not (is_python_file(file_name) or is_zip_file(file_name)):
        bot.reply_to(
            message,
            '❌ **Invalid File Type**\n\nOnly `.py` or `.zip` files are allowed.'
        )
        return
    
    # Check file size
    if message.document.file_size > Config.MAX_FILE_SIZE_MB * 1024 * 1024:
        bot.reply_to(
            message,
            f'❌ **File Too Large**\n\nMaximum size: {Config.MAX_FILE_SIZE_MB} MB'
        )
        return
    
    # Download file
    try:
        file_info = bot.get_file(message.document.file_id)
        downloaded = bot.download_file(file_info.file_path)
    except Exception as e:
        LOGGER.error(f'File download failed: {e}')
        bot.reply_to(message, '❌ Failed to download file. Please try again.')
        return
    
    # Secure filename and avoid duplicates
    safe_name = secure_filename(message.document.file_name)
    base, ext = os.path.splitext(safe_name)
    counter = 1
    while (Path(Config.PROJECT_DIR) / safe_name).exists():
        safe_name = f'{base}_{counter}{ext}'
        counter += 1
    
    file_path = Path(Config.PROJECT_DIR) / safe_name
    file_path.write_bytes(downloaded)
    
    # Handle ZIP extraction
    if is_zip_file(file_name):
        extract_dir = Path(Config.TEMP_DIR) / f'extract_{user_id}_{int(time.time())}'
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        main_py = extract_zip_and_get_main_py(file_path, extract_dir)
        
        if not main_py:
            shutil.rmtree(extract_dir, ignore_errors=True)
            file_path.unlink()
            bot.reply_to(message, '❌ No Python file found in the ZIP archive.')
            return
        
        # Rename to avoid conflicts
        new_name = secure_filename(main_py.name)
        counter = 1
        while (Path(Config.PROJECT_DIR) / new_name).exists():
            name, ext = os.path.splitext(main_py.name)
            new_name = f'{name}_{counter}{ext}'
            counter += 1
        
        shutil.copy2(main_py, Path(Config.PROJECT_DIR) / new_name)
        file_path.unlink()  # remove ZIP
        shutil.rmtree(extract_dir, ignore_errors=True)
        safe_name = new_name
    
    # Proceed to ask for bot name
    set_session(user_id, {
        'state': 'waiting_for_bot_name',
        'filename': safe_name,
        'original_name': message.document.file_name
    })
    
    bot.reply_to(
        message,
        '✅ **File Uploaded Successfully**\n\n'
        'Now send a **name** for your bot (max 30 characters).\n'
        'Type `/cancel` to abort.'
    )


@bot.message_handler(func=lambda m: get_session(m.from_user.id).get('state') == 'waiting_for_bot_name')
def process_bot_name(message: types.Message) -> None:
    """
    Receive bot name, insert into database, and offer next actions.
    """
    user_id = message.from_user.id
    session = get_session(user_id)
    
    if message.text.startswith('/'):
        bot.reply_to(message, '❌ Invalid name. Please send plain text.')
        return
    
    bot_name = message.text.strip()[:50]
    if not bot_name:
        bot.reply_to(message, '❌ Name cannot be empty.')
        return
    
    filename = session['filename']
    now = datetime.now().isoformat()
    
    # Insert deployment record
    execute_query(
        '''
        INSERT INTO deployments 
        (user_id, bot_name, filename, status, auto_restart, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        (user_id, bot_name, filename, 'Uploaded', 1, now, now),
        commit=True
    )
    
    # Increment user's bot count
    execute_query(
        'UPDATE users SET total_bots_deployed = total_bots_deployed + 1 WHERE id = ?',
        (user_id,),
        commit=True
    )
    
    clear_session(user_id)
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton('📚 Install Libraries', callback_data='install_libs'),
        types.InlineKeyboardButton('🚀 Deploy Now', callback_data='deploy_new'),
        types.InlineKeyboardButton('🌍 Make Public', callback_data='make_public'),
        types.InlineKeyboardButton('🤖 My Bots', callback_data='my_bots')
    )
    
    bot.reply_to(
        message,
        f'✅ **Bot Created Successfully**\n\n'
        f'**Name:** {bot_name}\n'
        f'**File:** `{session["original_name"]}`\n'
        f'**Status:** Ready for deployment\n\n'
        f'What would you like to do next?',
        reply_markup=markup
    )


# =============================================================================
#  CALLBACK QUERY HANDLER (INLINE BUTTONS)
# =============================================================================

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call: types.CallbackQuery) -> None:
    """
    Process all inline button callbacks.
    """
    user_id = call.from_user.id
    data = call.data
    message = call.message
    
    try:
        # ---------- Bot Actions ----------
        if data == 'my_bots':
            cmd_mybots(message)
            bot.answer_callback_query(call.id)
        
        elif data == 'upload':
            cmd_upload(message)
            bot.answer_callback_query(call.id)
        
        elif data == 'deploy_new':
            # Show list of undeployed/stopped bots
            files = execute_query(
                '''
                SELECT id, bot_name, filename FROM deployments
                WHERE user_id = ? AND (status = 'Uploaded' OR status = 'Stopped')
                ''',
                (user_id,),
                fetchall=True
            )
            
            if not files:
                bot.answer_callback_query(call.id, 'No bots available to deploy.')
                return
            
            markup = types.InlineKeyboardMarkup(row_width=1)
            for f in files:
                markup.add(
                    types.InlineKeyboardButton(
                        f'📁 {f["bot_name"]}',
                        callback_data=f'select_{f["id"]}'
                    )
                )
            
            edit_or_send(
                message.chat.id,
                message.message_id,
                '🚀 **Select a bot to deploy:**',
                reply_markup=markup
            )
            bot.answer_callback_query(call.id)
        
        elif data.startswith('select_'):
            bot_id = int(data.split('_')[1])
            bot_info = execute_query(
                'SELECT * FROM deployments WHERE id = ? AND user_id = ?',
                (bot_id, user_id),
                fetchone=True
            )
            
            if not bot_info:
                bot.answer_callback_query(call.id, 'Bot not found.')
                return
            
            success, msg = start_bot_process(bot_id, user_id, bot_info['filename'])
            
            if success:
                edit_or_send(
                    message.chat.id,
                    message.message_id,
                    f'✅ Bot **{bot_info["bot_name"]}** deployed successfully!'
                )
            else:
                edit_or_send(
                    message.chat.id,
                    message.message_id,
                    f'❌ Deployment failed:\n{msg}'
                )
            bot.answer_callback_query(call.id)
        
        elif data.startswith('bot_'):
            bot_id = int(data.split('_')[1])
            show_bot_details(call, bot_id)
        
        elif data.startswith('stop_'):
            bot_id = int(data.split('_')[1])
            stop_bot_process(bot_id, user_id)
            bot.answer_callback_query(call.id, 'Bot stopped.')
            show_bot_details(call, bot_id)
        
        elif data.startswith('restart_'):
            bot_id = int(data.split('_')[1])
            success, msg = restart_bot_process(bot_id, user_id)
            bot.answer_callback_query(call.id, 'Restarting...' if success else f'Error: {msg}')
            show_bot_details(call, bot_id)
        
        elif data.startswith('delete_'):
            bot_id = int(data.split('_')[1])
            markup = confirm_keyboard('delete', bot_id)
            edit_or_send(
                message.chat.id,
                message.message_id,
                f'⚠️ **Permanently delete this bot?**\n\nThis action cannot be undone.',
                reply_markup=markup
            )
            bot.answer_callback_query(call.id)
        
        elif data.startswith('confirm_delete_'):
            bot_id = int(data.split('_')[2])
            success = delete_bot_completely(bot_id, user_id)
            if success:
                edit_or_send(
                    message.chat.id,
                    message.message_id,
                    '✅ Bot deleted successfully.'
                )
                cmd_mybots(message)
            else:
                bot.answer_callback_query(call.id, '❌ Delete failed.')
        
        elif data.startswith('export_'):
            bot_id = int(data.split('_')[1])
            
            if not rate_limit('export', user_id):
                bot.answer_callback_query(call.id, '⏳ Rate limit. Try later.')
                return
            
            bot.answer_callback_query(call.id, '📦 Exporting...')
            zip_path = create_bot_export(bot_id, user_id)
            
            if zip_path:
                with open(zip_path, 'rb') as f:
                    bot.send_document(
                        user_id,
                        f,
                        caption=f'📦 **Bot Export**\nBot ID: {bot_id}'
                    )
                zip_path.unlink()
                increment_bot_download(bot_id)
            else:
                bot.send_message(user_id, '❌ Export failed.')
        
        elif data.startswith('logs_'):
            bot_id = int(data.split('_')[1])
            show_bot_logs(bot_id, message.chat.id)
            bot.answer_callback_query(call.id)
        
        elif data.startswith('autorestart_'):
            bot_id = int(data.split('_')[1])
            bot_info = execute_query(
                'SELECT auto_restart FROM deployments WHERE id = ?',
                (bot_id,),
                fetchone=True
            )
            if bot_info:
                new_state = 0 if bot_info['auto_restart'] == 1 else 1
                execute_query(
                    'UPDATE deployments SET auto_restart = ? WHERE id = ?',
                    (new_state, bot_id),
                    commit=True
                )
                state_text = 'enabled' if new_state else 'disabled'
                bot.answer_callback_query(call.id, f'Auto‑restart {state_text}.')
                show_bot_details(call, bot_id)
        
        elif data.startswith('stats_'):
            bot_id = int(data.split('_')[1])
            show_bot_stats(call, bot_id)
        
        # ---------- Public Bot Store ----------
        elif data.startswith('viewpublic_'):
            bot_id = int(data.split('_')[1])
            show_public_bot_details(call, bot_id)
        
        elif data == 'explore':
            cmd_explore(message)
            bot.answer_callback_query(call.id)
        
        elif data.startswith('explore_page_'):
            page = int(data.split('_')[2])
            bots = get_public_bots(page)
            
            if not bots:
                bot.answer_callback_query(call.id, 'No more bots.')
                return
            
            text = f'🌍 **Public Bot Store** (Page {page})\n━━━━━━━━━━━━━━━━━━━━\n'
            markup = types.InlineKeyboardMarkup(row_width=1)
            
            for bot_entry in bots:
                text += (
                    f'🤖 **{bot_entry["bot_name"]}**\n'
                    f'👤 @{bot_entry["username"] or "Unknown"}\n'
                    f'📝 {bot_entry["description"] or "No description"}\n'
                    f'🆔 `{bot_entry["id"]}`\n'
                    '━━━━━━━━━━━━━━━━━━━━\n'
                )
                markup.add(
                    types.InlineKeyboardButton(
                        f'View {bot_entry["bot_name"]}',
                        callback_data=f'viewpublic_{bot_entry["id"]}'
                    )
                )
            
            # Pagination
            total_public = execute_query(
                'SELECT COUNT(*) FROM deployments WHERE public_bot = 1',
                fetchone=True
            )[0]
            
            nav_buttons = []
            if page > 1:
                nav_buttons.append(
                    types.InlineKeyboardButton('⬅️ Previous', callback_data=f'explore_page_{page-1}')
                )
            if page * Config.PUBLIC_BOT_PAGE_SIZE < total_public:
                nav_buttons.append(
                    types.InlineKeyboardButton('Next ➡️', callback_data=f'explore_page_{page+1}')
                )
            
            if nav_buttons:
                markup.row(*nav_buttons)
            
            edit_or_send(
                message.chat.id,
                message.message_id,
                text,
                reply_markup=markup
            )
            bot.answer_callback_query(call.id)
        
        # ---------- Post‑Upload Actions ----------
        elif data == 'install_libs':
            set_session(user_id, {'state': 'waiting_for_libs'})
            bot.send_message(
                user_id,
                '📚 **Install Python Libraries**\n\n'
                'Send `pip install` commands, one per line.\n'
                'Example:\n'
                '```\npip install requests\npip install pyTelegramBotAPI\n```\n'
                'Type `/cancel` to abort.'
            )
            bot.answer_callback_query(call.id)
        
        elif data == 'make_public':
            set_session(user_id, {'state': 'waiting_for_public_bot_id'})
            bot.send_message(
                user_id,
                '🌍 **Make Bot Public**\n\n'
                'Enter the **ID** of the bot you want to make public.\n'
                'You can find the ID in /mybots.\n\n'
                'Type `/cancel` to abort.'
            )
            bot.answer_callback_query(call.id)
        
        # ---------- Admin Panel ----------
        elif data == 'admin_panel':
            if user_id == Config.ADMIN_ID:
                bot.send_message(
                    user_id,
                    '👑 **Admin Panel**',
                    reply_markup=admin_keyboard()
                )
            bot.answer_callback_query(call.id)
        
        else:
            bot.answer_callback_query(call.id, 'Unknown action.')
    
    except Exception as e:
        LOGGER.error(f'Callback error: {e}')
        bot.answer_callback_query(call.id, '⚠️ An error occurred.')


# =============================================================================
#  BOT DETAIL DISPLAY FUNCTIONS
# =============================================================================

def show_bot_details(call: types.CallbackQuery, bot_id: int) -> None:
    """
    Display detailed information about a specific bot.
    """
    bot_info = execute_query(
        'SELECT * FROM deployments WHERE id = ?',
        (bot_id,),
        fetchone=True
    )
    
    if not bot_info:
        bot.answer_callback_query(call.id, 'Bot not found.')
        return
    
    stats = None
    if bot_info['pid']:
        stats = get_process_stats(bot_info['pid'])
    
    status = bot_info['status']
    if stats:
        status = 'Running'
    elif bot_info['pid'] and not stats:
        status = 'Zombie'
    
    cpu = stats['cpu'] if stats else 0.0
    mem = stats['memory'] if stats else 0.0
    uptime = stats['uptime'] if stats else 'N/A'
    
    node = execute_query(
        'SELECT name FROM nodes WHERE id = ?',
        (bot_info['node_id'],),
        fetchone=True
    )
    node_name = node['name'] if node else 'Not assigned'
    
    # Fetch recent events
    recent_events = execute_query(
        'SELECT log_type, timestamp FROM bot_logs WHERE bot_id = ? ORDER BY id DESC LIMIT 3',
        (bot_id,),
        fetchall=True
    ) or []
    
    events_text = ''
    for ev in recent_events:
        events_text += f'• {ev["timestamp"][11:19]} - {ev["log_type"]}\n'
    
    text = (
        f'🤖 **Bot Details**\n'
        f'━━━━━━━━━━━━━━━━━━━━\n'
        f'**Name:** {bot_info["bot_name"]}\n'
        f'**ID:** `{bot_id}`\n'
        f'**File:** `{bot_info["filename"]}`\n'
        f'**Status:** {status}\n'
        f'**Node:** {node_name}\n'
        f'**CPU:** {cpu:.1f}%  **RAM:** {mem:.1f}%\n'
        f'**Uptime:** {uptime}\n'
        f'**Restarts:** {bot_info["restart_count"]}\n'
        f'**Auto‑restart:** {"✅" if bot_info["auto_restart"] else "❌"}\n'
        f'**Public:** {"🌍 Yes" if bot_info["public_bot"] else "🔒 No"}\n'
        f'**Created:** {bot_info["created_at"][:19]}\n'
        f'━━━━━━━━━━━━━━━━━━━━\n'
        f'**Recent Events:**\n{events_text or "None"}'
    )
    
    edit_or_send(
        call.message.chat.id,
        call.message.message_id,
        text,
        reply_markup=bot_actions_keyboard(bot_id)
    )
    
    bot.answer_callback_query(call.id)


def show_bot_logs(bot_id: int, chat_id: int) -> None:
    """
    Send the last 2000 characters of the bot's log file.
    """
    log_file = Path(Config.LOGS_DIR) / f'bot_{bot_id}.log'
    
    if not log_file.exists():
        bot.send_message(chat_id, '📜 No logs available for this bot.')
        return
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            logs = f.read()[-2000:]
        
        bot_name = execute_query(
            'SELECT bot_name FROM deployments WHERE id = ?',
            (bot_id,),
            fetchone=True
        )
        name = bot_name['bot_name'] if bot_name else f'Bot_{bot_id}'
        
        bot.send_message(
            chat_id,
            f'📜 **Logs for {name}**\n```\n{logs}\n```',
            parse_mode='Markdown'
        )
    except Exception as e:
        bot.send_message(chat_id, f'❌ Error reading logs: {e}')


def show_bot_stats(call: types.CallbackQuery, bot_id: int) -> None:
    """
    Display detailed statistics and event history for a bot.
    """
    bot_info = execute_query(
        'SELECT * FROM deployments WHERE id = ?',
        (bot_id,),
        fetchone=True
    )
    
    if not bot_info:
        bot.answer_callback_query(call.id, 'Bot not found.')
        return
    
    events = execute_query(
        'SELECT * FROM bot_logs WHERE bot_id = ? ORDER BY id DESC LIMIT 15',
        (bot_id,),
        fetchall=True
    ) or []
    
    text = (
        f'📊 **Bot Statistics**\n'
        f'━━━━━━━━━━━━━━━━━━━━\n'
        f'🤖 **{bot_info["bot_name"]}** (ID: {bot_id})\n'
        f'📁 File: `{bot_info["filename"]}`\n'
        f'📊 Status: {bot_info["status"]}\n'
        f'🔄 Restarts: {bot_info["restart_count"]}\n'
        f'🔁 Auto‑restart: {"Yes" if bot_info["auto_restart"] else "No"}\n'
        f'📅 Created: {bot_info["created_at"][:19]}\n'
        f'🕒 Last active: {bot_info["last_active"][:19] if bot_info["last_active"] else "Never"}\n'
        f'━━━━━━━━━━━━━━━━━━━━\n'
        f'📜 **Event History**\n'
    )
    
    if events:
        for ev in events[:10]:
            text += f'• {ev["timestamp"][:19]} - {ev["log_type"]}: {ev["message"][:50]}\n'
    else:
        text += 'No events recorded.\n'
    
    bot.send_message(call.message.chat.id, text)
    bot.answer_callback_query(call.id)


def show_public_bot_details(call: types.CallbackQuery, bot_id: int) -> None:
    """
    Display public bot information and increment view counter.
    """
    bot_info = execute_query(
        '''
        SELECT d.*, u.username
        FROM deployments d
        LEFT JOIN users u ON d.user_id = u.id
        WHERE d.id = ? AND d.public_bot = 1
        ''',
        (bot_id,),
        fetchone=True
    )
    
    if not bot_info:
        bot.answer_callback_query(call.id, 'Bot not public or not found.')
        return
    
    # Increment view count
    increment_bot_view(bot_id)
    
    # Get stats
    stats = execute_query(
        'SELECT views, downloads FROM bot_stats WHERE bot_id = ?',
        (bot_id,),
        fetchone=True
    ) or {'views': 0, 'downloads': 0}
    
    text = (
        f'🌍 **Public Bot**\n'
        f'━━━━━━━━━━━━━━━━━━━━\n'
        f'🤖 **{bot_info["bot_name"]}**\n'
        f'👤 **Owner:** @{bot_info["username"] or "Unknown"}\n'
        f'📝 **Description:** {bot_info["description"] or "No description"}\n'
        f'🏷️ **Tags:** {bot_info["tags"] or "None"}\n'
        f'📅 **Created:** {bot_info["created_at"][:19]}\n'
        f'👁️ **Views:** {stats["views"]}\n'
        f'📥 **Downloads:** {stats["downloads"]}\n'
        f'━━━━━━━━━━━━━━━━━━━━\n'
        f'You can export this bot using the button below.'
    )
    
    edit_or_send(
        call.message.chat.id,
        call.message.message_id,
        text,
        reply_markup=public_bot_keyboard(bot_id)
    )
    
    bot.answer_callback_query(call.id)


# =============================================================================
#  REPLY DELETION FEATURE
# =============================================================================

@bot.message_handler(func=lambda m: m.reply_to_message is not None and m.text == '/delete')
def handle_delete_reply(message: types.Message) -> None:
    """
    When a user replies to a bot message with /delete, delete both messages.
    """
    chat_id = message.chat.id
    user_id = message.from_user.id
    reply_msg = message.reply_to_message
    
    # Check if the replied message was sent by the bot
    if reply_msg.from_user.id != bot.get_me().id:
        bot.reply_to(message, '❌ You can only delete messages sent by me.')
        return
    
    # Delete the user's command message and the bot's message
    try:
        bot.delete_message(chat_id, reply_msg.message_id)
        bot.delete_message(chat_id, message.message_id)
    except Exception as e:
        LOGGER.warning(f'Delete reply failed: {e}')
        bot.reply_to(message, '❌ Could not delete messages. Insufficient permissions.')


# =============================================================================
#  TEXT MESSAGE HANDLER (KEYBOARD BUTTONS)
# =============================================================================

@bot.message_handler(func=lambda message: True)
def handle_text_messages(message: types.Message) -> None:
    """
    Process all text messages, including keyboard button presses.
    """
    user_id = message.from_user.id
    text = message.text.strip()
    
    # Update user activity
    update_user_activity(user_id)
    
    # ---------- Check for session states ----------
    session = get_session(user_id)
    state = session.get('state')
    
    if state == 'waiting_for_libs':
        # Process pip install commands
        if text.lower() == '/cancel':
            clear_session(user_id)
            bot.reply_to(message, '❌ Installation cancelled.', reply_markup=main_menu_keyboard(user_id))
            return
        
        commands = [line.strip() for line in text.split('\n') if line.strip()]
        results = []
        
        for cmd in commands:
            if 'pip install' in cmd or 'pip3 install' in cmd:
                try:
                    proc = subprocess.run(
                        cmd.split(),
                        capture_output=True,
                        text=True,
                        timeout=120
                    )
                    if proc.returncode == 0:
                        results.append(f'✅ {cmd}')
                    else:
                        results.append(f'❌ {cmd}\n   {proc.stderr[:100]}')
                except subprocess.TimeoutExpired:
                    results.append(f'⏰ {cmd} (timeout)')
                except Exception as e:
                    results.append(f'⚠️ {cmd} (error: {str(e)[:50]})')
            else:
                results.append(f'⚠️ {cmd} (skipped, not a pip command)')
        
        result_text = '\n'.join(results)
        bot.reply_to(
            message,
            f'📚 **Installation Results**\n\n{result_text}',
            reply_markup=main_menu_keyboard(user_id)
        )
        clear_session(user_id)
        return
    
    elif state == 'waiting_for_public_bot_id':
        if text.lower() == '/cancel':
            clear_session(user_id)
            bot.reply_to(message, '❌ Cancelled.', reply_markup=main_menu_keyboard(user_id))
            return
        
        try:
            bot_id = int(text)
        except ValueError:
            bot.reply_to(message, '❌ Invalid bot ID. Must be a number.')
            return
        
        # Verify ownership
        bot_record = execute_query(
            'SELECT bot_name FROM deployments WHERE id = ? AND user_id = ?',
            (bot_id, user_id),
            fetchone=True
        )
        
        if not bot_record:
            bot.reply_to(message, '❌ Bot not found or you do not own it.')
            return
        
        set_session(user_id, {'state': 'waiting_for_public_description', 'bot_id': bot_id})
        bot.reply_to(
            message,
            f'🌍 **Make Bot Public**\n\n'
            f'Bot: **{bot_record["bot_name"]}** (ID: {bot_id})\n\n'
            f'Please enter a short **description** for this bot (max 200 chars).\n'
            f'Type `/cancel` to abort.'
        )
        return
    
    elif state == 'waiting_for_public_description':
        if text.lower() == '/cancel':
            clear_session(user_id)
            bot.reply_to(message, '❌ Cancelled.', reply_markup=main_menu_keyboard(user_id))
            return
        
        description = text.strip()[:200]
        bot_id = session.get('bot_id')
        
        set_session(user_id, {'state': 'waiting_for_public_tags', 'bot_id': bot_id, 'description': description})
        bot.reply_to(
            message,
            '🏷️ **Tags** (optional)\n\n'
            'Enter comma‑separated tags, e.g.: `music, download, utility`\n'
            'Or send `-` to skip.\n\n'
            'Type `/cancel` to abort.'
        )
        return
    
    elif state == 'waiting_for_public_tags':
        if text.lower() == '/cancel':
            clear_session(user_id)
            bot.reply_to(message, '❌ Cancelled.', reply_markup=main_menu_keyboard(user_id))
            return
        
        tags = '' if text == '-' else text.strip()[:100]
        bot_id = session.get('bot_id')
        description = session.get('description', '')
        
        success = set_bot_public(bot_id, user_id, True, description, tags)
        
        if success:
            bot.reply_to(
                message,
                f'🌍 **Bot is now public!**\n\n'
                f'Anyone can view and export your bot from the Public Store.\n'
                f'Description: {description or "None"}\n'
                f'Tags: {tags or "None"}',
                reply_markup=main_menu_keyboard(user_id)
            )
        else:
            bot.reply_to(message, '❌ Failed to make bot public.')
        
        clear_session(user_id)
        return
    
    # ---------- Admin key generation states ----------
    if state == 'admin_gen_days' and user_id == Config.ADMIN_ID:
        if text.lower() == '/cancel':
            clear_session(user_id)
            bot.reply_to(message, '❌ Cancelled.', reply_markup=admin_keyboard())
            return
        
        try:
            days = int(text)
            if days <= 0:
                raise ValueError
        except ValueError:
            bot.reply_to(message, '❌ Please enter a positive number.')
            return
        
        set_session(user_id, {'state': 'admin_gen_limit', 'days': days})
        bot.reply_to(message, f'⏰ Duration set to **{days} days**.\n\nNow enter **file limit** (1‑100):')
        return
    
    elif state == 'admin_gen_limit' and user_id == Config.ADMIN_ID:
        if text.lower() == '/cancel':
            clear_session(user_id)
            bot.reply_to(message, '❌ Cancelled.', reply_markup=admin_keyboard())
            return
        
        try:
            limit = int(text)
            if limit < 1 or limit > 100:
                raise ValueError
        except ValueError:
            bot.reply_to(message, '❌ Please enter a number between 1 and 100.')
            return
        
        days = session.get('days', 30)
        key = generate_prime_key(days, limit, user_id)
        
        bot.reply_to(
            message,
            f'✅ **Prime Key Generated**\n\n'
            f'🔑 **Key:** `{key}`\n'
            f'⏰ **Duration:** {days} days\n'
            f'📦 **File Limit:** {limit}\n\n'
            f'Share this key with the user.',
            reply_markup=admin_keyboard()
        )
        clear_session(user_id)
        return
    
    elif state == 'admin_broadcast' and user_id == Config.ADMIN_ID:
        if text.lower() == '/cancel':
            clear_session(user_id)
            bot.reply_to(message, '❌ Cancelled.', reply_markup=admin_keyboard())
            return
        
        # Trigger broadcast
        users = execute_query('SELECT id FROM users', fetchall=True) or []
        success = 0
        fail = 0
        
        status_msg = bot.reply_to(
            message,
            f'📢 **Broadcasting...**\n\nTotal: {len(users)}\nProgress: 0%'
        )
        
        for i, user in enumerate(users):
            try:
                bot.send_message(
                    user['id'],
                    f'📢 **Broadcast Message**\n\n{text}'
                )
                success += 1
            except:
                fail += 1
            
            if i % 10 == 0 and i > 0:
                percent = int((i+1)/len(users)*100)
                try:
                    bot.edit_message_text(
                        f'📢 **Broadcasting...**\n\nTotal: {len(users)}\nProgress: {percent}%',
                        status_msg.chat.id,
                        status_msg.message_id
                    )
                except:
                    pass
        
        bot.edit_message_text(
            f'✅ **Broadcast Complete**\n\n'
            f'Total: {len(users)}\n'
            f'✅ Success: {success}\n'
            f'❌ Failed: {fail}',
            status_msg.chat.id,
            status_msg.message_id
        )
        clear_session(user_id)
        return
    
    # ---------- Main Menu Button Handlers ----------
    if text == '📤 Upload Bot':
        cmd_upload(message)
    elif text == '🤖 My Bots':
        cmd_mybots(message)
    elif text == '🚀 Deploy Bot':
        # Show quick deploy list
        files = execute_query(
            'SELECT id, bot_name FROM deployments WHERE user_id = ? AND (status = ? OR status = ?)',
            (user_id, 'Uploaded', 'Stopped'),
            fetchall=True
        )
        if not files:
            bot.reply_to(message, '📭 No bots available to deploy. Upload one first.')
            return
        
        markup = types.InlineKeyboardMarkup(row_width=1)
        for f in files:
            markup.add(
                types.InlineKeyboardButton(f'📁 {f["bot_name"]}', callback_data=f'select_{f["id"]}')
            )
        bot.send_message(user_id, '🚀 **Select a bot to deploy:**', reply_markup=markup)
    
    elif text == '📊 Dashboard':
        cmd_dashboard(message)
    
    elif text == '⚙️ Settings':
        # Simple settings menu
        prime = check_prime_status(user_id)
        user = get_user(user_id)
        
        text_msg = (
            '⚙️ **Settings**\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            f'👤 **User ID:** `{user_id}`\n'
            f'💎 **Prime:** {"Active" if not prime["expired"] else "Inactive"}\n'
            f'📦 **File Limit:** {user["file_limit"] if user else 1}\n'
            f'🌐 **Language:** English\n'
            f'━━━━━━━━━━━━━━━━━━━━\n'
            f'Notifications and other settings can be configured via commands.'
        )
        bot.send_message(user_id, text_msg)
    
    elif text == '👑 Prime Info':
        prime = check_prime_status(user_id)
        if not prime['expired']:
            text_msg = (
                '👑 **Prime Membership**\n'
                '━━━━━━━━━━━━━━━━━━━━\n'
                f'✅ **Status:** Active\n'
                f'📅 **Expires:** {prime["expiry_date"][:19]}\n'
                f'⏳ **Days left:** {prime["days_left"]}\n'
                f'📦 **File limit:** {prime["file_limit"]}\n'
                '━━━━━━━━━━━━━━━━━━━━\n'
                'Thank you for supporting ZEN X HOST!'
            )
        else:
            text_msg = (
                '👑 **Prime Membership**\n'
                '━━━━━━━━━━━━━━━━━━━━\n'
                '❌ **Status:** Inactive\n\n'
                '**Benefits:**\n'
                '• Upload up to 10 bots\n'
                '• Deploy multiple bots simultaneously\n'
                '• Auto‑restart on crash\n'
                '• Priority support\n\n'
                'Contact @{Config.ADMIN_USERNAME} to purchase a key.'
            )
        bot.send_message(user_id, text_msg)
    
    elif text == '🔔 Notifications':
        notifications = get_unread_notifications(user_id)
        if not notifications:
            bot.send_message(user_id, '🔔 **No new notifications.**')
        else:
            text_msg = '🔔 **Notifications**\n━━━━━━━━━━━━━━━━━━━━\n'
            for n in notifications[:10]:
                text_msg += f'• {n["created_at"][:19]}\n{n["message"]}\n━━━━━━━━━━━━━━━━━━━━\n'
            bot.send_message(user_id, text_msg)
    
    elif text == '🌍 Explore Bots':
        cmd_explore(message)
    
    elif text == '🔑 Activate Prime':
        bot.reply_to(
            message,
            '🔑 **Activate Prime**\n\n'
            'Please enter your activation key.\n'
            'Format: `ZENX-XXXXXXXXXXXX`\n\n'
            'Type `/activate <key>` directly or send the key now.'
        )
        set_session(user_id, {'state': 'waiting_for_key'})
    
    elif text == '📞 Contact Admin':
        bot.reply_to(
            message,
            f'📞 **Contact Admin**\n\n'
            f'👤 **Admin:** @{Config.ADMIN_USERNAME}\n'
            f'🤖 **Bot:** @{Config.BOT_USERNAME}\n\n'
            f'For support, bug reports, or premium purchase.'
        )
    
    elif text == 'ℹ️ Help':
        bot.reply_to(
            message,
            'ℹ️ **Help & Commands**\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '**General Commands:**\n'
            '/start - Main menu\n'
            '/upload - Upload a bot\n'
            '/mybots - List your bots\n'
            '/deploy <id> - Deploy a bot\n'
            '/stop <id> - Stop a bot\n'
            '/restart <id> - Restart a bot\n'
            '/delete <id> - Delete a bot\n'
            '/export <id> - Export bot files\n'
            '/logs <id> - View bot logs\n'
            '/explore - Public bot store\n'
            '/dashboard - System stats\n'
            '/activate <key> - Activate Prime\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '**Admin Commands:**\n'
            '/admin - Open admin panel\n'
            '/backup - Full system backup\n'
            '/broadcast - Message all users\n'
            '/node - Manage nodes\n'
            '/clean <n> - Delete last n messages\n'
            '━━━━━━━━━━━━━━━━━━━━'
        )
    
    elif text == '👑 Admin Panel' and user_id == Config.ADMIN_ID:
        bot.send_message(user_id, '👑 **Admin Panel**', reply_markup=admin_keyboard())
    
    elif text == '🏠 Main Menu':
        cmd_start(message)
    
    # ---------- Admin Panel Button Handlers ----------
    elif user_id == Config.ADMIN_ID:
        if text == '🎫 Generate Key':
            set_session(user_id, {'state': 'admin_gen_days'})
            bot.reply_to(message, '🎫 **Generate Prime Key**\n\nEnter duration in days:')
        
        elif text == '👥 All Users':
            users = execute_query(
                'SELECT id, username, is_prime, expiry, total_bots_deployed, join_date '
                'FROM users ORDER BY id DESC LIMIT 20',
                fetchall=True
            ) or []
            text_msg = '👥 **Recent Users**\n━━━━━━━━━━━━━━━━━━━━\n'
            for u in users:
                prime_icon = '👑' if u['is_prime'] else '🆓'
                expiry = u['expiry'][:10] if u['expiry'] else 'None'
                text_msg += (
                    f'{prime_icon} **{u["username"] or u["id"]}** (ID: {u["id"]})\n'
                    f'Bots: {u["total_bots_deployed"]} | Exp: {expiry}\n'
                    '━━━━━━━━━━━━━━━━━━━━\n'
                )
            bot.send_message(user_id, text_msg)
        
        elif text == '🤖 All Bots':
            bots = execute_query(
                '''
                SELECT d.id, d.bot_name, d.status, u.username, d.auto_restart, d.node_id
                FROM deployments d
                LEFT JOIN users u ON d.user_id = u.id
                ORDER BY d.id DESC LIMIT 20
                ''',
                fetchall=True
            ) or []
            text_msg = '🤖 **All Bots**\n━━━━━━━━━━━━━━━━━━━━\n'
            for b in bots:
                icon = '🟢' if b['status'] == 'Running' else '🔴'
                auto = '🔁' if b['auto_restart'] else '⏸️'
                node = b['node_id'] or 'None'
                text_msg += (
                    f'{icon}{auto} **{b["bot_name"]}** (ID: {b["id"]})\n'
                    f'👤 @{b["username"] or "Unknown"} | Node: {node}\n'
                    '━━━━━━━━━━━━━━━━━━━━\n'
                )
            bot.send_message(user_id, text_msg)
        
        elif text == '📈 Statistics':
            stats = get_system_stats()
            text_msg = (
                f'📈 **System Statistics**\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'👥 **Users:** {stats["total_users"]} (👑 {stats["prime_users"]})\n'
                f'🤖 **Bots:** {stats["total_bots"]} (🟢 {stats["running_bots"]})\n'
                f'📦 **Deployed today:** {stats["deployed_today"]}\n'
                f'💾 **Backups:** {stats["backup_count"]} (last: {stats["last_backup"][:19]})\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'🖥️ **Resources**\n'
                f'• CPU: {create_progress_bar(stats["cpu_percent"])} {stats["cpu_percent"]:.1f}%\n'
                f'• RAM: {create_progress_bar(stats["ram_percent"])} {stats["ram_percent"]:.1f}%\n'
                f'• Disk: {create_progress_bar(stats["disk_percent"])} {stats["disk_percent"]:.1f}%\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'🌐 **Nodes**\n'
                f'• Active: {stats["active_nodes"]}/{stats["total_nodes"]}\n'
                f'• Capacity: {stats["used_capacity"]}/{stats["total_capacity"]}\n'
                f'• Crash rate: {stats["crash_rate"]:.2f}/min\n'
                f'━━━━━━━━━━━━━━━━━━━━'
            )
            bot.send_message(user_id, text_msg)
        
        elif text == '🗄️ Database':
            view_database_admin(message)
        
        elif text == '💾 Backup':
            cmd_backup(message)
        
        elif text == '⚙️ Maintenance':
            Config.MAINTENANCE = not Config.MAINTENANCE
            status = 'ENABLED' if Config.MAINTENANCE else 'DISABLED'
            bot.reply_to(message, f'⚙️ Maintenance mode **{status}**.')
            log_event('MAINTENANCE', f'Mode {status}', user_id)
        
        elif text == '🌐 Nodes':
            nodes = get_all_nodes()
            text_msg = '🌐 **Node Status**\n━━━━━━━━━━━━━━━━━━━━\n'
            for n in nodes:
                load_percent = (n['current_load'] / n['capacity']) * 100 if n['capacity'] > 0 else 0
                bar = create_progress_bar(load_percent)
                status_icon = '🟢' if n['status'] == 'active' else ('🟡' if n['status'] == 'maintenance' else '🔴')
                text_msg += (
                    f'{status_icon} **{n["name"]}** (ID: {n["id"]})\n'
                    f'Region: {n["region"]} | Status: {n["status"]}\n'
                    f'Load: {bar} {load_percent:.1f}% ({n["current_load"]}/{n["capacity"]})\n'
                    f'Deployed: {n["total_deployed"]}\n'
                    '━━━━━━━━━━━━━━━━━━━━\n'
                )
            bot.send_message(user_id, text_msg)
        
        elif text == '🔧 Logs':
            logs = execute_query(
                'SELECT * FROM server_logs ORDER BY id DESC LIMIT 15',
                fetchall=True
            ) or []
            text_msg = '🔧 **Server Logs**\n━━━━━━━━━━━━━━━━━━━━\n'
            for l in logs:
                text_msg += (
                    f'{l["timestamp"][:19]} | {l["severity"]}\n'
                    f'**{l["event"]}**\n`{truncate(l["details"], 100)}`\n'
                    '━━━━━━━━━━━━━━━━━━━━\n'
                )
            bot.send_message(user_id, text_msg)
        
        elif text == '📊 System Info':
            stats = get_system_stats()
            text_msg = (
                f'📊 **System Information**\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'🖥️ **Platform:** {stats["platform"]}\n'
                f'🐍 **Python:** {stats["python_version"]}\n'
                f'⏱️ **Uptime:** {format_uptime(stats["uptime_seconds"])}\n'
                f'📦 **Bot version:** 5.0\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'💾 **Storage**\n'
                f'• Projects: {format_size(sum(f.stat().st_size for f in Path(Config.PROJECT_DIR).glob("*")) if Path(Config.PROJECT_DIR).exists() else 0)}\n'
                f'• Logs: {format_size(sum(f.stat().st_size for f in Path(Config.LOGS_DIR).glob("*")) if Path(Config.LOGS_DIR).exists() else 0)}\n'
                f'• Backups: {stats["backup_count"]} files\n'
                f'━━━━━━━━━━━━━━━━━━━━\n'
                f'⚙️ **Configuration**\n'
                f'• Max bots/user: {Config.MAX_BOTS_PER_USER}\n'
                f'• Max concurrent: {Config.MAX_CONCURRENT_DEPLOYMENTS}\n'
                f'• Auto‑restart: {Config.AUTO_RESTART_BOTS}\n'
                f'• Auto‑backup thresholds: {Config.AUTO_BACKUP_THRESHOLDS}\n'
                f'━━━━━━━━━━━━━━━━━━━━'
            )
            bot.send_message(user_id, text_msg)
        
        elif text == '🔔 Broadcast':
            set_session(user_id, {'state': 'admin_broadcast'})
            bot.reply_to(message, '📢 **Broadcast Message**\n\nEnter the message to send to all users:')
        
        elif text == '🔄 Cleanup':
            cleanup_system_admin(message)
        
        elif text == '🏠 Main Menu':
            cmd_start(message)
    
    else:
        # Unknown text – show help
        bot.reply_to(
            message,
            '❓ Unknown command. Use /help to see available commands.',
            reply_markup=main_menu_keyboard(user_id)
        )


# =============================================================================
#  ADMIN HELPER FUNCTIONS
# =============================================================================

def view_database_admin(message: types.Message) -> None:
    """Display paginated list of all deployments."""
    page = 1
    per_page = 5
    
    total = execute_query('SELECT COUNT(*) FROM deployments', fetchone=True)[0]
    pages = (total + per_page - 1) // per_page
    
    deployments = execute_query(
        '''
        SELECT d.id, d.bot_name, d.status, u.username, d.created_at, d.node_id
        FROM deployments d
        LEFT JOIN users u ON d.user_id = u.id
        ORDER BY d.id DESC
        LIMIT ? OFFSET ?
        ''',
        (per_page, (page - 1) * per_page),
        fetchall=True
    ) or []
    
    text = f'🗄️ **Database** (Page {page}/{pages})\n━━━━━━━━━━━━━━━━━━━━\n'
    for d in deployments:
        node = d['node_id'] or 'None'
        text += (
            f'ID: {d["id"]} | {d["bot_name"]}\n'
            f'Status: {d["status"]} | Node: {node}\n'
            f'Owner: @{d["username"] or "Unknown"}\n'
            f'Created: {d["created_at"][:19]}\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
        )
    
    markup = types.InlineKeyboardMarkup()
    if page < pages:
        markup.add(types.InlineKeyboardButton('Next ➡️', callback_data='db_page_2'))
    
    bot.send_message(message.chat.id, text, reply_markup=markup)


def cleanup_system_admin(message: types.Message) -> None:
    """Remove old export files, temp folders, and old logs."""
    user_id = message.from_user.id
    
    # Clean exports older than 1 day
    export_count = 0
    for f in Path(Config.EXPORTS_DIR).glob('*.zip'):
        if (datetime.now() - datetime.fromtimestamp(f.stat().st_mtime)).days > 1:
            f.unlink()
            export_count += 1
    
    # Clean temp directories
    temp_count = 0
    for f in Path(Config.TEMP_DIR).glob('*'):
        if f.is_dir():
            shutil.rmtree(f, ignore_errors=True)
            temp_count += 1
        else:
            f.unlink()
            temp_count += 1
    
    # Clean logs older than 14 days
    log_count = 0
    for f in Path(Config.LOGS_DIR).glob('*.log'):
        if (datetime.now() - datetime.fromtimestamp(f.stat().st_mtime)).days > 14:
            f.unlink()
            log_count += 1
    
    bot.reply_to(
        message,
        f'✅ **Cleanup Completed**\n\n'
        f'• Exports removed: {export_count}\n'
        f'• Temp files removed: {temp_count}\n'
        f'• Old logs removed: {log_count}'
    )
    log_event('CLEANUP', f'Removed {export_count} exports, {temp_count} temp, {log_count} logs', user_id)


# =============================================================================
#  BACKGROUND THREADS
# =============================================================================

def start_background_services() -> None:
    """Launch all background threads."""
    threads = [
        threading.Thread(target=health_check_worker, daemon=True),
        threading.Thread(target=auto_backup_scheduler, daemon=True),
        threading.Thread(target=cleanup_old_sessions, daemon=True)
    ]
    
    for t in threads:
        t.start()
        LOGGER.info(f'Started background thread: {t.name}')
    
    LOGGER.info('All background services running.')


# =============================================================================
#  FLASK WEB SERVER (HEALTH CHECKS)
# =============================================================================

@app.route('/')
def index():
    return 'ZEN X HOST BOT v5.0 - Online'

@app.route('/health')
def health_check():
    stats = get_system_stats()
    return jsonify({
        'status': 'ok',
        'version': '5.0',
        'timestamp': datetime.now().isoformat(),
        'stats': stats
    })

@app.route('/stats')
def stats_endpoint():
    stats = get_system_stats()
    return jsonify(stats)


# =============================================================================
#  MAIN ENTRY POINT
# =============================================================================

def main() -> None:
    """Initialize everything and start the bot."""
    LOGGER.info('=' * 60)
    LOGGER.info(' ZEN X HOST BOT v5.0 - Starting...')
    LOGGER.info('=' * 60)
    
    # Initialize database
    init_database()
    
    # Recover previously running bots
    recover_deployments()
    
    # Start background threads
    start_background_services()
    
    # Start Flask web server in a daemon thread
    flask_thread = threading.Thread(
        target=lambda: app.run(
            host='0.0.0.0',
            port=Config.PORT,
            debug=False,
            use_reloader=False,
            threaded=True
        ),
        daemon=True
    )
    flask_thread.start()
    LOGGER.info(f'Flask server started on port {Config.PORT}')
    
    # Notify admin that bot is online
    try:
        bot.send_message(
            Config.ADMIN_ID,
            f'🚀 **ZEN X HOST BOT v5.0 started**\n\n'
            f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
            f'🖥️ Platform: {platform.system()}\n'
            f'🐍 Python: {platform.python_version()}'
        )
    except Exception as e:
        LOGGER.error(f'Failed to send startup notification: {e}')
    
    # Start polling (with auto-reconnect)
    LOGGER.info('Bot is now polling for updates...')
    
    while True:
        try:
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=60)
        except Exception as e:
            LOGGER.error(f'Polling error: {e}')
            time.sleep(10)


if __name__ == '__main__':
    main()
