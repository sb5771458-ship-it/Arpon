#!/usr/bin/env python3
"""
================================================================================
  ARPONTOLS — Enterprise Digital Products Selling Platform (Telegram Bot)
  File: clainds.py
  Stack: Python 3.10+, aiogram 3.x, aiohttp, sqlite3 (WAL mode)
================================================================================

FEATURES INCLUDED:
  1. Customer Storefront:
     - /start Main Menu, /shop Brand Folders & Paginated Catalog, /search,
       /orders (Receipts & Re-view Credentials), /balance (Multi-Gateway Top-Up),
       /tickets (Text + Screenshot Support System), Developer API for Resellers,
       Referral Program & Earnings Transfer, Stock Watcher Restock Alerts.
  2. Upstream Wholesale Auto-Purchase Engine:
     - Connect ANY upstream wholesale Telegram bot / REST Supplier API
       (DigiHub / StackVault / generic REST compatible).
     - Automatic catalog sync, brand grouping, live reseller balance checks,
       automated profit margin calculation, instant upstream auto-purchase on
       customer order, and automatic atomic refund if upstream fails.
  3. Multi-Gateway Payment Engine:
     - Binance Pay (API + Manual Pay ID fallback)
     - USDT BEP-20 (Auto-Detect via BSCScan / Micro-Amount matching)
     - CryptoBot (@CryptoBot Crypto Pay API)
     - NOWPayments API
     - bKash / Nagad Manual Deposit (BDT->USD rate + TrxID + 1-Click Admin Approval)
  4. Full Telegram Admin Panel (/admin) with Role-Based Access Control (RBAC):
     - Catalog & Margins, Custom Local Products & Bulk Stock, Orders History &
       Refunds, Deposits Approval Log, Users Management, Support Tickets,
       Promo Codes / Coupons, Broadcast, Payment Gateways Config, Store Settings,
       Analytics & CSV Export, Team & Staff RBAC Scopes, Force-Sub Hub,
       Stock Watcher, External Suppliers API Manager, Support Settings,
       Referrals, Maintenance Mode, Wholesale Credit Monitor.
  5. Easy API Key Configuration (3 Ways):
     - Edit DEFAULT_CONFIG below or set environment variables (.env supported)
     - Interactive First-Run Terminal Setup Wizard (auto-saves to bot_config.json)
     - Live In-Bot Admin Panel (/admin -> External Suppliers / Payment Gateways)
  6. Built-in Reseller / Website REST API (Optional aiohttp server):
     - Ready to connect your future Website & Resellers to the same database!

QUICK START:
  pip install aiogram aiohttp
  python clainds.py
================================================================================
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import hmac
import html
import io
import json
import logging
import os
import random
import re
import secrets
import sqlite3
import string
import sys
import time
import urllib.parse
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

# ------------------------------------------------------------------------------
# Optional Dependency Guard (allows importing & unit-testing core modules even
# before aiogram/aiohttp are installed, while clearly guiding setup on run)
# ------------------------------------------------------------------------------
try:
    import aiohttp
    from aiohttp import web
except ImportError:
    aiohttp = None  # type: ignore
    web = None  # type: ignore

try:
    from aiogram import Bot, Dispatcher, F, Router
    from aiogram.client.default import DefaultBotProperties
    from aiogram.enums import ParseMode
    from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
    from aiogram.filters import Command, CommandObject, CommandStart
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.fsm.storage.memory import MemoryStorage
    from aiogram.types import (
        BotCommand,
        BotCommandScopeChat,
        BotCommandScopeDefault,
        BufferedInputFile,
        CallbackQuery,
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        Message,
    )
    AIOGRAM_AVAILABLE = True
except ImportError:
    AIOGRAM_AVAILABLE = False

# ==============================================================================
# 1. CONFIGURATION & API KEY MANAGEMENT
# ==============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = BASE_DIR / "bot_config.json"
ENV_FILE = BASE_DIR / ".env"
DB_FILE = BASE_DIR / "arpontols_store.db"

# You can fill in your API keys directly here, in .env, in bot_config.json,
# via the first-run interactive terminal wizard, OR live inside /admin!
DEFAULT_CONFIG: Dict[str, Any] = {
    "BOT_TOKEN": "9163998885:AAEIniYDdaAT-NSCsrZH3d8-f7H4ltYEC_M",  # Telegram Bot Token from @BotFather
    "OWNER_ID": 8606975760,                       # Your Telegram numeric User ID (from @userinfobot)
    "STORE_NAME": "Arpontols",           # Your Store / Brand Name
    "OWNER_USERNAME": "@Arponsuport",    # Owner Telegram @username (auto-binds Admin on first /start or /admin)
    "SUPPORT_USERNAME": "@Arponsuport",  # Direct Support @username
    "CHANNEL_URL": "https://t.me/ARPONTOLS",  # Official Telegram Channel URL
    "RESTOCK_CHANNEL_ID": "@ARPONTOLS",  # Channel ID or @username for auto restock alerts
    "ORDER_LOG_CHANNEL_ID": "@ARPONTOLS",  # Channel ID or @username for auto live order posts
    # Primary Upstream Wholesale Bot / Supplier API
    "SUPPLIER_NAME": "Primary Wholesale API",
    "SUPPLIER_BASE_URL": "",             # e.g. https://api.supplier.com/v1
    "SUPPLIER_API_KEY": "",              # Your Upstream Supplier API Key
    "SUPPLIER_AUTH_HEADER": "Authorization",  # e.g. Authorization or X-API-Key
    "SUPPLIER_AUTH_PREFIX": "Bearer ",   # e.g. "Bearer " or "" (empty if raw key)
    "SUPPLIER_PRODUCTS_ENDPOINT": "/products",
    "SUPPLIER_BALANCE_ENDPOINT": "/balance",
    "SUPPLIER_ORDER_ENDPOINT": "/order",
    # Profit & Store Defaults
    "DEFAULT_MARGIN_PCT": 35.0,          # Default profit markup % (e.g., 35% -> cost $10 sells at $13.50)
    "REFERRAL_COMMISSION_PCT": 5.0,      # Referral commission % on referred user deposits/orders
    "MIN_DEPOSIT_USD": 1.0,              # Minimum deposit amount in USD
    "BDT_PER_USD": 125.0,                # Exchange rate for bKash / Nagad manual deposits
    "AUTO_SYNC_INTERVAL_SEC": 300,       # Auto-sync supplier catalog every 5 minutes
    "DEPOSIT_POLL_INTERVAL_SEC": 45,     # Auto-check pending crypto deposits every 45 seconds
    # Payment Gateway Keys (also editable live from /admin -> Payment Gateways)
    "BINANCE_PAY_API_KEY": "",
    "BINANCE_PAY_SECRET_KEY": "",
    "BINANCE_PAY_ID": "510600663",       # Binance Pay ID
    "USDT_BEP20_WALLET": "0x3456f0e1ef2d12d33344cab0064218c200abe5bc",  # BSC (BEP-20) USDT wallet
    "BSCSCAN_API_KEY": "",               # Free API key from bscscan.com for auto-detect
    "CRYPTOBOT_API_TOKEN": "",           # Token from @CryptoBot -> Crypto Pay
    "NOWPAYMENTS_API_KEY": "",           # API Key from nowpayments.io
    "BKASH_NUMBER": "01988883900",       # bKash Personal/Send Money number
    "NAGAD_NUMBER": "01988883900",       # Nagad Personal/Send Money number
    # Built-in Reseller & Future Website REST API Server
    "ENABLE_WEB_API": False,             # Set True to expose local REST API for Website & Resellers
    "WEB_API_HOST": "0.0.0.0",
    "WEB_API_PORT": 8080,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ARPONTOLS")


def load_dotenv_file(env_path: Path = ENV_FILE) -> None:
    """Lightweight .env loader without external dependencies."""
    if not env_path.exists():
        return
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val
    except Exception as exc:
        logger.warning("Failed to parse .env file: %s", exc)


def load_config(interactive: bool = False) -> Dict[str, Any]:
    """Load config from DEFAULT_CONFIG -> bot_config.json -> environment variables."""
    load_dotenv_file()
    cfg = dict(DEFAULT_CONFIG)

    if CONFIG_FILE.exists():
        try:
            saved = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            if isinstance(saved, dict):
                cfg.update(saved)
        except Exception as exc:
            logger.warning("Could not read %s: %s", CONFIG_FILE.name, exc)

    # Environment variable overrides
    for key, default_val in DEFAULT_CONFIG.items():
        env_val = os.environ.get(key)
        if env_val is not None and env_val != "":
            if isinstance(default_val, bool):
                cfg[key] = env_val.lower() in ("1", "true", "yes", "on")
            elif isinstance(default_val, int):
                try:
                    cfg[key] = int(env_val)
                except ValueError:
                    pass
            elif isinstance(default_val, float):
                try:
                    cfg[key] = float(env_val)
                except ValueError:
                    pass
            else:
                cfg[key] = env_val

    if interactive and not cfg.get("BOT_TOKEN"):
        cfg = run_interactive_setup_wizard(cfg)

    return cfg


def save_config(cfg: Dict[str, Any]) -> None:
    """Persist configuration safely to bot_config.json."""
    CONFIG_FILE.write_text(
        json.dumps(cfg, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def run_interactive_setup_wizard(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Interactive terminal wizard so the user can enter Telegram & Supplier API keys easily."""
    print("\n" + "=" * 68)
    print("  ARPONTOLS BOT — FIRST-TIME API & CONFIGURATION WIZARD")
    print("=" * 68)
    print("Press Enter to keep the current value shown in [brackets].\n")

    def ask(prompt: str, key: str, cast: type = str, required: bool = False) -> Any:
        current = cfg.get(key, "")
        while True:
            shown = str(current) if current not in ("", 0, None) else "not set"
            raw = input(f"  {prompt} [{shown}]: ").strip()
            if not raw:
                if required and current in ("", 0, None):
                    print("    ! This field is required. Please enter a value.")
                    continue
                return current
            try:
                return cast(raw)
            except ValueError:
                print(f"    ! Invalid value. Expected {cast.__name__}.")

    cfg["BOT_TOKEN"] = ask("Telegram Bot Token (@BotFather)", "BOT_TOKEN", str, required=True)
    cfg["OWNER_ID"] = ask("Owner Telegram User ID (@userinfobot)", "OWNER_ID", int, required=True)
    cfg["STORE_NAME"] = ask("Store Name", "STORE_NAME", str)
    cfg["OWNER_USERNAME"] = ask("Owner @username", "OWNER_USERNAME", str)
    cfg["SUPPORT_USERNAME"] = ask("Support @username", "SUPPORT_USERNAME", str)
    cfg["CHANNEL_URL"] = ask("Official Channel URL", "CHANNEL_URL", str)

    print("\n--- Upstream Wholesale Bot / Supplier API (Optional now, editable in /admin) ---")
    cfg["SUPPLIER_NAME"] = ask("Supplier Name", "SUPPLIER_NAME", str)
    cfg["SUPPLIER_BASE_URL"] = ask("Supplier API Base URL (e.g. https://api.example.com/v1)", "SUPPLIER_BASE_URL", str)
    cfg["SUPPLIER_API_KEY"] = ask("Supplier API Key", "SUPPLIER_API_KEY", str)
    cfg["DEFAULT_MARGIN_PCT"] = ask("Default Profit Margin % (e.g. 35)", "DEFAULT_MARGIN_PCT", float)

    print("\n--- Payment Gateways (Optional now, editable live in /admin) ---")
    cfg["BINANCE_PAY_ID"] = ask("Binance Pay ID (for manual/instant fallback)", "BINANCE_PAY_ID", str)
    cfg["USDT_BEP20_WALLET"] = ask("USDT BEP-20 Wallet Address", "USDT_BEP20_WALLET", str)
    cfg["CRYPTOBOT_API_TOKEN"] = ask("CryptoBot API Token (@CryptoBot)", "CRYPTOBOT_API_TOKEN", str)
    cfg["BKASH_NUMBER"] = ask("bKash Number (BD manual deposit)", "BKASH_NUMBER", str)
    cfg["NAGAD_NUMBER"] = ask("Nagad Number (BD manual deposit)", "NAGAD_NUMBER", str)

    save_config(cfg)
    print(f"\n[OK] Configuration saved to {CONFIG_FILE.name}! Starting bot...\n")
    return cfg


# ==============================================================================
# 2. CONSTANTS, RBAC SCOPES, BRAND NORMALIZER & HELPERS
# ==============================================================================

ALL_RBAC_SCOPES: List[Tuple[str, str]] = [
    ("catalog", "📦 Catalog & Margins"),
    ("custom_products", "🏪 Custom Products"),
    ("orders", "📋 Orders History"),
    ("deposits", "📑 Deposits Log"),
    ("users", "👥 Users Management"),
    ("tickets", "🎫 Support Tickets"),
    ("coupons", "🏷️ Promo Codes"),
    ("broadcast", "📢 Broadcast"),
    ("payments", "💳 Payment Gateways"),
    ("settings", "⚙️ Store Settings"),
    ("analytics", "📊 Analytics & Margin"),
    ("staff", "🛡️ Team & Staff Roles"),
    ("forcesub", "📣 Force-Sub Hub"),
    ("stock_watcher", "👁️ Stock Watcher"),
    ("suppliers", "🔌 External Suppliers"),
    ("support", "💬 Support Settings"),
    ("referrals", "👥 Referrals"),
    ("maintenance", "⚡ Pause / Resume"),
]

BRAND_ICON_MAP: Dict[str, str] = {
    "ChatGPT": "🤖",
    "API Codex": "⚙️",
    "Claude": "🧠",
    "CapCut PRO": "✂️",
    "Cursor AI": "💻",
    "Lovable AI": "💜",
    "Google & Gemini": "🌐",
    "Microsoft & Windows": "🪟",
    "YouTube Premium": "▶️",
    "ElevenLabs": "🎙️",
    "TikTok": "🎵",
    "Spotify": "🎧",
    "Prime Video": "🎬",
    "Apple Services": "🍎",
    "Canva": "🎨",
    "Netflix": "🍿",
    "VPN & Security": "🛡️",
    "Telegram": "✈️",
    "Gaming": "🎮",
}

BRAND_KEYWORDS: List[Tuple[str, Tuple[str, ...]]] = [
    ("ChatGPT", ("chatgpt", "openai", "gpt-4", "gpt4", "gpt plus", "gpt team")),
    ("API Codex", ("codex", "api key", "api credit", "openrouter", "anthropic api")),
    ("Claude", ("claude", "anthropic")),
    ("CapCut PRO", ("capcut",)),
    ("Cursor AI", ("cursor",)),
    ("Lovable AI", ("lovable",)),
    ("Google & Gemini", ("gemini", "google", "workspace", "gdrive", "google one", "veo")),
    ("Microsoft & Windows", ("windows", "office", "microsoft", "365", "azure", "xbox")),
    ("YouTube Premium", ("youtube", "yt premium")),
    ("ElevenLabs", ("elevenlabs", "eleven labs")),
    ("TikTok", ("tiktok",)),
    ("Spotify", ("spotify",)),
    ("Prime Video", ("prime video", "amazon prime")),
    ("Apple Services", ("apple", "icloud", "itunes")),
    ("Canva", ("canva",)),
    ("Netflix", ("netflix",)),
    ("VPN & Security", ("vpn", "nordvpn", "expressvpn", "surfshark", "mullvad")),
    ("Telegram", ("telegram", "tg premium", "stars")),
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def esc(text: Any) -> str:
    """Escape HTML special characters safely."""
    return html.escape(str(text if text is not None else ""))


def fmt_money(amount: float, symbol: str = "$") -> str:
    return f"{symbol}{float(amount):,.2f}"


def gen_code(prefix: str, length: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    token = "".join(secrets.choice(alphabet) for _ in range(length))
    return f"{prefix}-{token}"


def detect_brand(product_name: str, explicit_brand: Optional[str] = None) -> str:
    """Normalize or infer a clean brand folder name."""
    if explicit_brand and explicit_brand.strip():
        return explicit_brand.strip()
    low = product_name.lower()
    for brand, keywords in BRAND_KEYWORDS:
        if any(kw in low for kw in keywords):
            return brand
    return "Standalone"


def brand_icon(brand: str) -> str:
    return BRAND_ICON_MAP.get(brand, "🏷️")


def calculate_sell_price(
    cost_price: float,
    global_margin_pct: float,
    margin_override_pct: Optional[float] = None,
    fixed_price: Optional[float] = None,
) -> float:
    """Compute final retail sell price with profit margin protection."""
    if fixed_price is not None and fixed_price > 0:
        return round(float(fixed_price), 2)
    margin = float(margin_override_pct) if margin_override_pct is not None else float(global_margin_pct)
    sell = float(cost_price) * (1.0 + margin / 100.0)
    return round(max(sell, 0.01), 2)


# ==============================================================================
# 3. DATABASE MANAGER (SQLite3 + WAL Mode + Atomic Balance Operations)
# ==============================================================================

class DatabaseManager:
    """Thread-safe SQLite3 data layer for the entire platform."""

    def __init__(self, db_path: Union[str, Path] = DB_FILE):
        self.db_path = str(db_path)
        self._init_schema()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        try:
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT DEFAULT '',
                    full_name TEXT DEFAULT '',
                    balance REAL DEFAULT 0.0,
                    total_spent REAL DEFAULT 0.0,
                    total_deposited REAL DEFAULT 0.0,
                    referral_balance REAL DEFAULT 0.0,
                    referred_by INTEGER DEFAULT NULL,
                    referral_count INTEGER DEFAULT 0,
                    is_banned INTEGER DEFAULT 0,
                    ban_reason TEXT DEFAULT '',
                    api_key TEXT UNIQUE DEFAULT NULL,
                    api_enabled INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL,
                    last_active TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS staff_roles (
                    user_id INTEGER PRIMARY KEY,
                    role_name TEXT DEFAULT 'Staff',
                    scopes_json TEXT DEFAULT '[]',
                    added_by INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS suppliers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    base_url TEXT NOT NULL,
                    api_key TEXT NOT NULL,
                    auth_header TEXT DEFAULT 'Authorization',
                    auth_prefix TEXT DEFAULT 'Bearer ',
                    products_endpoint TEXT DEFAULT '/products',
                    balance_endpoint TEXT DEFAULT '/balance',
                    order_endpoint TEXT DEFAULT '/order',
                    is_active INTEGER DEFAULT 1,
                    is_primary INTEGER DEFAULT 0,
                    cached_balance REAL DEFAULT 0.0,
                    last_sync_at TEXT DEFAULT '',
                    extra_headers_json TEXT DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    supplier_id INTEGER DEFAULT NULL,
                    external_id TEXT DEFAULT '',
                    brand TEXT DEFAULT 'Standalone',
                    name TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    cost_price REAL DEFAULT 0.0,
                    margin_override_pct REAL DEFAULT NULL,
                    fixed_price REAL DEFAULT NULL,
                    stock INTEGER DEFAULT 0,
                    is_custom INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 1,
                    warranty_text TEXT DEFAULT 'Standard Warranty',
                    delivery_type TEXT DEFAULT 'supplier_api',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(supplier_id, external_id)
                );

                CREATE TABLE IF NOT EXISTS local_stock_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER NOT NULL,
                    credential_data TEXT NOT NULL,
                    is_sold INTEGER DEFAULT 0,
                    order_id INTEGER DEFAULT NULL,
                    added_at TEXT NOT NULL,
                    sold_at TEXT DEFAULT NULL,
                    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_code TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    product_name TEXT NOT NULL,
                    brand TEXT DEFAULT '',
                    quantity INTEGER DEFAULT 1,
                    cost_unit_price REAL DEFAULT 0.0,
                    sell_unit_price REAL DEFAULT 0.0,
                    discount_amount REAL DEFAULT 0.0,
                    total_paid REAL DEFAULT 0.0,
                    net_profit REAL DEFAULT 0.0,
                    coupon_code TEXT DEFAULT '',
                    supplier_id INTEGER DEFAULT NULL,
                    supplier_order_id TEXT DEFAULT '',
                    delivered_data TEXT DEFAULT '',
                    status TEXT DEFAULT 'completed',
                    source TEXT DEFAULT 'telegram',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS deposits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    deposit_code TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    gateway TEXT NOT NULL,
                    amount_usd REAL NOT NULL,
                    expected_crypto_amount REAL DEFAULT NULL,
                    tx_reference TEXT DEFAULT '',
                    status TEXT DEFAULT 'pending',
                    meta_json TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    confirmed_at TEXT DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS payment_gateways (
                    code TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    is_enabled INTEGER DEFAULT 0,
                    fee_pct REAL DEFAULT 0.0,
                    min_amount REAL DEFAULT 1.0,
                    config_json TEXT DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS coupons (
                    code TEXT PRIMARY KEY,
                    discount_type TEXT DEFAULT 'percent',
                    discount_value REAL NOT NULL,
                    min_order REAL DEFAULT 0.0,
                    max_uses INTEGER DEFAULT 0,
                    used_count INTEGER DEFAULT 0,
                    expires_at TEXT DEFAULT '',
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS coupon_redemptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coupon_code TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    order_id INTEGER NOT NULL,
                    redeemed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_code TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    subject TEXT NOT NULL,
                    category TEXT DEFAULT 'General',
                    status TEXT DEFAULT 'open',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ticket_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id INTEGER NOT NULL,
                    sender_id INTEGER NOT NULL,
                    sender_role TEXT NOT NULL,
                    text TEXT DEFAULT '',
                    photo_file_id TEXT DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(ticket_id) REFERENCES tickets(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS force_sub_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    invite_link TEXT NOT NULL,
                    is_active INTEGER DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS stock_watchers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    notified INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    UNIQUE(user_id, product_id)
                );

                CREATE TABLE IF NOT EXISTS referral_earnings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER NOT NULL,
                    referred_user_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    source_amount REAL NOT NULL,
                    commission_amount REAL NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS incoming_payment_sms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gateway TEXT NOT NULL,
                    tx_id TEXT UNIQUE NOT NULL,
                    amount_bdt REAL DEFAULT 0.0,
                    sender TEXT DEFAULT '',
                    raw_text TEXT DEFAULT '',
                    is_claimed INTEGER DEFAULT 0,
                    received_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT DEFAULT '',
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
                CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);
                CREATE INDEX IF NOT EXISTS idx_deposits_status ON deposits(status);
                CREATE INDEX IF NOT EXISTS idx_tickets_user ON tickets(user_id);
                """
            )

    def bootstrap_from_config(self, cfg: Dict[str, Any]) -> None:
        """Seed default settings, payment gateways, and primary supplier if not yet stored."""
        defaults = {
            "store_name": str(cfg.get("STORE_NAME", "ARPONTOLS")),
            "owner_username": str(cfg.get("OWNER_USERNAME", "@Support")),
            "support_username": str(cfg.get("SUPPORT_USERNAME", "@Support")),
            "channel_url": str(cfg.get("CHANNEL_URL", "https://t.me/")),
            "restock_channel_id": str(cfg.get("RESTOCK_CHANNEL_ID", "@ARPONTOLS")),
            "order_log_channel_id": str(cfg.get("ORDER_LOG_CHANNEL_ID", "@ARPONTOLS")),
            "owner_id": str(cfg.get("OWNER_ID", 0)),
            "default_margin_pct": str(cfg.get("DEFAULT_MARGIN_PCT", 35.0)),
            "referral_commission_pct": str(cfg.get("REFERRAL_COMMISSION_PCT", 5.0)),
            "referral_signup_bonus_usd": "0.05",
            "min_deposit_usd": str(cfg.get("MIN_DEPOSIT_USD", 1.0)),
            "bdt_per_usd": str(cfg.get("BDT_PER_USD", 125.0)),
            "currency_symbol": "$",
            "maintenance_mode": "0",
            "maintenance_msg": "⚡ Store is currently under scheduled maintenance. Please check back shortly.",
            "forcesub_enabled": "1",

            "help_text": (
                "📖 <b>How to Purchase</b>\n"
                "1. Top up your wallet balance via <b>Deposit</b>.\n"
                "2. Open <b>Shop</b> or use /search to find your digital product.\n"
                "3. Select quantity, apply a promo code if available, and tap <b>Buy Now</b>.\n"
                "4. Your credentials/keys are delivered <b>instantly</b> in chat and saved under <b>My Orders</b>.\n\n"
                "Need help? Open a <b>Support Ticket</b> anytime!"
            ),
        }
        with self.connect() as conn:
            for k, v in defaults.items():
                conn.execute("INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)", (k, v))

            # Seed payment gateways
            gateways = [
                (
                    "binance_pay",
                    "🔶 Binance Pay (Instant 0% Fee)",
                    1 if (cfg.get("BINANCE_PAY_API_KEY") or cfg.get("BINANCE_PAY_ID")) else 1,
                    0.0,
                    float(cfg.get("MIN_DEPOSIT_USD", 1.0)),
                    json.dumps({
                        "api_key": cfg.get("BINANCE_PAY_API_KEY", ""),
                        "secret_key": cfg.get("BINANCE_PAY_SECRET_KEY", ""),
                        "pay_id": cfg.get("BINANCE_PAY_ID", ""),
                    }),
                ),
                (
                    "usdt_bep20",
                    "💎 USDT BEP-20 (Auto-Detect)",
                    1 if cfg.get("USDT_BEP20_WALLET") else 1,
                    0.0,
                    float(cfg.get("MIN_DEPOSIT_USD", 1.0)),
                    json.dumps({
                        "wallet_address": cfg.get("USDT_BEP20_WALLET", ""),
                        "bscscan_api_key": cfg.get("BSCSCAN_API_KEY", ""),
                    }),
                ),
                (
                    "cryptobot",
                    "🤖 CryptoBot (Instant Crypto)",
                    1 if cfg.get("CRYPTOBOT_API_TOKEN") else 0,
                    0.0,
                    float(cfg.get("MIN_DEPOSIT_USD", 1.0)),
                    json.dumps({"api_token": cfg.get("CRYPTOBOT_API_TOKEN", "")}),
                ),
                (
                    "nowpayments",
                    "🌐 NOWPayments (Multi-Crypto)",
                    1 if cfg.get("NOWPAYMENTS_API_KEY") else 0,
                    0.5,
                    2.0,
                    json.dumps({"api_key": cfg.get("NOWPAYMENTS_API_KEY", "")}),
                ),
                (
                    "bkash_manual",
                    "📱 bKash (BD Instant Verify)",
                    1 if cfg.get("BKASH_NUMBER") else 1,
                    0.0,
                    1.0,
                    json.dumps({
                        "number": cfg.get("BKASH_NUMBER", ""),
                        "account_type": "Personal / Send Money",
                    }),
                ),
                (
                    "nagad_manual",
                    "📲 Nagad (BD Instant Verify)",
                    1 if cfg.get("NAGAD_NUMBER") else 1,
                    0.0,
                    1.0,
                    json.dumps({
                        "number": cfg.get("NAGAD_NUMBER", ""),
                        "account_type": "Personal / Send Money",
                    }),
                ),
                (
                    "wallet_manual",
                    "💼 Wallet (bKash / Nagad / International)",
                    1,
                    0.0,
                    0.10,
                    json.dumps({
                        "instructions": (
                            "💳 Bangladesh Payment\n"
                            "📱 bKash / Nagad Personal (Send Money): Set your number in /admin -> Payment Gateways\n\n"
                            "📸 After completing payment, tap Submit Transaction ID / Proof below."
                        )
                    }),
                ),
            ]
            for g in gateways:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO payment_gateways
                    (code, display_name, is_enabled, fee_pct, min_amount, config_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    g,
                )

            # Auto-populate user's default payment numbers/IDs into existing DB if empty
            gw_defaults_fill = {
                "binance_pay": ("pay_id", str(cfg.get("BINANCE_PAY_ID", "510600663"))),
                "usdt_bep20": ("wallet_address", str(cfg.get("USDT_BEP20_WALLET", "0x3456f0e1ef2d12d33344cab0064218c200abe5bc"))),
                "bkash_manual": ("number", str(cfg.get("BKASH_NUMBER", "01988883900"))),
                "nagad_manual": ("number", str(cfg.get("NAGAD_NUMBER", "01988883900"))),
            }
            for gw_code, (field_k, field_v) in gw_defaults_fill.items():
                if not field_v:
                    continue
                row_gw = conn.execute("SELECT config_json FROM payment_gateways WHERE code = ?", (gw_code,)).fetchone()
                if row_gw:
                    gobj = json.loads(row_gw["config_json"] or "{}")
                    if not str(gobj.get(field_k, "")).strip():
                        gobj[field_k] = field_v
                        conn.execute(
                            "UPDATE payment_gateways SET config_json = ?, is_enabled = 1 WHERE code = ?",
                            (json.dumps(gobj), gw_code),
                        )

            # Seed mandatory Force-Sub channel (@ARPONTOLS) if none configured yet
            fsub_count = conn.execute("SELECT COUNT(*) FROM force_sub_channels").fetchone()[0]
            if fsub_count == 0:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO force_sub_channels(chat_id, title, invite_link, is_active)
                    VALUES ('@ARPONTOLS', 'ARPONTOLS Official Channel', 'https://t.me/ARPONTOLS', 1)
                    """
                )
                conn.execute("UPDATE settings SET value = '1' WHERE key = 'forcesub_enabled'")

            # Seed primary supplier if provided in config and no supplier exists yet
            sup_count = conn.execute("SELECT COUNT(*) FROM suppliers").fetchone()[0]
            if sup_count == 0 and cfg.get("SUPPLIER_BASE_URL") and cfg.get("SUPPLIER_API_KEY"):
                conn.execute(
                    """
                    INSERT INTO suppliers
                    (name, base_url, api_key, auth_header, auth_prefix,
                     products_endpoint, balance_endpoint, order_endpoint, is_active, is_primary)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
                    """,
                    (
                        cfg.get("SUPPLIER_NAME", "Primary Wholesale API"),
                        cfg["SUPPLIER_BASE_URL"].rstrip("/"),
                        cfg["SUPPLIER_API_KEY"],
                        cfg.get("SUPPLIER_AUTH_HEADER", "Authorization"),
                        cfg.get("SUPPLIER_AUTH_PREFIX", "Bearer "),
                        cfg.get("SUPPLIER_PRODUCTS_ENDPOINT", "/products"),
                        cfg.get("SUPPLIER_BALANCE_ENDPOINT", "/balance"),
                        cfg.get("SUPPLIER_ORDER_ENDPOINT", "/order"),
                    ),
                )

    # --- Settings ---
    def get_setting(self, key: str, default: str = "") -> str:
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default

    def get_setting_float(self, key: str, default: float = 0.0) -> float:
        try:
            return float(self.get_setting(key, str(default)))
        except ValueError:
            return default

    def get_setting_bool(self, key: str, default: bool = False) -> bool:
        val = self.get_setting(key, "1" if default else "0")
        return val in ("1", "true", "True", "yes")

    def set_setting(self, key: str, value: Any) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO settings(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )

    # --- Users ---
    def upsert_user(
        self,
        user_id: int,
        username: str = "",
        full_name: str = "",
        referred_by: Optional[int] = None,
    ) -> Tuple[sqlite3.Row, bool]:
        now = utc_now_iso()
        with self.connect() as conn:
            existing = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if existing:
                conn.execute(
                    "UPDATE users SET username = ?, full_name = ?, last_active = ? WHERE user_id = ?",
                    (username or "", full_name or "", now, user_id),
                )
                # Late-bind referral if user was never referred yet and has not spent anything
                just_referred = False
                if (
                    referred_by
                    and referred_by != user_id
                    and not existing["referred_by"]
                    and float(existing["total_spent"] or 0) == 0
                ):
                    ref_row = conn.execute("SELECT user_id FROM users WHERE user_id = ?", (referred_by,)).fetchone()
                    if ref_row:
                        bonus_row = conn.execute(
                            "SELECT value FROM settings WHERE key = 'referral_signup_bonus_usd'"
                        ).fetchone()
                        signup_bonus = float(bonus_row["value"]) if bonus_row else 0.05
                        conn.execute("UPDATE users SET referred_by = ? WHERE user_id = ?", (referred_by, user_id))
                        conn.execute(
                            "UPDATE users SET referral_count = referral_count + 1, balance = balance + ? WHERE user_id = ?",
                            (max(0.0, signup_bonus), referred_by),
                        )
                        if signup_bonus > 0:
                            conn.execute(
                                """
                                INSERT INTO referral_earnings
                                (referrer_id, referred_user_id, source_type, source_amount, commission_amount, created_at)
                                VALUES (?, ?, 'signup', 0, ?, ?)
                                """,
                                (referred_by, user_id, signup_bonus, now),
                            )
                        just_referred = True
                return conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone(), just_referred

            valid_ref = None
            if referred_by and referred_by != user_id:
                ref_row = conn.execute("SELECT user_id FROM users WHERE user_id = ?", (referred_by,)).fetchone()
                if ref_row:
                    valid_ref = referred_by
                    bonus_row = conn.execute(
                        "SELECT value FROM settings WHERE key = 'referral_signup_bonus_usd'"
                    ).fetchone()
                    signup_bonus = float(bonus_row["value"]) if bonus_row else 0.0
                    conn.execute(
                        "UPDATE users SET referral_count = referral_count + 1, balance = balance + ? WHERE user_id = ?",
                        (max(0.0, signup_bonus), valid_ref),
                    )
                    if signup_bonus > 0:
                        conn.execute(
                            """
                            INSERT INTO referral_earnings
                            (referrer_id, referred_user_id, source_type, source_amount, commission_amount, created_at)
                            VALUES (?, ?, 'signup', 0, ?, ?)
                            """,
                            (valid_ref, user_id, signup_bonus, now),
                        )

            conn.execute(
                """
                INSERT INTO users(user_id, username, full_name, referred_by, created_at, last_active)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (user_id, username or "", full_name or "", valid_ref, now, now),
            )
            return conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone(), True

    def get_user(self, user_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()

    def find_user(self, query: str) -> Optional[sqlite3.Row]:
        q = query.strip().lstrip("@")
        with self.connect() as conn:
            if q.isdigit():
                return conn.execute("SELECT * FROM users WHERE user_id = ?", (int(q),)).fetchone()
            return conn.execute(
                "SELECT * FROM users WHERE LOWER(username) = LOWER(?)", (q,)
            ).fetchone()

    def get_user_by_api_key(self, api_key: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM users WHERE api_key = ? AND api_enabled = 1 AND is_banned = 0",
                (api_key,),
            ).fetchone()

    def generate_user_api_key(self, user_id: int) -> str:
        key = f"arp_{secrets.token_hex(24)}"
        with self.connect() as conn:
            conn.execute("UPDATE users SET api_key = ?, api_enabled = 1 WHERE user_id = ?", (key, user_id))
        return key

    def adjust_balance(
        self,
        user_id: int,
        delta: float,
        *,
        is_deposit: bool = False,
        is_spend: bool = False,
        require_sufficient: bool = False,
    ) -> Tuple[bool, float]:
        """Atomic balance update inside an IMMEDIATE transaction."""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)).fetchone()
                if not row:
                    conn.execute("ROLLBACK")
                    return False, 0.0
                current = float(row["balance"])
                new_bal = round(current + float(delta), 4)
                if require_sufficient and new_bal < -1e-9:
                    conn.execute("ROLLBACK")
                    return False, current
                new_bal = max(0.0, new_bal) if require_sufficient else new_bal
                conn.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_bal, user_id))
                if is_deposit and delta > 0:
                    conn.execute(
                        "UPDATE users SET total_deposited = total_deposited + ? WHERE user_id = ?",
                        (float(delta), user_id),
                    )
                if is_spend and delta < 0:
                    conn.execute(
                        "UPDATE users SET total_spent = total_spent + ? WHERE user_id = ?",
                        (abs(float(delta)), user_id),
                    )
                conn.execute("COMMIT")
                return True, new_bal
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def credit_referral_commission(
        self, referred_user_id: int, source_type: str, source_amount: float
    ) -> Optional[Tuple[int, float]]:
        pct = self.get_setting_float("referral_commission_pct", 5.0)
        if pct <= 0 or source_amount <= 0:
            return None
        with self.connect() as conn:
            row = conn.execute(
                "SELECT referred_by FROM users WHERE user_id = ?", (referred_user_id,)
            ).fetchone()
            if not row or not row["referred_by"]:
                return None
            referrer_id = int(row["referred_by"])
            commission = round(source_amount * (pct / 100.0), 4)
            if commission <= 0:
                return None
            conn.execute(
                "UPDATE users SET referral_balance = referral_balance + ? WHERE user_id = ?",
                (commission, referrer_id),
            )
            conn.execute(
                """
                INSERT INTO referral_earnings
                (referrer_id, referred_user_id, source_type, source_amount, commission_amount, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (referrer_id, referred_user_id, source_type, source_amount, commission, utc_now_iso()),
            )
            return referrer_id, commission

    def transfer_referral_to_main(self, user_id: int) -> float:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT referral_balance FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if not row or float(row["referral_balance"]) <= 0:
                conn.execute("ROLLBACK")
                return 0.0
            amt = round(float(row["referral_balance"]), 2)
            conn.execute(
                "UPDATE users SET referral_balance = 0, balance = balance + ? WHERE user_id = ?",
                (amt, user_id),
            )
            conn.execute("COMMIT")
            return amt

    def set_user_ban(self, user_id: int, banned: bool, reason: str = "") -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE users SET is_banned = ?, ban_reason = ? WHERE user_id = ?",
                (1 if banned else 0, reason, user_id),
            )

    def list_users_paginated(self, page: int = 1, per_page: int = 10) -> Tuple[List[sqlite3.Row], int]:
        offset = max(0, (page - 1) * per_page)
        with self.connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            rows = conn.execute(
                "SELECT * FROM users ORDER BY last_active DESC LIMIT ? OFFSET ?",
                (per_page, offset),
            ).fetchall()
        return rows, int(total)

    def get_all_active_user_ids(self) -> List[int]:
        with self.connect() as conn:
            rows = conn.execute("SELECT user_id FROM users WHERE is_banned = 0").fetchall()
            return [int(r["user_id"]) for r in rows]

    # --- RBAC Staff ---
    def get_staff(self, user_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM staff_roles WHERE user_id = ?", (user_id,)).fetchone()

    def list_staff(self) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM staff_roles ORDER BY created_at DESC").fetchall()

    def upsert_staff(self, user_id: int, role_name: str, scopes: List[str], added_by: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO staff_roles(user_id, role_name, scopes_json, added_by, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET role_name=excluded.role_name, scopes_json=excluded.scopes_json
                """,
                (user_id, role_name, json.dumps(sorted(set(scopes))), added_by, utc_now_iso()),
            )

    def delete_staff(self, user_id: int) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM staff_roles WHERE user_id = ?", (user_id,))

    def has_scope(self, user_id: int, owner_id: int, scope: str) -> bool:
        if int(user_id) == int(owner_id):
            return True
        staff = self.get_staff(user_id)
        if not staff:
            return False
        try:
            scopes = json.loads(staff["scopes_json"] or "[]")
        except Exception:
            scopes = []
        return scope in scopes or "*" in scopes

    def is_admin_or_staff(self, user_id: int, owner_id: int) -> bool:
        return int(user_id) == int(owner_id) or self.get_staff(user_id) is not None

    # --- Suppliers ---
    def list_suppliers(self, only_active: bool = False) -> List[sqlite3.Row]:
        sql = "SELECT * FROM suppliers"
        if only_active:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_primary DESC, id ASC"
        with self.connect() as conn:
            return conn.execute(sql).fetchall()

    def get_supplier(self, supplier_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM suppliers WHERE id = ?", (supplier_id,)).fetchone()

    def get_primary_supplier(self) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM suppliers WHERE is_active = 1 ORDER BY is_primary DESC, id ASC LIMIT 1"
            ).fetchone()
            return row

    def add_supplier(
        self,
        name: str,
        base_url: str,
        api_key: str,
        auth_header: str = "Authorization",
        auth_prefix: str = "Bearer ",
        products_endpoint: str = "/products",
        balance_endpoint: str = "/balance",
        order_endpoint: str = "/order",
        is_primary: bool = False,
    ) -> int:
        with self.connect() as conn:
            if is_primary:
                conn.execute("UPDATE suppliers SET is_primary = 0")
            cur = conn.execute(
                """
                INSERT INTO suppliers
                (name, base_url, api_key, auth_header, auth_prefix,
                 products_endpoint, balance_endpoint, order_endpoint, is_active, is_primary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    name.strip(),
                    base_url.strip().rstrip("/"),
                    api_key.strip(),
                    auth_header.strip(),
                    auth_prefix,
                    products_endpoint.strip(),
                    balance_endpoint.strip(),
                    order_endpoint.strip(),
                    1 if is_primary else 0,
                ),
            )
            return int(cur.lastrowid)

    def update_supplier_balance(self, supplier_id: int, balance: float) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE suppliers SET cached_balance = ?, last_sync_at = ? WHERE id = ?",
                (float(balance), utc_now_iso(), supplier_id),
            )

    def set_primary_supplier(self, supplier_id: int) -> None:
        with self.connect() as conn:
            conn.execute("UPDATE suppliers SET is_primary = 0")
            conn.execute("UPDATE suppliers SET is_primary = 1, is_active = 1 WHERE id = ?", (supplier_id,))

    def toggle_supplier(self, supplier_id: int) -> bool:
        with self.connect() as conn:
            row = conn.execute("SELECT is_active FROM suppliers WHERE id = ?", (supplier_id,)).fetchone()
            if not row:
                return False
            new_state = 0 if row["is_active"] else 1
            conn.execute("UPDATE suppliers SET is_active = ? WHERE id = ?", (new_state, supplier_id))
            return bool(new_state)

    def update_supplier_key(self, supplier_id: int, api_key: str, base_url: Optional[str] = None) -> None:
        clean_key = api_key.strip()
        if clean_key.lower().startswith("bearer "):
            clean_key = clean_key[7:].strip()
        with self.connect() as conn:
            if base_url:
                conn.execute(
                    "UPDATE suppliers SET api_key = ?, base_url = ?, auth_header = 'Authorization', auth_prefix = 'Bearer ' WHERE id = ?",
                    (clean_key, base_url.strip().rstrip("/"), supplier_id),
                )
            else:
                conn.execute(
                    "UPDATE suppliers SET api_key = ?, auth_header = 'Authorization', auth_prefix = 'Bearer ' WHERE id = ?",
                    (clean_key, supplier_id),
                )

    def delete_supplier(self, supplier_id: int) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM products WHERE supplier_id = ? AND is_custom = 0", (supplier_id,))
            conn.execute("DELETE FROM suppliers WHERE id = ?", (supplier_id,))

    # --- Products & Local Stock ---
    def upsert_supplier_product(
        self,
        supplier_id: int,
        external_id: str,
        name: str,
        cost_price: float,
        stock: int,
        brand: Optional[str] = None,
        description: str = "",
        warranty_text: str = "Instant Delivery",
    ) -> Tuple[int, bool]:
        """Upsert upstream product and return (product_id, became_restocked)."""
        norm_brand = detect_brand(name, brand)
        now = utc_now_iso()
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT id, stock FROM products WHERE supplier_id = ? AND external_id = ?",
                (supplier_id, str(external_id)),
            ).fetchone()
            if existing:
                old_stock = int(existing["stock"])
                conn.execute(
                    """
                    UPDATE products
                    SET brand = ?, name = ?, description = ?, cost_price = ?, stock = ?,
                        warranty_text = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (norm_brand, name, description, float(cost_price), int(stock), warranty_text, now, existing["id"]),
                )
                return int(existing["id"]), max(0, int(stock) - old_stock) if int(stock) > old_stock else 0

            cur = conn.execute(
                """
                INSERT INTO products
                (supplier_id, external_id, brand, name, description, cost_price, stock,
                 is_custom, is_active, warranty_text, delivery_type, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, ?, 'supplier_api', ?, ?)
                """,
                (supplier_id, str(external_id), norm_brand, name, description, float(cost_price), int(stock), warranty_text, now, now),
            )
            return int(cur.lastrowid), 0  # Initial catalog import: do not spam restock alerts

    def create_custom_product(
        self,
        brand: str,
        name: str,
        sell_price: float,
        cost_price: float = 0.0,
        warranty_text: str = "Standard Warranty",
        description: str = "",
    ) -> int:
        now = utc_now_iso()
        ext_id = f"custom_{secrets.token_hex(6)}"
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO products
                (supplier_id, external_id, brand, name, description, cost_price, fixed_price,
                 stock, is_custom, is_active, warranty_text, delivery_type, created_at, updated_at)
                VALUES (NULL, ?, ?, ?, ?, ?, ?, 0, 1, 1, ?, 'local_stock', ?, ?)
                """,
                (ext_id, brand.strip() or "Standalone", name.strip(), description.strip(), float(cost_price), float(sell_price), warranty_text.strip(), now, now),
            )
            return int(cur.lastrowid)

    def add_local_stock_lines(self, product_id: int, lines: List[str]) -> Tuple[int, bool]:
        cleaned = [ln.strip() for ln in lines if ln.strip()]
        if not cleaned:
            return 0, False
        now = utc_now_iso()
        with self.connect() as conn:
            row = conn.execute("SELECT stock FROM products WHERE id = ?", (product_id,)).fetchone()
            old_stock = int(row["stock"]) if row else 0
            conn.executemany(
                "INSERT INTO local_stock_items(product_id, credential_data, added_at) VALUES (?, ?, ?)",
                [(product_id, item, now) for item in cleaned],
            )
            new_stock = conn.execute(
                "SELECT COUNT(*) FROM local_stock_items WHERE product_id = ? AND is_sold = 0",
                (product_id,),
            ).fetchone()[0]
            conn.execute(
                "UPDATE products SET stock = ?, updated_at = ? WHERE id = ?",
                (int(new_stock), now, product_id),
            )
            return len(cleaned), (old_stock == 0 and new_stock > 0)

    def consume_local_stock(self, product_id: int, quantity: int, order_id: int) -> Optional[List[str]]:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT id, credential_data FROM local_stock_items
                WHERE product_id = ? AND is_sold = 0
                ORDER BY id ASC LIMIT ?
                """,
                (product_id, quantity),
            ).fetchall()
            if len(rows) < quantity:
                conn.execute("ROLLBACK")
                return None
            ids = [r["id"] for r in rows]
            creds = [r["credential_data"] for r in rows]
            placeholders = ",".join("?" for _ in ids)
            conn.execute(
                f"UPDATE local_stock_items SET is_sold = 1, order_id = ?, sold_at = ? WHERE id IN ({placeholders})",
                [order_id, now, *ids],
            )
            remaining = conn.execute(
                "SELECT COUNT(*) FROM local_stock_items WHERE product_id = ? AND is_sold = 0",
                (product_id,),
            ).fetchone()[0]
            conn.execute("UPDATE products SET stock = ?, updated_at = ? WHERE id = ?", (int(remaining), now, product_id))
            conn.execute("COMMIT")
            return creds

    def get_product(self, product_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()

    def get_catalog_entries(self, include_inactive: bool = False) -> Tuple[List[Dict[str, Any]], int]:
        """
        Build DigiHub-style unified catalog entries:
        - Brands with >=2 products become Brand Folders: {type:'brand', brand, plan_count, total_stock}
        - Single items / Standalone items become direct product buttons: {type:'product', product}
        """
        where = "" if include_inactive else "WHERE is_active = 1"
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM products {where} ORDER BY stock DESC, brand ASC, name ASC"
            ).fetchall()

        by_brand: Dict[str, List[sqlite3.Row]] = {}
        for r in rows:
            b = r["brand"] or "Standalone"
            by_brand.setdefault(b, []).append(r)

        entries: List[Dict[str, Any]] = []
        folders: List[Dict[str, Any]] = []
        singles: List[Dict[str, Any]] = []

        for b, items in by_brand.items():
            if b != "Standalone" and len(items) >= 2:
                folders.append({
                    "type": "brand",
                    "brand": b,
                    "plan_count": len(items),
                    "total_stock": sum(max(0, int(x["stock"])) for x in items),
                })
            else:
                for it in items:
                    singles.append({"type": "product", "product": it})

        folders.sort(key=lambda x: (-x["total_stock"], x["brand"]))
        entries.extend(folders)
        entries.extend(singles)
        return entries, len(rows)

    def get_products_by_brand(self, brand: str, include_inactive: bool = False) -> List[sqlite3.Row]:
        sql = "SELECT * FROM products WHERE brand = ?"
        if not include_inactive:
            sql += " AND is_active = 1"
        sql += " ORDER BY stock DESC, cost_price ASC"
        with self.connect() as conn:
            return conn.execute(sql, (brand,)).fetchall()

    def list_products(self, only_custom: bool = False, include_inactive: bool = True) -> List[sqlite3.Row]:
        clauses = []
        if only_custom:
            clauses.append("is_custom = 1")
        if not include_inactive:
            clauses.append("is_active = 1")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.connect() as conn:
            return conn.execute(f"SELECT * FROM products {where} ORDER BY id DESC").fetchall()

    def search_products(self, query: str, limit: int = 20) -> List[sqlite3.Row]:
        q = f"%{query.strip()}%"
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT * FROM products
                WHERE is_active = 1 AND (LOWER(name) LIKE LOWER(?) OR LOWER(brand) LIKE LOWER(?) OR LOWER(description) LIKE LOWER(?))
                ORDER BY stock DESC, name ASC LIMIT ?
                """,
                (q, q, q, limit),
            ).fetchall()

    def update_product_pricing(
        self,
        product_id: int,
        margin_override_pct: Optional[float] = None,
        fixed_price: Optional[float] = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE products SET margin_override_pct = ?, fixed_price = ?, updated_at = ? WHERE id = ?",
                (margin_override_pct, fixed_price, utc_now_iso(), product_id),
            )

    def toggle_product_active(self, product_id: int) -> bool:
        with self.connect() as conn:
            row = conn.execute("SELECT is_active FROM products WHERE id = ?", (product_id,)).fetchone()
            if not row:
                return False
            new_val = 0 if row["is_active"] else 1
            conn.execute("UPDATE products SET is_active = ?, updated_at = ? WHERE id = ?", (new_val, utc_now_iso(), product_id))
            return bool(new_val)

    def delete_product(self, product_id: int) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM products WHERE id = ?", (product_id,))

    # --- Orders ---
    def create_order(
        self,
        user_id: int,
        product: sqlite3.Row,
        quantity: int,
        sell_unit_price: float,
        discount_amount: float,
        total_paid: float,
        coupon_code: str = "",
        supplier_order_id: str = "",
        delivered_data: str = "",
        status: str = "completed",
        source: str = "telegram",
    ) -> Tuple[int, str]:
        order_code = gen_code("ORD", 8)
        cost_unit = float(product["cost_price"])
        net_profit = round(float(total_paid) - (cost_unit * int(quantity)), 4)
        now = utc_now_iso()
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO orders
                (order_code, user_id, product_id, product_name, brand, quantity,
                 cost_unit_price, sell_unit_price, discount_amount, total_paid, net_profit,
                 coupon_code, supplier_id, supplier_order_id, delivered_data, status, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_code, user_id, product["id"], product["name"], product["brand"],
                    int(quantity), cost_unit, float(sell_unit_price), float(discount_amount),
                    float(total_paid), net_profit, coupon_code, product["supplier_id"],
                    supplier_order_id, delivered_data, status, source, now,
                ),
            )
            order_id = int(cur.lastrowid)
            if coupon_code:
                conn.execute("UPDATE coupons SET used_count = used_count + 1 WHERE code = ?", (coupon_code,))
                conn.execute(
                    "INSERT INTO coupon_redemptions(coupon_code, user_id, order_id, redeemed_at) VALUES (?, ?, ?, ?)",
                    (coupon_code, user_id, order_id, now),
                )
            return order_id, order_code

    def update_order_delivery(self, order_id: int, delivered_data: str, supplier_order_id: str = "", status: str = "completed") -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE orders SET delivered_data = ?, supplier_order_id = ?, status = ? WHERE id = ?",
                (delivered_data, supplier_order_id, status, order_id),
            )

    def get_user_orders(self, user_id: int, limit: int = 50) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT ?", (user_id, limit)
            ).fetchall()

    def get_recent_orders(self, limit: int = 50) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM orders ORDER BY id DESC LIMIT ?", (limit,)).fetchall()

    def get_order_by_id(self, order_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()

    def get_order_by_code(self, code: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM orders WHERE UPPER(order_code) = UPPER(?)", (code.strip(),)).fetchone()

    def refund_order(self, order_id: int) -> Tuple[bool, str]:
        order = self.get_order_by_id(order_id)
        if not order:
            return False, "Order not found."
        if order["status"] == "refunded":
            return False, "Order already refunded."
        ok, _ = self.adjust_balance(int(order["user_id"]), float(order["total_paid"]))
        if not ok:
            return False, "Could not credit user balance."
        with self.connect() as conn:
            conn.execute("UPDATE orders SET status = 'refunded', net_profit = 0 WHERE id = ?", (order_id,))
        return True, f"Refunded {fmt_money(order['total_paid'])} to user {order['user_id']}."

    # --- Payment Gateways & Deposits ---
    def list_gateways(self, only_enabled: bool = False) -> List[sqlite3.Row]:
        sql = "SELECT * FROM payment_gateways"
        if only_enabled:
            sql += " WHERE is_enabled = 1"
        with self.connect() as conn:
            return conn.execute(sql).fetchall()

    def get_gateway(self, code: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM payment_gateways WHERE code = ?", (code,)).fetchone()

    def toggle_gateway(self, code: str) -> bool:
        with self.connect() as conn:
            row = conn.execute("SELECT is_enabled FROM payment_gateways WHERE code = ?", (code,)).fetchone()
            if not row:
                return False
            nv = 0 if row["is_enabled"] else 1
            conn.execute("UPDATE payment_gateways SET is_enabled = ? WHERE code = ?", (nv, code))
            return bool(nv)

    def update_gateway_meta(
        self,
        code: str,
        display_name: Optional[str] = None,
        min_amount: Optional[float] = None,
        fee_pct: Optional[float] = None,
    ) -> None:
        with self.connect() as conn:
            if display_name is not None:
                conn.execute("UPDATE payment_gateways SET display_name = ? WHERE code = ?", (display_name.strip(), code))
            if min_amount is not None:
                conn.execute("UPDATE payment_gateways SET min_amount = ? WHERE code = ?", (max(0.01, float(min_amount)), code))
            if fee_pct is not None:
                conn.execute("UPDATE payment_gateways SET fee_pct = ? WHERE code = ?", (max(0.0, float(fee_pct)), code))

    def update_gateway_config(self, code: str, updates: Dict[str, Any]) -> None:
        gw = self.get_gateway(code)
        if not gw:
            return
        cfg = json.loads(gw["config_json"] or "{}")
        cfg.update(updates)
        with self.connect() as conn:
            conn.execute(
                "UPDATE payment_gateways SET config_json = ? WHERE code = ?",
                (json.dumps(cfg), code),
            )

    def create_deposit(
        self,
        user_id: int,
        gateway: str,
        amount_usd: float,
        expected_crypto_amount: Optional[float] = None,
        tx_reference: str = "",
        meta: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, str]:
        code = gen_code("DEP", 8)
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO deposits
                (deposit_code, user_id, gateway, amount_usd, expected_crypto_amount, tx_reference, status, meta_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (code, user_id, gateway, float(amount_usd), expected_crypto_amount, tx_reference, json.dumps(meta or {}), utc_now_iso()),
            )
            return int(cur.lastrowid), code

    def get_deposit(self, deposit_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM deposits WHERE id = ?", (deposit_id,)).fetchone()

    def list_deposits(self, status: Optional[str] = None, limit: int = 40) -> List[sqlite3.Row]:
        with self.connect() as conn:
            if status:
                return conn.execute(
                    "SELECT * FROM deposits WHERE status = ? ORDER BY id DESC LIMIT ?", (status, limit)
                ).fetchall()
            return conn.execute("SELECT * FROM deposits ORDER BY id DESC LIMIT ?", (limit,)).fetchall()

    def confirm_deposit(self, deposit_id: int, tx_ref: str = "") -> Tuple[bool, Optional[sqlite3.Row]]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            dep = conn.execute("SELECT * FROM deposits WHERE id = ?", (deposit_id,)).fetchone()
            if not dep or dep["status"] != "pending":
                conn.execute("ROLLBACK")
                return False, dep
            conn.execute(
                "UPDATE deposits SET status = 'confirmed', tx_reference = COALESCE(NULLIF(?, ''), tx_reference), confirmed_at = ? WHERE id = ?",
                (tx_ref, utc_now_iso(), deposit_id),
            )
            conn.execute("COMMIT")
        self.adjust_balance(int(dep["user_id"]), float(dep["amount_usd"]), is_deposit=True)
        self.credit_referral_commission(int(dep["user_id"]), "deposit", float(dep["amount_usd"]))
        return True, self.get_deposit(deposit_id)

    def is_tx_already_used(self, tx_ref: str, exclude_deposit_id: int = 0) -> bool:
        clean = tx_ref.strip().upper()
        if not clean or clean in ("SCREENSHOT ATTACHED",):
            return False
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM deposits
                WHERE UPPER(tx_reference) = ? AND status IN ('confirmed', 'pending') AND id != ?
                LIMIT 1
                """,
                (clean, exclude_deposit_id),
            ).fetchone()
            return row is not None

    def record_incoming_sms(self, gateway: str, tx_id: str, amount_bdt: float, sender: str = "", raw_text: str = "") -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO incoming_payment_sms(gateway, tx_id, amount_bdt, sender, raw_text, is_claimed, received_at)
                VALUES (?, ?, ?, ?, ?, 0, ?)
                """,
                (gateway, tx_id.strip().upper(), float(amount_bdt), sender, raw_text[:400], utc_now_iso()),
            )

    def claim_matching_sms(self, gateway: str, tx_id: str, min_bdt: float = 0.0) -> bool:
        clean = tx_id.strip().upper()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT id, amount_bdt FROM incoming_payment_sms
                WHERE gateway = ? AND UPPER(tx_id) = ? AND is_claimed = 0
                """,
                (gateway, clean),
            ).fetchone()
            if not row or (min_bdt > 0 and float(row["amount_bdt"]) + 2.0 < min_bdt):
                conn.execute("ROLLBACK")
                return False
            conn.execute("UPDATE incoming_payment_sms SET is_claimed = 1 WHERE id = ?", (row["id"],))
            conn.execute("COMMIT")
            return True

    def reject_deposit(self, deposit_id: int) -> bool:
        with self.connect() as conn:
            cur = conn.execute(
                "UPDATE deposits SET status = 'rejected' WHERE id = ? AND status = 'pending'", (deposit_id,)
            )
            return cur.rowcount > 0

    # --- Coupons ---
    def create_coupon(
        self, code: str, discount_type: str, discount_value: float, min_order: float = 0.0, max_uses: int = 0
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO coupons(code, discount_type, discount_value, min_order, max_uses, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(code) DO UPDATE SET
                    discount_type=excluded.discount_type, discount_value=excluded.discount_value,
                    min_order=excluded.min_order, max_uses=excluded.max_uses, is_active=1
                """,
                (code.strip().upper(), discount_type, float(discount_value), float(min_order), int(max_uses), utc_now_iso()),
            )

    def list_coupons(self) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM coupons ORDER BY created_at DESC").fetchall()

    def delete_coupon(self, code: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM coupons WHERE UPPER(code) = UPPER(?)", (code,))

    def validate_coupon(self, code: str, user_id: int, subtotal: float) -> Tuple[bool, float, str]:
        with self.connect() as conn:
            c = conn.execute("SELECT * FROM coupons WHERE UPPER(code) = UPPER(?)", (code.strip(),)).fetchone()
            if not c or not c["is_active"]:
                return False, 0.0, "Invalid or inactive promo code."
            if int(c["max_uses"]) > 0 and int(c["used_count"]) >= int(c["max_uses"]):
                return False, 0.0, "Promo code usage limit reached."
            if float(subtotal) < float(c["min_order"]):
                return False, 0.0, f"Minimum order {fmt_money(c['min_order'])} required."
            already = conn.execute(
                "SELECT 1 FROM coupon_redemptions WHERE coupon_code = ? AND user_id = ?",
                (c["code"], user_id),
            ).fetchone()
            if already:
                return False, 0.0, "You have already used this promo code."
            if c["discount_type"] == "percent":
                disc = round(subtotal * (float(c["discount_value"]) / 100.0), 2)
            else:
                disc = round(min(float(c["discount_value"]), subtotal), 2)
            return True, disc, f"Promo {c['code']} applied (-{fmt_money(disc)})"

    # --- Tickets ---
    def create_ticket(
        self, user_id: int, subject: str, body: str, photo_file_id: str = "", category: str = "General"
    ) -> Tuple[int, str]:
        code = gen_code("TKT", 6)
        now = utc_now_iso()
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO tickets(ticket_code, user_id, subject, category, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'open', ?, ?)
                """,
                (code, user_id, subject.strip()[:120], category, now, now),
            )
            tid = int(cur.lastrowid)
            conn.execute(
                """
                INSERT INTO ticket_messages(ticket_id, sender_id, sender_role, text, photo_file_id, created_at)
                VALUES (?, ?, 'user', ?, ?, ?)
                """,
                (tid, user_id, body.strip(), photo_file_id, now),
            )
            return tid, code

    def add_ticket_message(
        self, ticket_id: int, sender_id: int, sender_role: str, text: str, photo_file_id: str = ""
    ) -> None:
        now = utc_now_iso()
        new_status = "answered" if sender_role == "admin" else "open"
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO ticket_messages(ticket_id, sender_id, sender_role, text, photo_file_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (ticket_id, sender_id, sender_role, text.strip(), photo_file_id, now),
            )
            conn.execute("UPDATE tickets SET status = ?, updated_at = ? WHERE id = ?", (new_status, now, ticket_id))

    def close_ticket(self, ticket_id: int) -> None:
        with self.connect() as conn:
            conn.execute("UPDATE tickets SET status = 'closed', updated_at = ? WHERE id = ?", (utc_now_iso(), ticket_id))

    def get_ticket(self, ticket_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,)).fetchone()

    def get_ticket_messages(self, ticket_id: int) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM ticket_messages WHERE ticket_id = ? ORDER BY id ASC", (ticket_id,)
            ).fetchall()

    def list_user_tickets(self, user_id: int) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM tickets WHERE user_id = ? ORDER BY id DESC LIMIT 25", (user_id,)).fetchall()

    def list_all_tickets(self, only_open: bool = False) -> List[sqlite3.Row]:
        sql = "SELECT * FROM tickets"
        if only_open:
            sql += " WHERE status != 'closed'"
        sql += " ORDER BY updated_at DESC LIMIT 40"
        with self.connect() as conn:
            return conn.execute(sql).fetchall()

    def count_user_tickets(self, user_id: int) -> Tuple[int, int]:
        with self.connect() as conn:
            active = conn.execute(
                "SELECT COUNT(*) FROM tickets WHERE user_id = ? AND status != 'closed'", (user_id,)
            ).fetchone()[0]
            total = conn.execute("SELECT COUNT(*) FROM tickets WHERE user_id = ?", (user_id,)).fetchone()[0]
            return int(active), int(total)

    # --- Force-Sub & Stock Watcher ---
    def list_force_sub(self, only_active: bool = True) -> List[sqlite3.Row]:
        sql = "SELECT * FROM force_sub_channels" + (" WHERE is_active = 1" if only_active else "")
        with self.connect() as conn:
            return conn.execute(sql).fetchall()

    def add_force_sub(self, chat_id: str, title: str, invite_link: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO force_sub_channels(chat_id, title, invite_link, is_active)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, invite_link=excluded.invite_link, is_active=1
                """,
                (chat_id.strip(), title.strip(), invite_link.strip()),
            )

    def remove_force_sub(self, channel_id: int) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM force_sub_channels WHERE id = ?", (channel_id,))

    def add_stock_watcher(self, user_id: int, product_id: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO stock_watchers(user_id, product_id, notified, created_at)
                VALUES (?, ?, 0, ?)
                ON CONFLICT(user_id, product_id) DO UPDATE SET notified = 0
                """,
                (user_id, product_id, utc_now_iso()),
            )

    def pop_stock_watchers(self, product_id: int) -> List[int]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT user_id FROM stock_watchers WHERE product_id = ? AND notified = 0", (product_id,)
            ).fetchall()
            conn.execute("UPDATE stock_watchers SET notified = 1 WHERE product_id = ?", (product_id,))
            return [int(r["user_id"]) for r in rows]

    # --- Analytics & Audit ---
    def get_dashboard_stats(self) -> Dict[str, Any]:
        today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        with self.connect() as conn:
            total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            total_deposits = conn.execute(
                "SELECT COALESCE(SUM(amount_usd), 0) FROM deposits WHERE status = 'confirmed'"
            ).fetchone()[0]
            pending_deposits = conn.execute(
                "SELECT COUNT(*) FROM deposits WHERE status = 'pending'"
            ).fetchone()[0]
            rev_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt, COALESCE(SUM(total_paid), 0) AS rev,
                       COALESCE(SUM(net_profit), 0) AS profit
                FROM orders WHERE status = 'completed'
                """
            ).fetchone()
            today_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt, COALESCE(SUM(total_paid), 0) AS rev,
                       COALESCE(SUM(net_profit), 0) AS profit
                FROM orders WHERE status = 'completed' AND created_at LIKE ?
                """,
                (f"{today_prefix}%",),
            ).fetchone()
            week_row = conn.execute(
                """
                SELECT COALESCE(SUM(total_paid), 0) AS rev, COALESCE(SUM(net_profit), 0) AS profit
                FROM orders WHERE status = 'completed' AND created_at >= ?
                """,
                (week_ago,),
            ).fetchone()
            open_tickets = conn.execute("SELECT COUNT(*) FROM tickets WHERE status != 'closed'").fetchone()[0]
            staff_count = conn.execute("SELECT COUNT(*) FROM staff_roles").fetchone()[0]
            product_count = conn.execute("SELECT COUNT(*) FROM products WHERE is_active = 1").fetchone()[0]
            watcher_count = conn.execute("SELECT COUNT(*) FROM stock_watchers WHERE notified = 0").fetchone()[0]
            top_products = conn.execute(
                """
                SELECT product_name, SUM(quantity) AS qty, SUM(total_paid) AS rev, SUM(net_profit) AS prof
                FROM orders WHERE status = 'completed'
                GROUP BY product_id ORDER BY rev DESC LIMIT 5
                """
            ).fetchall()

        return {
            "total_users": int(total_users),
            "total_deposits": float(total_deposits),
            "pending_deposits": int(pending_deposits),
            "total_orders": int(rev_row["cnt"]),
            "total_revenue": float(rev_row["rev"]),
            "total_profit": float(rev_row["profit"]),
            "today_orders": int(today_row["cnt"]),
            "today_revenue": float(today_row["rev"]),
            "today_profit": float(today_row["profit"]),
            "week_revenue": float(week_row["rev"]),
            "week_profit": float(week_row["profit"]),
            "open_tickets": int(open_tickets),
            "staff_count": int(staff_count),
            "product_count": int(product_count),
            "watcher_count": int(watcher_count),
            "top_products": [dict(r) for r in top_products],
        }

    def export_orders_csv(self) -> bytes:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "Order Code", "User ID", "Brand", "Product", "Qty",
            "Cost/Unit", "Sell/Unit", "Discount", "Total Paid", "Net Profit",
            "Coupon", "Supplier Order ID", "Status", "Source", "Created At"
        ])
        with self.connect() as conn:
            for r in conn.execute("SELECT * FROM orders ORDER BY id DESC").fetchall():
                writer.writerow([
                    r["order_code"], r["user_id"], r["brand"], r["product_name"], r["quantity"],
                    r["cost_unit_price"], r["sell_unit_price"], r["discount_amount"],
                    r["total_paid"], r["net_profit"], r["coupon_code"],
                    r["supplier_order_id"], r["status"], r["source"], r["created_at"],
                ])
        return buf.getvalue().encode("utf-8")

    def log_audit(self, actor_id: int, action: str, details: str = "") -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO audit_logs(actor_id, action, details, created_at) VALUES (?, ?, ?, ?)",
                (actor_id, action, details[:500], utc_now_iso()),
            )


# ==============================================================================
# 4. UPSTREAM WHOLESALE SUPPLIER API CLIENT
# ==============================================================================

class SupplierClient:
    """
    Universal Async Wholesale Supplier API Connector.
    Compatible with DigiHub / StackVault / standard REST digital product APIs.
    """

    def __init__(self, timeout_sec: int = 25):
        self.timeout_sec = timeout_sec

    def _build_headers(self, supplier: sqlite3.Row) -> Dict[str, str]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        auth_header = (supplier["auth_header"] or "Authorization").strip()
        raw_prefix = supplier["auth_prefix"] if supplier["auth_prefix"] is not None else "Bearer "
        api_key = str(supplier["api_key"] or "").strip()

        # Smart Authorization formatting: prevents 'Bearercsa_...' or 'Bearer Bearer csa_...'
        if auth_header.lower() == "authorization":
            if api_key.lower().startswith("bearer "):
                auth_val = "Bearer " + api_key[7:].strip()
            else:
                pref = raw_prefix.strip() or "Bearer"
                auth_val = f"{pref} {api_key}"
        else:
            pref = raw_prefix.strip()
            if pref:
                auth_val = api_key if api_key.lower().startswith(pref.lower() + " ") else f"{pref} {api_key}"
            else:
                auth_val = api_key

        headers[auth_header] = auth_val
        try:
            extra = json.loads(supplier["extra_headers_json"] or "{}")
            if isinstance(extra, dict):
                headers.update({str(k): str(v) for k, v in extra.items()})
        except Exception:
            pass
        return headers

    async def _request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        payload: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Any, str]:
        if aiohttp is None:
            return False, None, "aiohttp is not installed."
        timeout = aiohttp.ClientTimeout(total=self.timeout_sec)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(method, url, headers=headers, json=payload) as resp:
                    raw_text = await resp.text()
                    try:
                        data = json.loads(raw_text) if raw_text else {}
                    except json.JSONDecodeError:
                        data = {"raw": raw_text}
                    if 200 <= resp.status < 300:
                        return True, data, ""
                    err_msg = (
                        (data.get("error") or data.get("message") or data.get("detail"))
                        if isinstance(data, dict) else None
                    ) or raw_text[:300] or "empty response from supplier"
                    if isinstance(err_msg, (dict, list)):
                        err_msg = json.dumps(err_msg, ensure_ascii=False)[:300]
                    print(f"[SUPPLIER ERROR] {method} {url} payload={payload} -> {resp.status} {raw_text[:500]}")
                    return False, data, f"HTTP {resp.status}: {err_msg}"
        except Exception as exc:
            return False, None, f"Connection error: {exc}"

    @staticmethod
    def parse_balance_response(data: Any) -> Optional[float]:
        if isinstance(data, (int, float)):
            return float(data)
        if not isinstance(data, dict):
            return None
        for key in ("balance", "credit", "reseller_balance", "wallet_balance", "usdt_balance"):
            if key in data and data[key] is not None:
                try:
                    return float(data[key])
                except (TypeError, ValueError):
                    pass
        if isinstance(data.get("data"), dict):
            return SupplierClient.parse_balance_response(data["data"])
        return None

    @staticmethod
    def parse_products_response(data: Any) -> List[Dict[str, Any]]:
        raw_list: List[Any] = []
        if isinstance(data, list):
            raw_list = data
        elif isinstance(data, dict):
            for key in ("products", "items", "catalog", "data", "results"):
                val = data.get(key)
                if isinstance(val, list):
                    raw_list = val
                    break
                if isinstance(val, dict):
                    for subkey in ("products", "items", "list"):
                        if isinstance(val.get(subkey), list):
                            raw_list = val[subkey]
                            break

        normalized: List[Dict[str, Any]] = []
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            ext_id = item.get("id") or item.get("product_id") or item.get("sku") or item.get("code")
            name = item.get("name") or item.get("title") or item.get("product_name")
            if not ext_id or not name:
                continue
            price_raw = item.get("price") or item.get("cost") or item.get("unit_price") or item.get("amount") or 0
            stock_raw = item.get("stock") if item.get("stock") is not None else item.get("quantity", item.get("available", 0))
            try:
                price = float(price_raw)
            except (TypeError, ValueError):
                price = 0.0
            try:
                stock = int(stock_raw)
            except (TypeError, ValueError):
                stock = 0
            normalized.append({
                "external_id": str(ext_id),
                "name": str(name).strip(),
                "brand": item.get("brand") or item.get("category") or item.get("group") or "",
                "cost_price": price,
                "stock": max(0, stock),
                "description": str(item.get("description") or item.get("details") or "").strip(),
                "warranty_text": str(item.get("warranty") or item.get("warranty_text") or "Instant Delivery").strip(),
            })
        return normalized

    @staticmethod
    def parse_order_response(data: Any) -> Tuple[str, str]:
        """Extract (supplier_order_id, delivered_credentials_text) from flexible API formats."""
        if not isinstance(data, dict):
            return "", str(data)
        root = data.get("data") if isinstance(data.get("data"), dict) else data
        sup_oid = str(
            root.get("order_id") or root.get("id") or root.get("reference")
            or data.get("order_id") or ""
        )
        for key in ("credentials", "items", "keys", "accounts", "delivered", "delivery", "content", "account", "key", "code"):
            val = root.get(key)
            if val:
                if isinstance(val, list):
                    lines = [
                        json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else str(x)
                        for x in val
                    ]
                    return sup_oid, "\n".join(lines)
                if isinstance(val, dict):
                    return sup_oid, "\n".join(f"{k}: {v}" for k, v in val.items())
                return sup_oid, str(val)
        return sup_oid, json.dumps(root, indent=2, ensure_ascii=False)

    async def fetch_balance(self, supplier: sqlite3.Row) -> Tuple[bool, float, str]:
        url = f"{supplier['base_url'].rstrip('/')}/{supplier['balance_endpoint'].lstrip('/')}"
        ok, data, err = await self._request("GET", url, self._build_headers(supplier))
        if not ok:
            return False, 0.0, err
        bal = self.parse_balance_response(data)
        if bal is None:
            return False, 0.0, f"Unexpected balance payload: {str(data)[:160]}"
        return True, bal, ""

    async def sync_catalog(self, db: DatabaseManager, supplier: sqlite3.Row) -> Tuple[bool, int, List[int], str]:
        """Fetch products from upstream supplier, upsert to DB, return restocked product IDs."""
        url = f"{supplier['base_url'].rstrip('/')}/{supplier['products_endpoint'].lstrip('/')}"
        ok, data, err = await self._request("GET", url, self._build_headers(supplier))
        if not ok:
            return False, 0, [], err
        items = self.parse_products_response(data)
        restocked_ids: List[Tuple[int, int]] = []
        for it in items:
            pid, added_qty = db.upsert_supplier_product(
                supplier_id=int(supplier["id"]),
                external_id=it["external_id"],
                name=it["name"],
                cost_price=it["cost_price"],
                stock=it["stock"],
                brand=it["brand"],
                description=it["description"],
                warranty_text=it["warranty_text"],
            )
            if added_qty > 0:
                restocked_ids.append((pid, added_qty))

        # Also refresh reseller balance
        b_ok, bal, _ = await self.fetch_balance(supplier)
        if b_ok:
            db.update_supplier_balance(int(supplier["id"]), bal)
        return True, len(items), restocked_ids, ""

    async def purchase_item(
        self, supplier: sqlite3.Row, external_id: str, quantity: int = 1
    ) -> Tuple[bool, str, str, str]:
        """Auto-purchase from upstream supplier. Returns (ok, supplier_order_id, delivered_text, error).

        Robust version: tries several common payload formats and endpoint names,
        sends an Idempotency-Key (prevents double purchase on retry) and reports
        the supplier's REAL error text instead of 'None'.
        """
        base = supplier["base_url"].rstrip("/")
        main_ep = "/" + str(supplier["order_endpoint"] or "/order").lstrip("/")
        endpoints = [main_ep]
        for alt in ("/order", "/orders", "/purchase", "/buy", "/order/create"):
            if alt not in endpoints:
                endpoints.append(alt)

        qty = int(quantity)
        ext_str = str(external_id)
        ext_val: Any = int(ext_str) if ext_str.isdigit() and len(ext_str) < 16 else ext_str
        payload_variants = [
            {"product_id": ext_str, "quantity": qty},
            {"product_id": ext_val, "quantity": qty},
            {"id": ext_str, "quantity": qty},
            {"product_id": ext_str, "qty": qty},
            {"productId": ext_str, "quantity": qty},
            {"sku": ext_str, "quantity": qty},
            {"product_id": ext_str, "id": ext_str, "quantity": qty},
        ]

        headers = self._build_headers(supplier)
        headers["Idempotency-Key"] = f"order-{ext_str}-{secrets.token_hex(8)}"

        last_err = "Unknown supplier error"
        for ep_idx, ep in enumerate(endpoints):
            url = f"{base}{ep}"
            endpoint_missing = False
            for payload in payload_variants:
                ok, data, err = await self._request("POST", url, headers, payload=payload)
                if ok:
                    if isinstance(data, dict) and data.get("success") is False:
                        err = str(data.get("error") or data.get("message") or json.dumps(data, ensure_ascii=False)[:200])
                        last_err = err
                        # Business error (out of stock / no balance etc.): stop, do not retry
                        return False, "", "", err
                    sup_oid, delivered = self.parse_order_response(data)
                    return True, sup_oid, delivered, ""
                last_err = err
                if "HTTP 404" in err or "HTTP 405" in err:
                    endpoint_missing = True
                    break  # wrong endpoint path -> try next endpoint
                if "HTTP 401" in err or "HTTP 403" in err or "Connection error" in err:
                    return False, "", "", err  # auth / network problem: retrying is pointless
                if not any(c in err for c in ("HTTP 400", "HTTP 422", "HTTP 500", "HTTP 502")):
                    return False, "", "", err
            if not endpoint_missing and ep_idx == 0:
                # Configured endpoint exists but rejected every payload format
                break
        return False, "", "", last_err[:300]


# ==============================================================================
# 5. MULTI-GATEWAY PAYMENT ENGINE
# ==============================================================================

USDT_BEP20_CONTRACT = "0x55d398326f99059fF775485246999027B3197955"


class PaymentEngine:
    """Handles automated & manual deposit invoice creation and verification."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    async def create_invoice(
        self, user_id: int, gateway_code: str, amount_usd: float
    ) -> Tuple[bool, Dict[str, Any], str]:
        gw = self.db.get_gateway(gateway_code)
        if not gw or not gw["is_enabled"]:
            return False, {}, "Selected payment gateway is currently disabled."
        if amount_usd < float(gw["min_amount"]):
            return False, {}, f"Minimum deposit for this method is {fmt_money(gw['min_amount'])}."

        gcfg = json.loads(gw["config_json"] or "{}")

        # --- 1. Binance Pay ---
        if gateway_code == "binance_pay":
            api_key = gcfg.get("api_key", "").strip()
            secret_key = gcfg.get("secret_key", "").strip()
            pay_id = gcfg.get("pay_id", "").strip()

            if api_key and secret_key and aiohttp is not None:
                dep_id, dep_code = self.db.create_deposit(user_id, gateway_code, amount_usd)
                ok, res, err = await self._binance_create_order(api_key, secret_key, dep_code, amount_usd)
                if ok:
                    meta = {
                        "mode": "api",
                        "prepay_id": res.get("prepayId", ""),
                        "checkout_url": res.get("checkoutUrl", ""),
                    }
                    with self.db.connect() as conn:
                        conn.execute("UPDATE deposits SET meta_json = ? WHERE id = ?", (json.dumps(meta), dep_id))
                    return True, {
                        "deposit_id": dep_id,
                        "deposit_code": dep_code,
                        "mode": "api",
                        "pay_url": res.get("checkoutUrl", ""),
                        "instructions": (
                            f"🔶 <b>Binance Pay Invoice</b>\n"
                            f"• Reference: <code>{dep_code}</code>\n"
                            f"• Amount: <b>{fmt_money(amount_usd)} USDT</b> (0% Fee)\n\n"
                            f"Tap the button below to pay via Binance Pay, then tap <b>Check Payment</b>."
                        ),
                    }, ""
                # If API failed, fall through to Pay ID if available
                if not pay_id:
                    self.db.reject_deposit(dep_id)
                    return False, {}, f"Binance Pay API error: {err}"

            if not pay_id:
                return False, {}, "Binance Pay is not configured yet. Ask Admin to set Pay ID or API keys."

            dep_id, dep_code = self.db.create_deposit(user_id, gateway_code, amount_usd, meta={"mode": "manual"})
            return True, {
                "deposit_id": dep_id,
                "deposit_code": dep_code,
                "mode": "manual",
                "instructions": (
                    f"🔶 <b>Binance Pay (Instant 0% Fee)</b>\n"
                    f"• Send exact amount: <b>{fmt_money(amount_usd)} USDT</b>\n"
                    f"• Binance Pay ID: <code>{esc(pay_id)}</code>\n"
                    f"• Note/Memo: <code>{dep_code}</code>\n\n"
                    f"After sending, tap <b>Submit Transaction ID</b> below for fast credit."
                ),
            }, ""

        # --- 2. USDT BEP-20 (Auto-Detect via unique micro-amount) ---
        if gateway_code == "usdt_bep20":
            wallet = gcfg.get("wallet_address", "").strip()
            if not wallet:
                return False, {}, "USDT BEP-20 wallet address is not configured by Admin yet."
            micro = round(random.randint(11, 989) / 10000.0, 4)
            exact_usdt = round(amount_usd + micro, 4)
            dep_id, dep_code = self.db.create_deposit(
                user_id,
                gateway_code,
                amount_usd,
                expected_crypto_amount=exact_usdt,
                meta={"wallet": wallet, "network": "BEP-20"},
            )
            return True, {
                "deposit_id": dep_id,
                "deposit_code": dep_code,
                "mode": "auto_bep20",
                "instructions": (
                    f"💎 <b>USDT BEP-20 (Auto-Detect)</b>\n"
                    f"• Deposit ID: <code>{dep_code}</code>\n"
                    f"• Network: <b>BNB Smart Chain (BEP-20)</b>\n"
                    f"• Send EXACTLY: <code>{exact_usdt:.4f}</code> <b>USDT</b>\n"
                    f"• Wallet Address:\n<code>{esc(wallet)}</code>\n\n"
                    f"⚠️ Send the <b>exact decimal amount</b> above so our auto-detector credits your balance automatically (1–2 mins). "
                    f"You can also tap <b>Verify Payment</b> or submit TxHash."
                ),
            }, ""

        # --- 3. CryptoBot (@CryptoBot Crypto Pay API) ---
        if gateway_code == "cryptobot":
            token = gcfg.get("api_token", "").strip()
            if not token:
                return False, {}, "CryptoBot API token is not configured by Admin yet."
            dep_id, dep_code = self.db.create_deposit(user_id, gateway_code, amount_usd)
            ok, inv, err = await self._cryptobot_create_invoice(token, amount_usd, dep_code)
            if not ok:
                self.db.reject_deposit(dep_id)
                return False, {}, f"CryptoBot error: {err}"
            meta = {"invoice_id": inv.get("invoice_id"), "pay_url": inv.get("bot_invoice_url") or inv.get("pay_url")}
            with self.db.connect() as conn:
                conn.execute("UPDATE deposits SET meta_json = ? WHERE id = ?", (json.dumps(meta), dep_id))
            return True, {
                "deposit_id": dep_id,
                "deposit_code": dep_code,
                "mode": "api",
                "pay_url": meta["pay_url"],
                "instructions": (
                    f"🤖 <b>CryptoBot Invoice</b>\n"
                    f"• Reference: <code>{dep_code}</code>\n"
                    f"• Amount: <b>{fmt_money(amount_usd)} USDT</b>\n\n"
                    f"Tap <b>Pay Now</b> to complete payment in @CryptoBot, then tap <b>Check Payment</b>."
                ),
            }, ""

        # --- 4. NOWPayments ---
        if gateway_code == "nowpayments":
            api_key = gcfg.get("api_key", "").strip()
            if not api_key:
                return False, {}, "NOWPayments API key is not configured by Admin yet."
            dep_id, dep_code = self.db.create_deposit(user_id, gateway_code, amount_usd)
            ok, inv, err = await self._nowpayments_create_invoice(api_key, amount_usd, dep_code)
            if not ok:
                self.db.reject_deposit(dep_id)
                return False, {}, f"NOWPayments error: {err}"
            meta = {"invoice_id": inv.get("id"), "pay_url": inv.get("invoice_url")}
            with self.db.connect() as conn:
                conn.execute("UPDATE deposits SET meta_json = ? WHERE id = ?", (json.dumps(meta), dep_id))
            return True, {
                "deposit_id": dep_id,
                "deposit_code": dep_code,
                "mode": "api",
                "pay_url": meta["pay_url"],
                "instructions": (
                    f"🌐 <b>NOWPayments Crypto Invoice</b>\n"
                    f"• Reference: <code>{dep_code}</code>\n"
                    f"• Amount: <b>{fmt_money(amount_usd)} USD</b>\n\n"
                    f"Tap <b>Pay Now</b> to pay with USDT, LTC, TRX, BTC, or TON, then tap <b>Check Payment</b>."
                ),
            }, ""

        # --- 4b. Custom Wallet / Multi-Method Manual Instructions ---
        if gateway_code == "wallet_manual":
            instr = gcfg.get("instructions", "").strip()
            if not instr:
                return False, {}, "Wallet instructions are not configured by Admin yet."
            dep_id, dep_code = self.db.create_deposit(user_id, gateway_code, amount_usd, meta={"mode": "manual"})
            return True, {
                "deposit_id": dep_id,
                "deposit_code": dep_code,
                "mode": "manual",
                "instructions": (
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"💼 <b>WALLET DEPOSIT</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"{esc(instr)}\n\n"
                    f"• Reference: <code>{dep_code}</code>\n"
                    f"• Amount: <b>{fmt_money(amount_usd)} USD</b>\n\n"
                    f"After sending, tap <b>Submit Transaction ID / Proof</b> below."
                ),
            }, ""

        # --- 5. bKash / Nagad Manual (BD) ---
        if gateway_code in ("bkash_manual", "nagad_manual"):
            number = gcfg.get("number", "").strip()
            acc_type = gcfg.get("account_type", "Personal / Send Money")
            if not number:
                return False, {}, f"{gw['display_name']} number is not configured by Admin yet."
            rate = self.db.get_setting_float("bdt_per_usd", 125.0)
            bdt_amount = round(amount_usd * rate)
            dep_id, dep_code = self.db.create_deposit(
                user_id,
                gateway_code,
                amount_usd,
                meta={"mode": "manual", "bdt_amount": bdt_amount, "rate": rate},
            )
            label = "bKash" if gateway_code == "bkash_manual" else "Nagad"
            return True, {
                "deposit_id": dep_id,
                "deposit_code": dep_code,
                "mode": "manual",
                "instructions": (
                    f"📱 <b>{label} Deposit</b>\n"
                    f"• Reference: <code>{dep_code}</code>\n"
                    f"• Credit Amount: <b>{fmt_money(amount_usd)} USD</b>\n"
                    f"• Exchange Rate: 1 USD = {rate:.0f} BDT\n"
                    f"• Send Exact BDT: <b>৳{bdt_amount:,} BDT</b>\n"
                    f"• {label} Number ({esc(acc_type)}): <code>{esc(number)}</code>\n\n"
                    f"After sending money, tap <b>Submit Transaction ID</b> below."
                ),
            }, ""

        return False, {}, "Unsupported payment gateway."

    async def verify_submitted_tx(
        self, deposit: sqlite3.Row, tx_ref: str, has_photo: bool = False
    ) -> Tuple[str, str]:
        """
        Validate & auto-verify a user-submitted Transaction ID / TxHash.
        Returns (outcome, message) where outcome is:
          - 'confirmed': Payment verified automatically & balance credited!
          - 'pending_admin': Format valid & unique; queued for instant owner confirmation.
          - 'invalid': Bad format or duplicate TxID (rejected immediately).
        """
        gw = deposit["gateway"]
        clean_tx = tx_ref.strip()
        if not clean_tx and not has_photo:
            return "invalid", "Please enter a valid Transaction ID (TrxID / TxHash)."

        # 1. Anti-duplicate protection across all gateways
        if clean_tx and self.db.is_tx_already_used(clean_tx, exclude_deposit_id=int(deposit["id"])):
            return "invalid", "⚠️ This Transaction ID has already been used! Duplicate TrxID is not allowed."

        # 2. Gateway-specific format validation + live auto-verification
        if gw == "bkash_manual":
            if not has_photo and not re.fullmatch(r"[A-Za-z0-9]{8,14}", clean_tx):
                return "invalid", "⚠️ Invalid bKash TrxID format. Example: <code>C7A8B9D0E1</code> (8–14 letters/digits)."
            meta = json.loads(deposit["meta_json"] or "{}")
            expected_bdt = float(meta.get("bdt_amount") or 0)
            if clean_tx and self.db.claim_matching_sms("bkash_manual", clean_tx, expected_bdt):
                self.db.confirm_deposit(int(deposit["id"]), clean_tx.upper())
                return "confirmed", "✅ bKash TrxID verified automatically!"
            return "pending_admin", "bKash TrxID validated & sent for instant verification."

        if gw == "nagad_manual":
            if not has_photo and not re.fullmatch(r"[A-Za-z0-9]{8,14}", clean_tx):
                return "invalid", "⚠️ Invalid Nagad TrxID format. Example: <code>72A9B4C1</code> (8–14 letters/digits)."
            meta = json.loads(deposit["meta_json"] or "{}")
            expected_bdt = float(meta.get("bdt_amount") or 0)
            if clean_tx and self.db.claim_matching_sms("nagad_manual", clean_tx, expected_bdt):
                self.db.confirm_deposit(int(deposit["id"]), clean_tx.upper())
                return "confirmed", "✅ Nagad TrxID verified automatically!"
            return "pending_admin", "Nagad TrxID validated & sent for instant verification."

        if gw == "binance_pay":
            if not has_photo and not re.fullmatch(r"[A-Za-z0-9_-]{6,32}", clean_tx):
                return "invalid", "⚠️ Invalid Binance Pay Order / Transaction ID format."
            paid, msg = await self.verify_deposit(deposit)
            if paid:
                return "confirmed", msg
            return "pending_admin", "Binance Pay ID validated & sent for instant verification."

        if gw == "usdt_bep20":
            # Live on-chain verification via public BNB Smart Chain JSON-RPC (NO API key needed!)
            if re.fullmatch(r"0x[0-9a-fA-F]{64}", clean_tx):
                gw_row = self.db.get_gateway("usdt_bep20")
                gcfg = json.loads(gw_row["config_json"] or "{}") if gw_row else {}
                wallet = gcfg.get("wallet_address", "").strip()
                expected = float(deposit["expected_crypto_amount"] or deposit["amount_usd"])
                ok_chain, chain_msg = await self._bsc_rpc_verify_usdt_txhash(clean_tx, wallet, expected)
                if ok_chain:
                    self.db.confirm_deposit(int(deposit["id"]), clean_tx)
                    return "confirmed", chain_msg
                return "pending_admin", f"TxHash recorded ({chain_msg}). Sent for admin check."
            if not has_photo:
                return "invalid", "⚠️ Invalid USDT BEP-20 TxHash. It must start with <code>0x</code> (66 characters)."
            return "pending_admin", "USDT BEP-20 proof recorded for verification."

        return "pending_admin", "Payment proof recorded for verification."

    async def _bsc_rpc_verify_usdt_txhash(
        self, tx_hash: str, target_wallet: str, expected_usdt: float
    ) -> Tuple[bool, str]:
        """Verify a BEP-20 USDT transfer directly on BNB Smart Chain via public JSON-RPC."""
        if aiohttp is None or not target_wallet:
            return False, "RPC unavailable"
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash],
        }
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.post("https://bsc-dataseed.binance.org/", json=payload) as resp:
                    data = await resp.json(content_type=None)
                    receipt = data.get("result")
                    if not isinstance(receipt, dict):
                        return False, "Transaction not confirmed on BSC yet"
                    if receipt.get("status") != "0x1":
                        return False, "Blockchain transaction failed"
                    target_norm = target_wallet.lower().replace("0x", "").rjust(64, "0")
                    for log in receipt.get("logs") or []:
                        if str(log.get("address", "")).lower() != USDT_BEP20_CONTRACT.lower():
                            continue
                        topics = log.get("topics") or []
                        if len(topics) >= 3 and topics[0].lower() == transfer_topic:
                            to_addr = topics[2].lower().replace("0x", "").rjust(64, "0")
                            if to_addr == target_norm:
                                raw_val = int(log.get("data", "0x0"), 16)
                                usdt_val = raw_val / (10 ** 18)
                                if usdt_val + 0.02 >= expected_usdt:
                                    return True, f"Verified {usdt_val:.4f} USDT on BNB Smart Chain!"
                                return False, f"Amount mismatch: received {usdt_val:.4f} USDT, expected {expected_usdt:.4f}"
                    return False, "No USDT transfer to store wallet found in this TxHash"
        except Exception as exc:
            return False, f"BSC check pending ({exc})"

    async def verify_deposit(self, deposit: sqlite3.Row) -> Tuple[bool, str]:
        """Attempt automated verification for a pending deposit."""
        if deposit["status"] != "pending":
            return deposit["status"] == "confirmed", f"Deposit status is already: {deposit['status']}"

        gw = self.db.get_gateway(deposit["gateway"])
        if not gw:
            return False, "Gateway not found."
        gcfg = json.loads(gw["config_json"] or "{}")
        meta = json.loads(deposit["meta_json"] or "{}")

        if deposit["gateway"] == "binance_pay" and meta.get("mode") == "api":
            ok, paid, tx_id = await self._binance_query_order(
                gcfg.get("api_key", ""), gcfg.get("secret_key", ""), deposit["deposit_code"]
            )
            if ok and paid:
                self.db.confirm_deposit(int(deposit["id"]), tx_id)
                return True, "Binance Pay payment confirmed!"
            return False, "Payment not received yet on Binance Pay."

        if deposit["gateway"] == "cryptobot":
            inv_id = meta.get("invoice_id")
            if inv_id:
                ok, paid = await self._cryptobot_check_invoice(gcfg.get("api_token", ""), inv_id)
                if ok and paid:
                    self.db.confirm_deposit(int(deposit["id"]), f"CB-{inv_id}")
                    return True, "CryptoBot payment confirmed!"
            return False, "Invoice is still unpaid on CryptoBot."

        if deposit["gateway"] == "usdt_bep20":
            wallet = gcfg.get("wallet_address", "").strip()
            bsc_key = gcfg.get("bscscan_api_key", "").strip()
            expected = float(deposit["expected_crypto_amount"] or deposit["amount_usd"])
            if wallet and bsc_key:
                found, tx_hash = await self._bscscan_find_usdt_tx(wallet, bsc_key, expected)
                if found:
                    self.db.confirm_deposit(int(deposit["id"]), tx_hash)
                    return True, f"USDT BEP-20 transfer confirmed! Tx: {tx_hash[:14]}..."
            return False, "No matching BEP-20 USDT transfer detected yet. Wait 1–2 mins or submit TxHash."

        return False, "Awaiting admin verification (manual gateway)."

    # --- Low-level Gateway HTTP helpers ---
    async def _binance_create_order(
        self, api_key: str, secret_key: str, merchant_trade_no: str, amount_usd: float
    ) -> Tuple[bool, Dict[str, Any], str]:
        if aiohttp is None:
            return False, {}, "aiohttp unavailable"
        ts = str(int(time.time() * 1000))
        nonce = secrets.token_hex(16)
        body = {
            "env": {"terminalType": "WEB"},
            "merchantTradeNo": re.sub(r"[^A-Za-z0-9]", "", merchant_trade_no)[:32],
            "orderAmount": round(amount_usd, 2),
            "currency": "USDT",
            "goods": {
                "goodsType": "02",
                "goodsCategory": "Z000",
                "referenceGoodsId": merchant_trade_no,
                "goodsName": f"Store Balance Top-Up {merchant_trade_no}",
            },
        }
        payload_str = json.dumps(body, separators=(",", ":"))
        sig_payload = f"{ts}\n{nonce}\n{payload_str}\n"
        signature = hmac.new(secret_key.encode(), sig_payload.encode(), hashlib.sha512).hexdigest().upper()
        headers = {
            "Content-Type": "application/json",
            "BinancePay-Timestamp": ts,
            "BinancePay-Nonce": nonce,
            "BinancePay-Certificate-SN": api_key,
            "BinancePay-Signature": signature,
        }
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
                async with session.post(
                    "https://bpay.binanceapi.com/binancepay/openapi/v2/order",
                    data=payload_str,
                    headers=headers,
                ) as resp:
                    data = await resp.json(content_type=None)
                    if data.get("status") == "SUCCESS" and data.get("data"):
                        return True, data["data"], ""
                    return False, {}, str(data.get("errorMessage") or data)
        except Exception as exc:
            return False, {}, str(exc)

    async def _binance_query_order(
        self, api_key: str, secret_key: str, merchant_trade_no: str
    ) -> Tuple[bool, bool, str]:
        if aiohttp is None or not api_key or not secret_key:
            return False, False, ""
        ts = str(int(time.time() * 1000))
        nonce = secrets.token_hex(16)
        body = {"merchantTradeNo": re.sub(r"[^A-Za-z0-9]", "", merchant_trade_no)[:32]}
        payload_str = json.dumps(body, separators=(",", ":"))
        sig_payload = f"{ts}\n{nonce}\n{payload_str}\n"
        signature = hmac.new(secret_key.encode(), sig_payload.encode(), hashlib.sha512).hexdigest().upper()
        headers = {
            "Content-Type": "application/json",
            "BinancePay-Timestamp": ts,
            "BinancePay-Nonce": nonce,
            "BinancePay-Certificate-SN": api_key,
            "BinancePay-Signature": signature,
        }
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.post(
                    "https://bpay.binanceapi.com/binancepay/openapi/v2/order/query",
                    data=payload_str,
                    headers=headers,
                ) as resp:
                    data = await resp.json(content_type=None)
                    if data.get("status") == "SUCCESS" and isinstance(data.get("data"), dict):
                        st = data["data"].get("status")
                        return True, (st == "PAID"), str(data["data"].get("transactionId") or "")
        except Exception:
            pass
        return False, False, ""

    async def _cryptobot_create_invoice(
        self, token: str, amount_usd: float, dep_code: str
    ) -> Tuple[bool, Dict[str, Any], str]:
        if aiohttp is None:
            return False, {}, "aiohttp unavailable"
        headers = {"Crypto-Pay-API-Token": token}
        payload = {
            "asset": "USDT",
            "amount": f"{amount_usd:.2f}",
            "description": f"Balance Top-Up {dep_code}",
            "payload": dep_code,
        }
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
                async with session.post("https://pay.crypt.bot/api/createInvoice", headers=headers, json=payload) as resp:
                    data = await resp.json(content_type=None)
                    if data.get("ok") and data.get("result"):
                        return True, data["result"], ""
                    return False, {}, str(data.get("error") or data)
        except Exception as exc:
            return False, {}, str(exc)

    async def _cryptobot_check_invoice(self, token: str, invoice_id: Any) -> Tuple[bool, bool]:
        if aiohttp is None or not token:
            return False, False
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.get(
                    f"https://pay.crypt.bot/api/getInvoices?invoice_ids={invoice_id}",
                    headers={"Crypto-Pay-API-Token": token},
                ) as resp:
                    data = await resp.json(content_type=None)
                    items = (data.get("result") or {}).get("items") or []
                    if items and items[0].get("status") == "paid":
                        return True, True
                    return True, False
        except Exception:
            return False, False

    async def _nowpayments_create_invoice(
        self, api_key: str, amount_usd: float, dep_code: str
    ) -> Tuple[bool, Dict[str, Any], str]:
        if aiohttp is None:
            return False, {}, "aiohttp unavailable"
        headers = {"x-api-key": api_key, "Content-Type": "application/json"}
        payload = {
            "price_amount": round(amount_usd, 2),
            "price_currency": "usd",
            "order_id": dep_code,
            "order_description": f"Store Deposit {dep_code}",
        }
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
                async with session.post("https://api.nowpayments.io/v1/invoice", headers=headers, json=payload) as resp:
                    data = await resp.json(content_type=None)
                    if data.get("invoice_url"):
                        return True, data, ""
                    return False, {}, str(data.get("message") or data)
        except Exception as exc:
            return False, {}, str(exc)

    async def _bscscan_find_usdt_tx(
        self, wallet_address: str, api_key: str, expected_amount: float
    ) -> Tuple[bool, str]:
        if aiohttp is None:
            return False, ""
        url = (
            "https://api.bscscan.com/api?module=account&action=tokentx"
            f"&contractaddress={USDT_BEP20_CONTRACT}&address={wallet_address}"
            f"&page=1&offset=20&sort=desc&apikey={api_key}"
        )
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.get(url) as resp:
                    data = await resp.json(content_type=None)
                    txs = data.get("result")
                    if not isinstance(txs, list):
                        return False, ""
                    for tx in txs:
                        if str(tx.get("to", "")).lower() != wallet_address.lower():
                            continue
                        decimals = int(tx.get("tokenDecimal") or 18)
                        val = int(tx.get("value") or 0) / (10 ** decimals)
                        if abs(val - expected_amount) <= 0.0002:
                            tx_hash = str(tx.get("hash") or "")
                            # Ensure tx_hash not already used
                            with self.db.connect() as conn:
                                used = conn.execute(
                                    "SELECT 1 FROM deposits WHERE tx_reference = ? AND status = 'confirmed'",
                                    (tx_hash,),
                                ).fetchone()
                            if not used:
                                return True, tx_hash
        except Exception:
            pass
        return False, ""


# ==============================================================================
# 6. ORDER FULFILLMENT SERVICE (LOCAL STOCK + UPSTREAM AUTO-PURCHASE)
# ==============================================================================

class FulfillmentService:
    """Executes atomic customer purchases with profit margin & upstream auto-buying."""

    def __init__(self, db: DatabaseManager, supplier_client: SupplierClient):
        self.db = db
        self.supplier_client = supplier_client

    def compute_quote(
        self, product: sqlite3.Row, quantity: int, user_id: int, coupon_code: str = ""
    ) -> Tuple[bool, Dict[str, Any], str]:
        if not product or not product["is_active"]:
            return False, {}, "Product is no longer available."
        if quantity < 1:
            return False, {}, "Quantity must be at least 1."
        if int(product["stock"]) < quantity:
            return False, {}, f"Only {product['stock']} unit(s) currently in stock."

        global_margin = self.db.get_setting_float("default_margin_pct", 35.0)
        unit_price = calculate_sell_price(
            float(product["cost_price"]),
            global_margin,
            product["margin_override_pct"],
            product["fixed_price"],
        )
        subtotal = round(unit_price * quantity, 2)
        discount = 0.0
        applied_coupon = ""
        if coupon_code:
            ok_c, disc, msg_c = self.db.validate_coupon(coupon_code, user_id, subtotal)
            if not ok_c:
                return False, {}, msg_c
            discount = disc
            applied_coupon = coupon_code.strip().upper()
        total = round(max(0.01, subtotal - discount), 2)
        return True, {
            "unit_price": unit_price,
            "subtotal": subtotal,
            "discount": discount,
            "total": total,
            "coupon_code": applied_coupon,
        }, ""

    async def execute_purchase(
        self,
        user_id: int,
        product_id: int,
        quantity: int = 1,
        coupon_code: str = "",
        source: str = "telegram",
    ) -> Tuple[bool, Dict[str, Any], str]:
        product = self.db.get_product(product_id)
        if not product:
            return False, {}, "Product not found."

        ok_q, quote, err_q = self.compute_quote(product, quantity, user_id, coupon_code)
        if not ok_q:
            return False, {}, err_q

        total = quote["total"]
        deducted, new_bal = self.db.adjust_balance(
            user_id, -total, is_spend=True, require_sufficient=True
        )
        if not deducted:
            return False, {}, f"Insufficient balance. Need {fmt_money(total)}, you have {fmt_money(new_bal)}."

        # Create pending order record first
        order_id, order_code = self.db.create_order(
            user_id=user_id,
            product=product,
            quantity=quantity,
            sell_unit_price=quote["unit_price"],
            discount_amount=quote["discount"],
            total_paid=total,
            coupon_code=quote["coupon_code"],
            status="pending",
            source=source,
        )

        # Route A: Custom / Local Inventory Delivery
        if product["delivery_type"] == "local_stock" or product["is_custom"]:
            creds = self.db.consume_local_stock(product_id, quantity, order_id)
            if not creds:
                # Atomic refund
                self.db.adjust_balance(user_id, total)
                self.db.update_order_delivery(order_id, "", "", status="failed")
                return False, {}, "Item just went out of stock. Your balance was automatically refunded."
            delivered_text = "\n".join(creds)
            self.db.update_order_delivery(order_id, delivered_text, "LOCAL", status="completed")
            self.db.credit_referral_commission(user_id, "order", total)
            return True, {
                "order_id": order_id,
                "order_code": order_code,
                "product_name": product["name"],
                "quantity": quantity,
                "total_paid": total,
                "delivered_data": delivered_text,
                "new_balance": new_bal,
            }, ""

        # Route B: Upstream Wholesale Supplier Auto-Purchase
        supplier = self.db.get_supplier(int(product["supplier_id"])) if product["supplier_id"] else None
        if not supplier or not supplier["is_active"]:
            self.db.adjust_balance(user_id, total)
            self.db.update_order_delivery(order_id, "", "", status="failed")
            return False, {}, "Upstream supplier is currently unavailable. Your balance has been refunded."

        s_ok, sup_oid, delivered_text, s_err = await self.supplier_client.purchase_item(
            supplier, str(product["external_id"]), quantity
        )
        if not s_ok:
            # Automatic atomic refund to customer
            self.db.adjust_balance(user_id, total)
            self.db.update_order_delivery(order_id, f"FAILED: {s_err}", "", status="failed")
            logger.error("Upstream purchase failed for %s: %s", order_code, s_err)
            return False, {}, f"Upstream fulfillment failed ({s_err}). Your balance was automatically refunded."

        self.db.update_order_delivery(order_id, delivered_text, sup_oid, status="completed")
        # Decrement cached local stock count immediately
        with self.db.connect() as conn:
            conn.execute(
                "UPDATE products SET stock = MAX(0, stock - ?), updated_at = ? WHERE id = ?",
                (quantity, utc_now_iso(), product_id),
            )
        self.db.credit_referral_commission(user_id, "order", total)
        return True, {
            "order_id": order_id,
            "order_code": order_code,
            "product_name": product["name"],
            "quantity": quantity,
            "total_paid": total,
            "delivered_data": delivered_text,
            "supplier_order_id": sup_oid,
            "new_balance": new_bal,
        }, ""


# ==============================================================================
# 7. BUILT-IN RESELLER & WEBSITE REST API SERVER (OPTIONAL aiohttp.web)
# ==============================================================================

class WebAPIServer:
    """
    Lightweight REST API allowing your future Website & Resellers to query
    products, check balance, and place automated orders using their Developer API Key.
    """

    def __init__(self, db: DatabaseManager, fulfillment: FulfillmentService, host: str = "0.0.0.0", port: int = 8080):
        self.db = db
        self.fulfillment = fulfillment
        self.host = host
        self.port = port
        self._runner: Any = None

    def _auth_user(self, request: Any) -> Optional[sqlite3.Row]:
        api_key = request.headers.get("X-API-Key", "").strip()
        if not api_key:
            auth = request.headers.get("Authorization", "")
            if auth.lower().startswith("bearer "):
                api_key = auth[7:].strip()
        if not api_key:
            return None
        return self.db.get_user_by_api_key(api_key)

    async def handle_balance(self, request: Any) -> Any:
        user = self._auth_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "Invalid or missing API key"}, status=401)
        return web.json_response({
            "ok": True,
            "user_id": user["user_id"],
            "balance": round(float(user["balance"]), 2),
            "currency": "USDT",
        })

    async def handle_products(self, request: Any) -> Any:
        user = self._auth_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "Invalid or missing API key"}, status=401)
        global_margin = self.db.get_setting_float("default_margin_pct", 35.0)
        products = []
        for p in self.db.list_products(include_inactive=False):
            price = calculate_sell_price(
                float(p["cost_price"]), global_margin, p["margin_override_pct"], p["fixed_price"]
            )
            products.append({
                "id": p["id"],
                "brand": p["brand"],
                "name": p["name"],
                "price": price,
                "stock": p["stock"],
                "warranty": p["warranty_text"],
            })
        return web.json_response({"ok": True, "count": len(products), "products": products})

    async def handle_order(self, request: Any) -> Any:
        user = self._auth_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "Invalid or missing API key"}, status=401)
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid JSON body"}, status=400)
        product_id = int(body.get("product_id") or 0)
        quantity = int(body.get("quantity") or 1)
        coupon = str(body.get("coupon_code") or "")
        ok, res, err = await self.fulfillment.execute_purchase(
            user_id=int(user["user_id"]),
            product_id=product_id,
            quantity=quantity,
            coupon_code=coupon,
            source="developer_api",
        )
        if not ok:
            return web.json_response({"ok": False, "error": err}, status=400)
        return web.json_response({"ok": True, "data": res})

    async def handle_orders(self, request: Any) -> Any:
        user = self._auth_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "Invalid or missing API key"}, status=401)
        rows = self.db.get_user_orders(int(user["user_id"]), limit=50)
        return web.json_response({
            "ok": True,
            "orders": [
                {
                    "order_code": r["order_code"],
                    "product_name": r["product_name"],
                    "quantity": r["quantity"],
                    "total_paid": r["total_paid"],
                    "status": r["status"],
                    "delivered_data": r["delivered_data"],
                    "created_at": r["created_at"],
                }
                for r in rows
            ],
        })

    async def handle_sms_webhook(self, request: Any) -> Any:
        """Accepts forwarded bKash/Nagad SMS payloads (gateway, tx_id, amount_bdt) for zero-touch auto-credit."""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)
        raw_text = str(body.get("text") or body.get("message") or "")
        gw = str(body.get("gateway") or "").strip().lower()
        if not gw:
            gw = "bkash_manual" if "bkash" in raw_text.lower() else ("nagad_manual" if "nagad" in raw_text.lower() else "bkash_manual")
        tx_id = str(body.get("tx_id") or "").strip()
        if not tx_id and raw_text:
            m_tx = re.search(r"(?:TrxID|TxnId|TxID)[:\s]+([A-Za-z0-9]{8,14})", raw_text, re.I)
            if m_tx:
                tx_id = m_tx.group(1)
        amt_bdt = float(body.get("amount_bdt") or 0)
        if amt_bdt <= 0 and raw_text:
            m_amt = re.search(r"(?:Tk|BDT)\s*([0-9,]+(?:\.\d+)?)", raw_text, re.I)
            if m_amt:
                amt_bdt = float(m_amt.group(1).replace(",", ""))
        if not tx_id:
            return web.json_response({"ok": False, "error": "tx_id not found"}, status=400)
        self.db.record_incoming_sms(gw, tx_id, amt_bdt, str(body.get("sender") or ""), raw_text)
        return web.json_response({"ok": True, "gateway": gw, "tx_id": tx_id.upper(), "amount_bdt": amt_bdt})

    async def start(self) -> None:
        if web is None:
            return
        app = web.Application()
        app.router.add_get("/api/v1/balance", self.handle_balance)
        app.router.add_get("/api/v1/products", self.handle_products)
        app.router.add_post("/api/v1/order", self.handle_order)
        app.router.add_get("/api/v1/orders", self.handle_orders)
        app.router.add_post("/api/v1/sms-webhook", self.handle_sms_webhook)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        logger.info("Reseller / Website REST API listening on http://%s:%s", self.host, self.port)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()


# ==============================================================================
# 8. TELEGRAM BOT FSM STATES & RUNTIME CONTEXT
# ==============================================================================

if AIOGRAM_AVAILABLE:

    class UserStates(StatesGroup):
        waiting_search = State()
        waiting_custom_qty = State()
        waiting_coupon = State()
        waiting_deposit_amount = State()
        waiting_deposit_tx = State()
        waiting_ticket_subject = State()
        waiting_ticket_body = State()
        waiting_ticket_reply = State()

    class AdminStates(StatesGroup):
        waiting_global_margin = State()
        waiting_prod_margin = State()
        waiting_prod_fixed = State()
        waiting_custom_prod_data = State()
        waiting_custom_stock_lines = State()
        waiting_user_search = State()
        waiting_user_balance_delta = State()
        waiting_coupon_create = State()
        waiting_broadcast_content = State()
        waiting_dm_target = State()
        waiting_dm_content = State()
        waiting_supplier_key_edit = State()
        waiting_gateway_config = State()
        waiting_gateway_meta = State()
        waiting_setting_value = State()
        waiting_staff_add = State()
        waiting_forcesub_add = State()
        waiting_supplier_add = State()
        waiting_ticket_admin_reply = State()
        waiting_order_search = State()
        waiting_catalog_search = State()


class BotAppContext:
    """Shared dependency container for handlers and background workers."""

    def __init__(self, cfg: Dict[str, Any], db: DatabaseManager):
        self.cfg = cfg
        self.db = db
        cfg_oid = int(cfg.get("OWNER_ID") or 0)
        db_oid = int(db.get_setting("owner_id", "0") or 0)
        self.owner_id = cfg_oid or db_oid
        self.supplier_client = SupplierClient()
        self.payment_engine = PaymentEngine(db)
        self.fulfillment = FulfillmentService(db, self.supplier_client)
        self.bot_username: str = ""

    def maybe_autobind_owner(self, user_id: int, username: str) -> bool:
        """Auto-bind numeric owner_id ONLY if owner_id is not set yet."""
        if self.owner_id and int(self.owner_id) > 0:
            return int(user_id) == int(self.owner_id)
        expected = self.db.get_setting("owner_username", "@Arponsuport").strip().lstrip("@").lower()
        actual = (username or "").strip().lstrip("@").lower()
        if expected and actual and actual == expected:
            self.owner_id = int(user_id)
            self.db.set_setting("owner_id", str(user_id))
            logger.info("Auto-bound Owner ID %s for @%s", user_id, actual)
            return True
        return False

    def currency(self) -> str:
        return self.db.get_setting("currency_symbol", "$")

    def store_name(self) -> str:
        return self.db.get_setting("store_name", "ARPONTOLS")


# ==============================================================================
# 9. UI HELPERS & KEYBOARD BUILDERS
# ==============================================================================

ITEMS_PER_PAGE = 12


async def safe_cb_answer(cb: CallbackQuery, text: str = "", show_alert: bool = False) -> None:
    """Safely answer a callback query without crashing if Telegram expired the query ID."""
    try:
        await cb.answer(text[:195] if text else None, show_alert=show_alert)
    except Exception:
        pass


async def notify_owner(
    bot: Bot,
    ctx: BotAppContext,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    photo_id: str = "",
) -> None:
    """Send a guaranteed private notification ONLY to the Owner."""
    owner_id = (
        int(ctx.owner_id or 0)
        or int(ctx.cfg.get("OWNER_ID") or 0)
        or int(ctx.db.get_setting("owner_id", "0") or 0)
    )
    if not owner_id:
        logger.warning("Owner ID not set; cannot deliver owner notification.")
        return
    try:
        if photo_id:
            await bot.send_photo(
                owner_id, photo_id, caption=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML
            )
        else:
            await bot.send_message(
                owner_id, text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True
            )
    except Exception as exc:
        logger.warning("Failed to notify owner (%s): %s", owner_id, exc)


async def safe_edit_or_send(
    target: Union[CallbackQuery, Message],
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    """Safely edit a callback message or send a fresh reply."""
    if isinstance(target, CallbackQuery):
        try:
            await target.message.edit_text(
                text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True
            )
            return
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return
            await target.message.answer(
                text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True
            )
    else:
        await target.answer(
            text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )


async def check_force_sub(bot: Bot, ctx: BotAppContext, user_id: int) -> Tuple[bool, Optional[InlineKeyboardMarkup]]:
    """Verify user membership in all active Force-Sub channels."""
    if ctx.db.is_admin_or_staff(user_id, ctx.owner_id):
        return True, None
    if not ctx.db.get_setting_bool("forcesub_enabled", False):
        return True, None
    channels = ctx.db.list_force_sub(only_active=True)
    if not channels:
        return True, None
    missing = []
    for ch in channels:
        try:
            member = await bot.get_chat_member(ch["chat_id"], user_id)
            if member.status in ("left", "kicked") or (member.status == "restricted" and not getattr(member, "is_member", True)):
                missing.append(ch)
        except Exception:
            # If bot cannot inspect channel, skip rather than hard-locking store
            continue
    if not missing:
        return True, None
    rows = [
        [InlineKeyboardButton(text=f"📢 Join {ch['title']}", url=ch["invite_link"])]
        for ch in missing
    ]
    rows.append([InlineKeyboardButton(text="✅ I've Joined — Verify", callback_data="user:home")])
    return False, InlineKeyboardMarkup(inline_keyboard=rows)


async def guard_customer_access(bot: Bot, ctx: BotAppContext, user_id: int, target: Union[CallbackQuery, Message]) -> bool:
    """Enforce ban, maintenance mode, and force-sub checks."""
    u = ctx.db.get_user(user_id)
    if u and u["is_banned"]:
        msg = f"🚫 Your account is suspended.\nReason: {esc(u['ban_reason'] or 'Policy violation')}"
        await safe_edit_or_send(target, msg)
        return False

    if ctx.db.get_setting_bool("maintenance_mode", False) and not ctx.db.is_admin_or_staff(user_id, ctx.owner_id):
        m_msg = ctx.db.get_setting("maintenance_msg", "⚡ Store is temporarily paused for maintenance.")
        await safe_edit_or_send(target, f"🛠️ <b>Maintenance Mode</b>\n\n{esc(m_msg)}")
        return False

    ok_sub, sub_kb = await check_force_sub(bot, ctx, user_id)
    if not ok_sub:
        await safe_edit_or_send(
            target,
            "📣 <b>Channel Subscription Required</b>\n\nPlease join our official channel(s) below to access the store:",
            reply_markup=sub_kb,
        )
        return False
    return True


def build_main_menu(ctx: BotAppContext, user: sqlite3.Row) -> Tuple[str, InlineKeyboardMarkup]:
    store = esc(ctx.store_name()).upper()
    owner_u = esc(ctx.db.get_setting("owner_username", "@Arponsuport"))
    channel_url = esc(ctx.db.get_setting("channel_url", "https://t.me/ARPONTOLS"))
    sym = ctx.currency()
    display_name = esc(user["full_name"] or user["username"] or f"User {user['user_id']}")
    order_cnt = len(ctx.db.get_user_orders(int(user["user_id"]), limit=500))
    stats = ctx.db.get_dashboard_stats()

    text = (
        f"⚡ <b>{store} — PREMIUM DIGITAL STORE</b> ⚡\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<blockquote>"
        f"👋 <b>Welcome, {display_name}</b>\n"
        f"🆔 <b>Account ID:</b> <code>{user['user_id']}</code>\n"
        f"💰 <b>Wallet Balance:</b> <code>{fmt_money(user['balance'], sym)} USDT</code>\n"
        f"📦 <b>Your Orders:</b> <code>{order_cnt}</code>  •  🎁 <b>Ref Bonus:</b> <code>{fmt_money(user['referral_balance'], sym)}</code>"
        f"</blockquote>\n"
        f"🟢 <b>Status:</b> Instant 24/7 Auto-Delivery  •  🏪 <b>{stats['product_count']} Products</b>\n"
        f"👑 <b>Owner:</b> {owner_u}\n"
        f"📢 <b>Channel:</b> {channel_url}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    rows = [
        [InlineKeyboardButton(text="🛍️ Browse Shop & Instant Buy", callback_data="shop:page:1")],
        [
            InlineKeyboardButton(text="💳 Top Up Balance", callback_data="dep:menu"),
            InlineKeyboardButton(text="📦 My Orders", callback_data="ord:list:1"),
        ],
        [
            InlineKeyboardButton(text="👤 My Profile", callback_data="user:profile"),
            InlineKeyboardButton(text="🔍 Search Catalog", callback_data="shop:search"),
        ],
        [
            InlineKeyboardButton(text="🔌 Developer API", callback_data="user:devapi"),
            InlineKeyboardButton(text="🎁 Invite & Earn", callback_data="user:referral"),
        ],
        [
            InlineKeyboardButton(text="🎫 Support Tickets", callback_data="tkt:menu"),
            InlineKeyboardButton(text="❓ Help & Guide", callback_data="user:help"),
        ],
        [InlineKeyboardButton(text="💬 Direct Support", callback_data="user:support")],
    ]
    if ctx.db.is_admin_or_staff(int(user["user_id"]), ctx.owner_id):
        rows.append([InlineKeyboardButton(text="🛡️ Open Admin Control Panel", callback_data="adm:home")])

    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_shop_catalog_view(ctx: BotAppContext, page: int = 1) -> Tuple[str, InlineKeyboardMarkup]:
    entries, total_items = ctx.db.get_catalog_entries(include_inactive=False)
    global_margin = ctx.db.get_setting_float("default_margin_pct", 35.0)
    sym = ctx.currency()

    total_pages = max(1, (len(entries) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
    page = max(1, min(page, total_pages))
    chunk = entries[(page - 1) * ITEMS_PER_PAGE : page * ITEMS_PER_PAGE]

    text = (
        f"🏪 <b>Products &amp; Brands — Page {page}/{total_pages} ({total_items} items)</b>\n\n"
        f"Browse our catalog below.\n"
        f"Tap any brand to view available plans, or an item for instant purchase:"
    )

    rows: List[List[InlineKeyboardButton]] = []
    for entry in chunk:
        if entry["type"] == "brand":
            b = entry["brand"]
            ico = brand_icon(b)
            label = f"{ico} {b} ▸ {entry['plan_count']} Plans ({entry['total_stock']}) »"
            b_enc = urllib.parse.quote(b, safe="")[:48]
            rows.append([InlineKeyboardButton(text=label, callback_data=f"shop:brand:{b_enc}:1")])
        else:
            p = entry["product"]
            price = calculate_sell_price(
                float(p["cost_price"]), global_margin, p["margin_override_pct"], p["fixed_price"]
            )
            dot = "🟢" if int(p["stock"]) > 0 else "🔴"
            short_name = p["name"][:26] + ("..." if len(p["name"]) > 26 else "")
            label = f"{short_name} | {fmt_money(price, sym)} | {dot} {p['stock']}"
            rows.append([InlineKeyboardButton(text=label, callback_data=f"shop:prod:{p['id']}:1:")])

    if not chunk:
        text += "\n\n<i>No products in catalog yet. Admin can sync an upstream supplier or add custom products in /admin.</i>"

    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"shop:page:{page - 1}"))
    nav_row.append(InlineKeyboardButton(text=f"📄 {page} / {total_pages}", callback_data="noop"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"shop:page:{page + 1}"))
    rows.append(nav_row)

    rows.append([
        InlineKeyboardButton(text="🔍 Search", callback_data="shop:search"),
        InlineKeyboardButton(text="🔄 Refresh", callback_data=f"shop:page:{page}"),
        InlineKeyboardButton(text="« Main Menu", callback_data="user:home"),
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_brand_plans_view(ctx: BotAppContext, brand: str, page: int = 1) -> Tuple[str, InlineKeyboardMarkup]:
    items = ctx.db.get_products_by_brand(brand, include_inactive=False)
    global_margin = ctx.db.get_setting_float("default_margin_pct", 35.0)
    sym = ctx.currency()

    per_page = 10
    total_pages = max(1, (len(items) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    chunk = items[(page - 1) * per_page : page * per_page]

    text = (
        f"{brand_icon(brand)} <b>{esc(brand)} — Available Plans ({len(items)})</b>\n"
        f"Page {page}/{total_pages}\n\n"
        f"Select a plan below for instant delivery:"
    )

    rows: List[List[InlineKeyboardButton]] = []
    for p in chunk:
        price = calculate_sell_price(
            float(p["cost_price"]), global_margin, p["margin_override_pct"], p["fixed_price"]
        )
        dot = "🟢" if int(p["stock"]) > 0 else "🔴"
        short_name = p["name"][:30] + ("..." if len(p["name"]) > 30 else "")
        rows.append([
            InlineKeyboardButton(
                text=f"{short_name} | {fmt_money(price, sym)} | {dot} {p['stock']}",
                callback_data=f"shop:prod:{p['id']}:1:",
            )
        ])

    b_enc = urllib.parse.quote(brand, safe="")[:48]
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"shop:brand:{b_enc}:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"shop:brand:{b_enc}:{page + 1}"))
    rows.append(nav)
    rows.append([
        InlineKeyboardButton(text="« Back to Catalog", callback_data="shop:page:1"),
        InlineKeyboardButton(text="🏠 Main Menu", callback_data="user:home"),
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_product_detail_view(
    ctx: BotAppContext, user_id: int, product_id: int, qty: int = 1, coupon_code: str = ""
) -> Tuple[str, InlineKeyboardMarkup]:
    p = ctx.db.get_product(product_id)
    if not p:
        return "❌ Product not found.", InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="« Back to Catalog", callback_data="shop:page:1")]]
        )

    sym = ctx.currency()
    global_margin = ctx.db.get_setting_float("default_margin_pct", 35.0)
    unit_price = calculate_sell_price(
        float(p["cost_price"]), global_margin, p["margin_override_pct"], p["fixed_price"]
    )
    stock = int(p["stock"])
    qty = max(1, min(qty, max(1, stock)))
    subtotal = round(unit_price * qty, 2)
    discount = 0.0
    coupon_line = ""
    valid_coupon = ""
    if coupon_code:
        ok_c, disc, msg_c = ctx.db.validate_coupon(coupon_code, user_id, subtotal)
        if ok_c:
            discount = disc
            valid_coupon = coupon_code.strip().upper()
            coupon_line = f"🏷️ <b>Promo Applied:</b> <code>{esc(valid_coupon)}</code> (-{fmt_money(discount, sym)})\n"
        else:
            coupon_line = f"⚠️ <i>{esc(msg_c)}</i>\n"

    total = round(max(0.01, subtotal - discount), 2)
    user = ctx.db.get_user(user_id)
    bal = float(user["balance"]) if user else 0.0
    dot = "🟢" if stock > 0 else "🔴"

    desc = esc(p["description"]) if p["description"] else "Instant automated digital delivery."
    text = (
        f"✨ <b>{esc(p['name'])}</b>\n"
        f"🏷️ <b>Brand:</b> {esc(p['brand'])}\n"
        f"🛡️ <b>Warranty:</b> {esc(p['warranty_text'] or 'Standard')}\n"
        f"📦 <b>Current Stock:</b> {dot} <b>{stock} units</b>\n\n"
        f"📝 <b>Details:</b>\n{desc}\n\n"
        f"💵 <b>Unit Price:</b> {fmt_money(unit_price, sym)} USDT\n"
        f"🔢 <b>Quantity:</b> {qty}\n"
        f"{coupon_line}"
        f"💳 <b>Total Payable:</b> <b>{fmt_money(total, sym)} USDT</b>\n"
        f"💰 <b>Your Balance:</b> {fmt_money(bal, sym)} USDT"
    )

    rows: List[List[InlineKeyboardButton]] = []
    if stock > 0:
        qty_btns = []
        for q_opt in (1, 2, 5, 10):
            if q_opt <= stock:
                mark = "• " if q_opt == qty else ""
                qty_btns.append(
                    InlineKeyboardButton(
                        text=f"{mark}{q_opt}x",
                        callback_data=f"shop:prod:{product_id}:{q_opt}:{valid_coupon}",
                    )
                )
        qty_btns.append(
            InlineKeyboardButton(text="✏️ Custom", callback_data=f"shop:cqty:{product_id}:{valid_coupon}")
        )
        rows.append(qty_btns)
        rows.append([
            InlineKeyboardButton(
                text=f"⚡ Buy Now ({fmt_money(total, sym)})",
                callback_data=f"shop:buy:{product_id}:{qty}:{valid_coupon}",
            )
        ])
        rows.append([
            InlineKeyboardButton(text="🏷️ Apply Promo Code", callback_data=f"shop:coupon:{product_id}:{qty}"),
            InlineKeyboardButton(text="💳 Top Up Balance", callback_data="dep:menu"),
        ])
    else:
        rows.append([
            InlineKeyboardButton(
                text="👁️ Notify Me When Restocked",
                callback_data=f"shop:watch:{product_id}",
            )
        ])

    rows.append([
        InlineKeyboardButton(text="« Back to Catalog", callback_data="shop:page:1"),
        InlineKeyboardButton(text="🏠 Main Menu", callback_data="user:home"),
    ])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def build_admin_dashboard(ctx: BotAppContext) -> Tuple[str, InlineKeyboardMarkup]:
    stats = ctx.db.get_dashboard_stats()
    sym = ctx.currency()
    primary_sup = ctx.db.get_primary_supplier()
    sup_name = esc(primary_sup["name"]) if primary_sup else "Not Configured"
    sup_bal = float(primary_sup["cached_balance"]) if primary_sup else 0.0
    margin_pct = ctx.db.get_setting_float("default_margin_pct", 35.0)
    paused = ctx.db.get_setting_bool("maintenance_mode", False)
    status_badge = "⏸️ PAUSED" if paused else "🟢 LIVE"

    dep_badge = f" ({stats['pending_deposits']})" if stats["pending_deposits"] > 0 else ""
    tkt_badge = f" ({stats['open_tickets']})" if stats["open_tickets"] > 0 else ""
    text = (
        f"🛡️ <b>{esc(ctx.store_name()).upper()} — MASTER ADMIN PANEL</b> [{status_badge}]\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<blockquote>"
        f"📊 <b>Live Financial Overview</b>\n"
        f"• Today: <b>{stats['today_orders']} orders</b> | Rev <b>{fmt_money(stats['today_revenue'], sym)}</b> | Profit <b>+{fmt_money(stats['today_profit'], sym)}</b>\n"
        f"• All-Time Rev: <b>{fmt_money(stats['total_revenue'], sym)}</b> | Net Profit: <b>+{fmt_money(stats['total_profit'], sym)}</b>\n"
        f"• Customers: <b>{stats['total_users']}</b> | Deposits: <b>{fmt_money(stats['total_deposits'], sym)}</b> (⏳ {stats['pending_deposits']})\n"
        f"• Active Items: <b>{stats['product_count']}</b> | Open Tickets: <b>{stats['open_tickets']}</b>"
        f"</blockquote>\n"
        f"🔌 <b>Wholesale Core ({sup_name}):</b>\n"
        f"• Reseller Credit: <b>{fmt_money(sup_bal, sym)} USDT</b>  •  Markup: <b>+{margin_pct:.1f}%</b>\n"
        f"👥 <b>Authorized Staff:</b> <b>{stats['staff_count']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Select a management module below:</i>"
    )

    pause_label = "▶️ Resume Store" if paused else "⏸️ Pause Store"
    fsub_cnt = len(ctx.db.list_force_sub(only_active=True))
    rows = [
        [
            InlineKeyboardButton(text="📦 Catalog & Margins", callback_data="adm:catalog"),
            InlineKeyboardButton(text="🏪 Custom Products", callback_data="adm:custom"),
        ],
        [
            InlineKeyboardButton(text="📋 Orders History", callback_data="adm:orders:1"),
            InlineKeyboardButton(text=f"📑 Deposits Log{dep_badge}", callback_data="adm:deposits:pending"),
        ],
        [
            InlineKeyboardButton(text="👥 Users Management", callback_data="adm:users"),
            InlineKeyboardButton(text=f"🎫 Tickets{tkt_badge}", callback_data="adm:tickets"),
        ],
        [
            InlineKeyboardButton(text="🏷️ Promo Codes", callback_data="adm:coupons"),
            InlineKeyboardButton(text="📢 Broadcast", callback_data="adm:broadcast"),
        ],
        [
            InlineKeyboardButton(text="💳 Payment Gateways", callback_data="adm:payments"),
            InlineKeyboardButton(text="⚙️ Store Settings", callback_data="adm:settings"),
        ],
        [
            InlineKeyboardButton(text="📊 Analytics & Margin", callback_data="adm:analytics"),
            InlineKeyboardButton(text="🛡️ Team & Staff Roles", callback_data="adm:staff"),
        ],
        [
            InlineKeyboardButton(text=f"📣 Force-Sub ({fsub_cnt})", callback_data="adm:forcesub"),
            InlineKeyboardButton(text="👁️ Stock Watcher", callback_data="adm:watcher"),
        ],
        [
            InlineKeyboardButton(text="🔌 External Suppliers", callback_data="adm:suppliers"),
            InlineKeyboardButton(text="💬 Support Settings", callback_data="adm:support"),
        ],
        [
            InlineKeyboardButton(text=f"📈 Profit (+{margin_pct:.0f}%)", callback_data="adm:set_global_margin"),
            InlineKeyboardButton(text="👥 Referrals", callback_data="adm:referrals"),
        ],
        [
            InlineKeyboardButton(text=pause_label, callback_data="adm:maintenance"),
            InlineKeyboardButton(text="⚡ Refill Credit", callback_data="adm:refill_credit"),
        ],
        [
            InlineKeyboardButton(text="🔄 Refresh Dashboard", callback_data="adm:home"),
            InlineKeyboardButton(text="« Customer Store", callback_data="user:home"),
        ],
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


# ==============================================================================
# 10. CUSTOMER STOREFRONT ROUTER & HANDLERS
# ==============================================================================

def build_customer_router(ctx: BotAppContext) -> Router:
    router = Router(name="customer_router")

    @router.callback_query(F.data == "noop")
    async def cb_noop(cb: CallbackQuery):
        await cb.answer()

    # --- /start (supports deep links: ref_<id> and buy_<product_id>) ---
    @router.message(CommandStart())
    async def cmd_start(message: Message, command: CommandObject, state: FSMContext, bot: Bot):
        await state.clear()
        args = (command.args or "").strip()
        ref_id: Optional[int] = None
        buy_pid: Optional[int] = None
        if args.startswith("ref_") and args[4:].isdigit():
            ref_id = int(args[4:])
        elif args.startswith("buy_") and args[4:].isdigit():
            buy_pid = int(args[4:])

        ctx.maybe_autobind_owner(message.from_user.id, message.from_user.username or "")
        user, is_new_user = ctx.db.upsert_user(
            message.from_user.id,
            message.from_user.username or "",
            message.from_user.full_name or "",
            referred_by=ref_id,
        )
        if is_new_user and ref_id and ref_id != message.from_user.id and user["referred_by"]:
            signup_bonus = ctx.db.get_setting_float("referral_signup_bonus_usd", 0.05)
            pct = ctx.db.get_setting_float("referral_commission_pct", 5.0)
            sym = ctx.currency()
            new_name = esc(message.from_user.full_name or message.from_user.username or str(message.from_user.id))
            bonus_line = (
                f"💰 <b>Instant Bonus Added:</b> <code>+{fmt_money(signup_bonus, sym)}</code> credited directly to your balance!\n"
                if signup_bonus > 0 else ""
            )
            try:
                await bot.send_message(
                    int(user["referred_by"]),
                    f"🎉 <b>New Referral Joined!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"👤 <b>User:</b> {new_name}\n"
                    f"{bonus_line}"
                    f"📈 <b>Ongoing Commission:</b> You will also earn <b>{pct:.1f}%</b> on their deposits &amp; orders!",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        if not await guard_customer_access(bot, ctx, message.from_user.id, message):
            return

        if buy_pid:
            text, kb = build_product_detail_view(ctx, message.from_user.id, buy_pid)
            await safe_edit_or_send(message, text, kb)
            return

        text, kb = build_main_menu(ctx, user)
        await safe_edit_or_send(message, text, kb)

    @router.callback_query(F.data == "user:home")
    async def cb_home(cb: CallbackQuery, state: FSMContext, bot: Bot):
        await state.clear()
        user, _ = ctx.db.upsert_user(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "")
        if not await guard_customer_access(bot, ctx, cb.from_user.id, cb):
            await cb.answer()
            return
        text, kb = build_main_menu(ctx, user)
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    # --- /shop & Catalog Navigation ---
    @router.message(Command("shop"))
    async def cmd_shop(message: Message, state: FSMContext, bot: Bot):
        await state.clear()
        ctx.db.upsert_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name or "")
        if not await guard_customer_access(bot, ctx, message.from_user.id, message):
            return
        text, kb = build_shop_catalog_view(ctx, 1)
        await safe_edit_or_send(message, text, kb)

    @router.callback_query(F.data.startswith("shop:page:"))
    async def cb_shop_page(cb: CallbackQuery, state: FSMContext, bot: Bot):
        await state.clear()
        if not await guard_customer_access(bot, ctx, cb.from_user.id, cb):
            await cb.answer()
            return
        page = int(cb.data.split(":")[2])
        text, kb = build_shop_catalog_view(ctx, page)
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    @router.callback_query(F.data.startswith("shop:brand:"))
    async def cb_shop_brand(cb: CallbackQuery, bot: Bot):
        if not await guard_customer_access(bot, ctx, cb.from_user.id, cb):
            await cb.answer()
            return
        parts = cb.data.split(":")
        brand = urllib.parse.unquote(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 1
        text, kb = build_brand_plans_view(ctx, brand, page)
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    @router.callback_query(F.data.startswith("shop:prod:"))
    async def cb_shop_prod(cb: CallbackQuery, bot: Bot):
        if not await guard_customer_access(bot, ctx, cb.from_user.id, cb):
            await cb.answer()
            return
        parts = cb.data.split(":")
        pid = int(parts[2])
        qty = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 1
        coupon = parts[4] if len(parts) > 4 else ""
        text, kb = build_product_detail_view(ctx, cb.from_user.id, pid, qty, coupon)
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    @router.callback_query(F.data.startswith("shop:cqty:"))
    async def cb_custom_qty(cb: CallbackQuery, state: FSMContext):
        parts = cb.data.split(":")
        pid = int(parts[2])
        coupon = parts[3] if len(parts) > 3 else ""
        await state.set_state(UserStates.waiting_custom_qty)
        await state.update_data(product_id=pid, coupon=coupon)
        await safe_edit_or_send(
            cb,
            "🔢 <b>Enter Quantity</b>\n\nSend the number of units you want to purchase (e.g. <code>3</code>):",
            InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data=f"shop:prod:{pid}:1:{coupon}")]]
            ),
        )
        await cb.answer()

    @router.message(UserStates.waiting_custom_qty)
    async def msg_custom_qty(message: Message, state: FSMContext):
        data = await state.get_data()
        pid = int(data["product_id"])
        coupon = data.get("coupon", "")
        raw = (message.text or "").strip()
        if not raw.isdigit() or int(raw) < 1:
            await message.answer("⚠️ Please enter a valid positive number (e.g. 2).")
            return
        await state.clear()
        text, kb = build_product_detail_view(ctx, message.from_user.id, pid, int(raw), coupon)
        await safe_edit_or_send(message, text, kb)

    @router.callback_query(F.data.startswith("shop:coupon:"))
    async def cb_apply_coupon(cb: CallbackQuery, state: FSMContext):
        parts = cb.data.split(":")
        pid, qty = int(parts[2]), int(parts[3])
        await state.set_state(UserStates.waiting_coupon)
        await state.update_data(product_id=pid, qty=qty)
        await safe_edit_or_send(
            cb,
            "🏷️ <b>Apply Promo Code</b>\n\nType your promo/coupon code below:",
            InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data=f"shop:prod:{pid}:{qty}:")]]
            ),
        )
        await cb.answer()

    @router.message(UserStates.waiting_coupon)
    async def msg_apply_coupon(message: Message, state: FSMContext):
        data = await state.get_data()
        pid, qty = int(data["product_id"]), int(data["qty"])
        code = (message.text or "").strip().upper()
        await state.clear()
        text, kb = build_product_detail_view(ctx, message.from_user.id, pid, qty, code)
        await safe_edit_or_send(message, text, kb)

    @router.callback_query(F.data.startswith("shop:watch:"))
    async def cb_watch_stock(cb: CallbackQuery):
        pid = int(cb.data.split(":")[2])
        ctx.db.add_stock_watcher(cb.from_user.id, pid)
        await cb.answer("👁️ Subscribed! We'll notify you as soon as this item is back in stock.", show_alert=True)

    @router.callback_query(F.data.startswith("shop:buy:"))
    async def cb_buy_now(cb: CallbackQuery, bot: Bot):
        if not await guard_customer_access(bot, ctx, cb.from_user.id, cb):
            await cb.answer()
            return
        parts = cb.data.split(":")
        pid = int(parts[2])
        qty = int(parts[3])
        coupon = parts[4] if len(parts) > 4 else ""

        await cb.answer("⚡ Processing your instant order...")
        ok, res, err = await ctx.fulfillment.execute_purchase(
            user_id=cb.from_user.id, product_id=pid, quantity=qty, coupon_code=coupon, source="telegram"
        )
        if not ok:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Top Up Balance", callback_data="dep:menu")],
                [InlineKeyboardButton(text="« Back to Product", callback_data=f"shop:prod:{pid}:{qty}:{coupon}")],
            ])
            await safe_edit_or_send(cb, f"❌ <b>Purchase Could Not Be Completed</b>\n\n{esc(err)}", kb)
            # Also notify Owner about the failed/rejected order attempt
            p_fail = ctx.db.get_product(pid)
            p_name = p_fail["name"] if p_fail else f"Product #{pid}"
            await notify_owner(
                bot,
                ctx,
                f"❌ <b>Order Failed / Rejected!</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"• <b>User:</b> <code>{cb.from_user.id}</code> (@{esc(cb.from_user.username or 'N/A')})\n"
                f"• <b>Product:</b> {esc(p_name)} × {qty}\n"
                f"• <b>Reason:</b> {esc(err)}",
            )
            return

        sym = ctx.currency()
        delivered_preview = esc(res["delivered_data"])
        if len(delivered_preview) > 3200:
            delivered_preview = delivered_preview[:3200] + "\n... (see full receipt in My Orders)"

        receipt = (
            f"✅ <b>Order Completed Successfully!</b>\n\n"
            f"🧾 <b>Order Code:</b> <code>{esc(res['order_code'])}</code>\n"
            f"📦 <b>Product:</b> {esc(res['product_name'])}\n"
            f"🔢 <b>Quantity:</b> {res['quantity']}\n"
            f"💵 <b>Total Paid:</b> {fmt_money(res['total_paid'], sym)} USDT\n"
            f"💰 <b>Remaining Balance:</b> {fmt_money(res['new_balance'], sym)} USDT\n\n"
            f"🔐 <b>Your Delivered Product / Credentials:</b>\n"
            f"<pre>{delivered_preview}</pre>\n\n"
            f"<i>Saved permanently under 📦 My Orders.</i>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 My Orders", callback_data="ord:list:1")],
            [InlineKeyboardButton(text="🛍️ Continue Shopping", callback_data="shop:page:1")],
            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="user:home")],
        ])
        await safe_edit_or_send(cb, receipt, kb)

        # Auto-post live order card to public channel (@ARPONTOLS or configured channel)
        await notify_order_channel(bot, ctx, res, pid, cb.from_user.username or cb.from_user.full_name or "Customer")

        # Notify Owner of the confirmed sale (always via notify_owner)
        await notify_owner(
            bot,
            ctx,
            f"✅ <b>Order Confirmed (New Sale)!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Order:</b> <code>{esc(res['order_code'])}</code>\n"
            f"• <b>Buyer:</b> <code>{cb.from_user.id}</code> (@{esc(cb.from_user.username or 'N/A')})\n"
            f"• <b>Item:</b> {esc(res['product_name'])} × {res['quantity']}\n"
            f"• <b>Paid:</b> <b>{fmt_money(res['total_paid'], sym)} USDT</b>",
        )

    # --- /search ---
    @router.message(Command("search"))
    async def cmd_search(message: Message, state: FSMContext, bot: Bot):
        ctx.db.upsert_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name or "")
        if not await guard_customer_access(bot, ctx, message.from_user.id, message):
            return
        await state.set_state(UserStates.waiting_search)
        await safe_edit_or_send(
            message,
            "🔍 <b>Search Products &amp; Brands</b>\n\n"
            "Type the name of the product, brand, or service you are looking for:\n"
            "<i>(Examples: ChatGPT, VPN, Canva, Spotify)</i>\n\n"
            "<i>Or tap Back below to return to the catalog.</i>",
            InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="« Back to Catalog", callback_data="shop:page:1")]]
            ),
        )

    @router.callback_query(F.data == "shop:search")
    async def cb_search(cb: CallbackQuery, state: FSMContext):
        await state.set_state(UserStates.waiting_search)
        await safe_edit_or_send(
            cb,
            "🔍 <b>Search Products &amp; Brands</b>\n\n"
            "Type the name of the product, brand, or service you are looking for:\n"
            "<i>(Examples: ChatGPT, VPN, Canva, Spotify)</i>\n\n"
            "<i>Or tap Back below to return to the catalog.</i>",
            InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="« Back to Catalog", callback_data="shop:page:1")]]
            ),
        )
        await cb.answer()

    @router.message(UserStates.waiting_search)
    async def msg_search_query(message: Message, state: FSMContext):
        q = (message.text or "").strip()
        if not q:
            return
        await state.clear()
        results = ctx.db.search_products(q, limit=15)
        global_margin = ctx.db.get_setting_float("default_margin_pct", 35.0)
        sym = ctx.currency()
        if not results:
            await safe_edit_or_send(
                message,
                f"🔍 No products matched <b>{esc(q)}</b>.\nTry another keyword or browse our full catalog.",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔍 Search Again", callback_data="shop:search")],
                    [InlineKeyboardButton(text="🛍️ Browse Catalog", callback_data="shop:page:1")],
                ]),
            )
            return

        rows = []
        for p in results:
            price = calculate_sell_price(
                float(p["cost_price"]), global_margin, p["margin_override_pct"], p["fixed_price"]
            )
            dot = "🟢" if int(p["stock"]) > 0 else "🔴"
            rows.append([
                InlineKeyboardButton(
                    text=f"{p['name'][:30]} | {fmt_money(price, sym)} | {dot} {p['stock']}",
                    callback_data=f"shop:prod:{p['id']}:1:",
                )
            ])
        rows.append([
            InlineKeyboardButton(text="🔍 Search Again", callback_data="shop:search"),
            InlineKeyboardButton(text="« Back to Catalog", callback_data="shop:page:1"),
        ])
        await safe_edit_or_send(
            message,
            f"🔍 <b>Search Results for \"{esc(q)}\" ({len(results)} found):</b>",
            InlineKeyboardMarkup(inline_keyboard=rows),
        )

    # --- /orders ---
    async def render_user_orders(target: Union[CallbackQuery, Message], user_id: int, page: int = 1):
        orders = ctx.db.get_user_orders(user_id, limit=50)
        sym = ctx.currency()
        if not orders:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛍️ Browse Products", callback_data="shop:page:1")],
                [InlineKeyboardButton(text="« Main Menu", callback_data="user:home")],
            ])
            await safe_edit_or_send(target, "📦 You haven't made any purchases yet.", kb)
            return

        per_page = 8
        total_pages = max(1, (len(orders) + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        chunk = orders[(page - 1) * per_page : page * per_page]

        rows = []
        for o in chunk:
            st_ico = "✅" if o["status"] == "completed" else ("↩️" if o["status"] == "refunded" else "❌")
            rows.append([
                InlineKeyboardButton(
                    text=f"{st_ico} {o['order_code']} | {o['product_name'][:20]} ({fmt_money(o['total_paid'], sym)})",
                    callback_data=f"ord:view:{o['id']}",
                )
            ])
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"ord:list:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="noop"))
        if page < total_pages:
            nav.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"ord:list:{page + 1}"))
        rows.append(nav)
        rows.append([InlineKeyboardButton(text="« Main Menu", callback_data="user:home")])
        await safe_edit_or_send(
            target,
            f"📦 <b>My Orders &amp; Receipts ({len(orders)} total)</b>\nTap any order to view credentials:",
            InlineKeyboardMarkup(inline_keyboard=rows),
        )

    @router.message(Command("orders"))
    async def cmd_orders(message: Message, state: FSMContext, bot: Bot):
        await state.clear()
        if not await guard_customer_access(bot, ctx, message.from_user.id, message):
            return
        await render_user_orders(message, message.from_user.id, 1)

    @router.callback_query(F.data.startswith("ord:list:"))
    async def cb_orders_list(cb: CallbackQuery):
        page = int(cb.data.split(":")[2])
        await render_user_orders(cb, cb.from_user.id, page)
        await cb.answer()

    @router.callback_query(F.data.startswith("ord:view:"))
    async def cb_order_view(cb: CallbackQuery):
        oid = int(cb.data.split(":")[2])
        o = ctx.db.get_order_by_id(oid)
        if not o or (int(o["user_id"]) != cb.from_user.id and not ctx.db.is_admin_or_staff(cb.from_user.id, ctx.owner_id)):
            await cb.answer("Order not found.", show_alert=True)
            return
        sym = ctx.currency()
        text = (
            f"🧾 <b>Order Receipt — {esc(o['order_code'])}</b>\n\n"
            f"• <b>Product:</b> {esc(o['product_name'])}\n"
            f"• <b>Quantity:</b> {o['quantity']}\n"
            f"• <b>Total Paid:</b> {fmt_money(o['total_paid'], sym)} USDT\n"
            f"• <b>Status:</b> {esc(o['status']).upper()}\n"
            f"• <b>Date (UTC):</b> {esc(o['created_at'])}\n\n"
            f"🔐 <b>Delivered Credentials:</b>\n"
            f"<pre>{esc(o['delivered_data'][:3200])}</pre>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎫 Report Issue with Order", callback_data="tkt:new")],
            [InlineKeyboardButton(text="« Back to Orders", callback_data="ord:list:1")],
        ])
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    # --- /balance & Deposit Flow ---
    async def render_deposit_menu(target: Union[CallbackQuery, Message], user_id: int):
        gateways = ctx.db.list_gateways(only_enabled=True)
        user = ctx.db.get_user(user_id)
        sym = ctx.currency()
        bal = float(user["balance"]) if user else 0.0
        text = (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💳 <b>TOP UP STORE BALANCE</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Current Balance: <b>{fmt_money(bal, sym)} USDT</b>\n\n"
            f"Select your payment method below:\n"
            f"<i>Funds are credited to your store balance instantly upon confirmation.</i>"
        )
        rows = [
            [InlineKeyboardButton(text=g["display_name"], callback_data=f"dep:gw:{g['code']}")]
            for g in gateways
        ]
        rows.append([InlineKeyboardButton(text="💼 Wallet Summary", callback_data="user:profile")])
        rows.append([InlineKeyboardButton(text="👥 Referral Program & Earnings", callback_data="user:referral")])
        rows.append([InlineKeyboardButton(text="« Main Menu", callback_data="user:home")])
        await safe_edit_or_send(target, text, InlineKeyboardMarkup(inline_keyboard=rows))

    @router.message(Command("balance"))
    async def cmd_balance(message: Message, state: FSMContext, bot: Bot):
        await state.clear()
        ctx.db.upsert_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name or "")
        if not await guard_customer_access(bot, ctx, message.from_user.id, message):
            return
        await render_deposit_menu(message, message.from_user.id)

    @router.callback_query(F.data == "dep:menu")
    async def cb_dep_menu(cb: CallbackQuery, state: FSMContext):
        await state.clear()
        await render_deposit_menu(cb, cb.from_user.id)
        await cb.answer()

    @router.callback_query(F.data.startswith("dep:gw:"))
    async def cb_dep_gateway(cb: CallbackQuery, state: FSMContext):
        gw_code = cb.data.split(":")[2]
        gw = ctx.db.get_gateway(gw_code)
        if not gw or not gw["is_enabled"]:
            await cb.answer("Gateway unavailable.", show_alert=True)
            return
        await state.set_state(UserStates.waiting_deposit_amount)
        await state.update_data(gateway_code=gw_code)
        min_amt = float(gw["min_amount"])
        rows = [
            [
                InlineKeyboardButton(text="$5", callback_data=f"dep:amt:{gw_code}:5"),
                InlineKeyboardButton(text="$10", callback_data=f"dep:amt:{gw_code}:10"),
                InlineKeyboardButton(text="$25", callback_data=f"dep:amt:{gw_code}:25"),
                InlineKeyboardButton(text="$50", callback_data=f"dep:amt:{gw_code}:50"),
            ],
            [InlineKeyboardButton(text="« Back", callback_data="dep:menu")],
        ]
        await safe_edit_or_send(
            cb,
            f"{esc(gw['display_name'])}\n\n"
            f"Choose a quick amount or type any custom USD amount (min <b>${min_amt:.2f}</b>):",
            InlineKeyboardMarkup(inline_keyboard=rows),
        )
        await cb.answer()

    async def process_deposit_invoice(
        target: Union[CallbackQuery, Message], bot: Bot, user_id: int, gw_code: str, amount: float
    ):
        ok, inv, err = await ctx.payment_engine.create_invoice(user_id, gw_code, amount)
        if not ok:
            await safe_edit_or_send(
                target,
                f"⚠️ <b>Deposit Setup Error</b>\n\n{esc(err)}",
                InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Back", callback_data="dep:menu")]]),
            )
            return

        rows: List[List[InlineKeyboardButton]] = []
        if inv.get("pay_url"):
            rows.append([InlineKeyboardButton(text="💳 Pay Now ↗", url=inv["pay_url"])])
        if inv.get("mode") in ("api", "auto_bep20"):
            rows.append([InlineKeyboardButton(text="🔄 Verify / Check Payment", callback_data=f"dep:check:{inv['deposit_id']}")])
        if inv.get("mode") in ("manual", "auto_bep20"):
            rows.append([InlineKeyboardButton(text="🧾 Submit Transaction ID / Proof", callback_data=f"dep:tx:{inv['deposit_id']}")])
        rows.append([InlineKeyboardButton(text="« Back to Deposit Menu", callback_data="dep:menu")])
        await safe_edit_or_send(target, inv["instructions"], InlineKeyboardMarkup(inline_keyboard=rows))

        # Instantly alert OWNER ONLY about the new deposit request
        u = ctx.db.get_user(user_id)
        uname = f"@{u['username']}" if (u and u["username"]) else "No username"
        rate = ctx.db.get_setting_float("bdt_per_usd", 125.0)
        bdt_line = f" (৳{round(amount * rate):,} BDT)" if gw_code in ("bkash_manual", "nagad_manual") else ""
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Approve & Credit", callback_data=f"adm:dep_ok:{inv['deposit_id']}"),
            InlineKeyboardButton(text="❌ Reject", callback_data=f"adm:dep_no:{inv['deposit_id']}"),
        ]])
        await notify_owner(
            bot,
            ctx,
            f"🔔 <b>New Deposit Request Initiated!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Code:</b> <code>{esc(inv['deposit_code'])}</code>\n"
            f"• <b>User:</b> <code>{user_id}</code> ({esc(uname)})\n"
            f"• <b>Gateway:</b> <b>{esc(gw_code)}</b>\n"
            f"• <b>Amount:</b> <b>{fmt_money(amount, ctx.currency())} USD</b>{bdt_line}\n"
            f"• <b>Status:</b> ⏳ Waiting for payment / TxID",
            reply_markup=admin_kb,
        )

    @router.callback_query(F.data.startswith("dep:amt:"))
    async def cb_dep_quick_amt(cb: CallbackQuery, state: FSMContext, bot: Bot):
        await state.clear()
        _, _, gw_code, amt_str = cb.data.split(":")
        await process_deposit_invoice(cb, bot, cb.from_user.id, gw_code, float(amt_str))
        await cb.answer()

    @router.message(UserStates.waiting_deposit_amount)
    async def msg_dep_custom_amt(message: Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        gw_code = data["gateway_code"]
        raw = (message.text or "").strip().lstrip("$")
        try:
            amt = round(float(raw), 2)
            if amt <= 0:
                raise ValueError
        except ValueError:
            await message.answer("⚠️ Please enter a valid positive number (e.g. <code>12.5</code>).", parse_mode=ParseMode.HTML)
            return
        await state.clear()
        await process_deposit_invoice(message, bot, message.from_user.id, gw_code, amt)

    @router.callback_query(F.data.startswith("dep:check:"))
    async def cb_dep_check(cb: CallbackQuery):
        dep_id = int(cb.data.split(":")[2])
        dep = ctx.db.get_deposit(dep_id)
        if not dep or int(dep["user_id"]) != cb.from_user.id:
            await cb.answer("Deposit not found.", show_alert=True)
            return
        paid, msg = await ctx.payment_engine.verify_deposit(dep)
        if paid:
            await safe_edit_or_send(
                cb,
                f"✅ <b>Deposit Confirmed!</b>\n\n"
                f"• Reference: <code>{esc(dep['deposit_code'])}</code>\n"
                f"• Credited: <b>{fmt_money(dep['amount_usd'], ctx.currency())} USDT</b>\n\n"
                f"You can now purchase products instantly in the Shop!",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🛍️ Open Shop", callback_data="shop:page:1")],
                    [InlineKeyboardButton(text="🏠 Main Menu", callback_data="user:home")],
                ]),
            )
            await cb.answer("✅ Credited!", show_alert=True)
        else:
            await cb.answer(msg[:190], show_alert=True)

    @router.callback_query(F.data.startswith("dep:tx:"))
    async def cb_dep_submit_tx(cb: CallbackQuery, state: FSMContext):
        dep_id = int(cb.data.split(":")[2])
        await state.set_state(UserStates.waiting_deposit_tx)
        await state.update_data(deposit_id=dep_id)
        await safe_edit_or_send(
            cb,
            "🧾 <b>Submit Transaction ID / Payment Proof</b>\n\n"
            "Send your Transaction ID (TrxID / Order ID / TxHash) or upload a payment screenshot with the TrxID in the caption:",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="dep:menu")]]),
        )
        await cb.answer()

    @router.message(UserStates.waiting_deposit_tx)
    async def msg_dep_tx(message: Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        dep_id = int(data["deposit_id"])
        photo_id = message.photo[-1].file_id if message.photo else ""
        tx_ref = (message.text or message.caption or "").strip()

        dep = ctx.db.get_deposit(dep_id)
        if not dep:
            await state.clear()
            await message.answer("⚠️ Deposit record not found.")
            return

        outcome, vmsg = await ctx.payment_engine.verify_submitted_tx(dep, tx_ref, has_photo=bool(photo_id))
        if outcome == "invalid":
            await message.answer(
                f"❌ <b>Verification Failed</b>\n\n{vmsg}\n\nPlease send the correct Transaction ID again:",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="dep:menu")]]),
            )
            return

        await state.clear()
        save_ref = tx_ref[:200] if tx_ref else "Screenshot attached"
        with ctx.db.connect() as conn:
            conn.execute("UPDATE deposits SET tx_reference = ? WHERE id = ?", (save_ref, dep_id))

        if outcome == "confirmed":
            u_now = ctx.db.get_user(message.from_user.id)
            new_bal = float(u_now["balance"]) if u_now else float(dep["amount_usd"])
            await message.answer(
                f"🎉 <b>Payment Successfully Verified!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>Top-Up Completed Automatically</b>\n"
                f"• Reference: <code>{esc(dep['deposit_code'])}</code>\n"
                f"• TxID: <code>{esc(save_ref)}</code>\n"
                f"• Credited: <b>+{fmt_money(dep['amount_usd'], ctx.currency())} USDT</b>\n"
                f"• New Balance: <b>{fmt_money(new_bal, ctx.currency())} USDT</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🛍️ Open Shop Now", callback_data="shop:page:1")],
                    [InlineKeyboardButton(text="🏠 Main Menu", callback_data="user:home")],
                ]),
            )
            await notify_owner(
                bot,
                ctx,
                f"💚 <b>Auto-Payment Confirmed!</b>\n"
                f"• Code: <code>{esc(dep['deposit_code'])}</code> | User: <code>{message.from_user.id}</code>\n"
                f"• Gateway: <b>{esc(dep['gateway'])}</b> | Amount: <b>{fmt_money(dep['amount_usd'], ctx.currency())} USD</b>\n"
                f"• TxID: <code>{esc(save_ref)}</code>",
            )
            return

        await message.answer(
            f"⏳ <b>Transaction ID Verified &amp; Submitted!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• Deposit Code: <code>{esc(dep['deposit_code'])}</code>\n"
            f"• Amount: <b>{fmt_money(dep['amount_usd'], ctx.currency())} USDT</b>\n"
            f"• TxID / Proof: <code>{esc(save_ref)}</code>\n\n"
            f"✅ Your balance will be credited as soon as payment confirmation completes.",
            parse_mode=ParseMode.HTML,
        )

        # Notify OWNER ONLY with 1-click Approve / Reject buttons
        admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Approve Deposit", callback_data=f"adm:dep_ok:{dep_id}"),
            InlineKeyboardButton(text="❌ Reject", callback_data=f"adm:dep_no:{dep_id}"),
        ]])
        caption = (
            f"💳 <b>Deposit Proof Submitted!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Code:</b> <code>{esc(dep['deposit_code'])}</code>\n"
            f"• <b>User:</b> <code>{message.from_user.id}</code> (@{esc(message.from_user.username or 'N/A')})\n"
            f"• <b>Gateway:</b> <b>{esc(dep['gateway'])}</b>\n"
            f"• <b>Amount:</b> <b>{fmt_money(dep['amount_usd'], ctx.currency())} USD</b>\n"
            f"• <b>TxID / Proof:</b> <code>{esc(tx_ref)}</code>"
        )
        await notify_owner(bot, ctx, caption, reply_markup=admin_kb, photo_id=photo_id)

    # --- Profile, Developer API, Referrals, Help, Support ---
    @router.callback_query(F.data == "user:profile")
    async def cb_profile(cb: CallbackQuery):
        u = ctx.db.get_user(cb.from_user.id)
        if not u:
            await cb.answer("Profile not found.", show_alert=True)
            return
        sym = ctx.currency()
        orders_cnt = len(ctx.db.get_user_orders(cb.from_user.id, limit=1000))
        text = (
            f"👤 <b>My Profile &amp; Wallet</b>\n\n"
            f"• <b>User ID:</b> <code>{u['user_id']}</code>\n"
            f"• <b>Username:</b> @{esc(u['username'] or 'None')}\n"
            f"• <b>Store Balance:</b> <b>{fmt_money(u['balance'], sym)} USDT</b>\n"
            f"• <b>Referral Earnings:</b> {fmt_money(u['referral_balance'], sym)} USDT\n"
            f"• <b>Total Deposited:</b> {fmt_money(u['total_deposited'], sym)} USDT\n"
            f"• <b>Total Spent:</b> {fmt_money(u['total_spent'], sym)} USDT\n"
            f"• <b>Orders Placed:</b> {orders_cnt}\n"
            f"• <b>Joined:</b> {esc(u['created_at'][:10])}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Top Up Balance", callback_data="dep:menu")],
            [InlineKeyboardButton(text="📦 My Orders", callback_data="ord:list:1")],
            [InlineKeyboardButton(text="« Main Menu", callback_data="user:home")],
        ])
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    @router.callback_query(F.data == "user:devapi")
    async def cb_devapi(cb: CallbackQuery):
        u = ctx.db.get_user(cb.from_user.id)
        api_key = u["api_key"] if u and u["api_key"] else "Not generated yet"
        port = ctx.cfg.get("WEB_API_PORT", 8080)
        text = (
            f"🔌 <b>Reseller Developer API</b>\n\n"
            f"Connect your own Telegram bot or website to resell our catalog automatically!\n\n"
            f"🔑 <b>Your API Key:</b>\n<code>{esc(api_key)}</code>\n\n"
            f"📡 <b>Endpoints (Header: <code>X-API-Key: YOUR_KEY</code>):</b>\n"
            f"• <code>GET  /api/v1/balance</code>\n"
            f"• <code>GET  /api/v1/products</code>\n"
            f"• <code>POST /api/v1/order</code> — <code>{{\"product_id\": 1, \"quantity\": 1}}</code>\n"
            f"• <code>GET  /api/v1/orders</code>\n\n"
            f"🌐 <b>Server Port:</b> <code>{port}</code> (Enable <code>ENABLE_WEB_API</code> in config)"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Generate / Rotate API Key", callback_data="user:devapi_regen")],
            [InlineKeyboardButton(text="« Main Menu", callback_data="user:home")],
        ])
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    @router.callback_query(F.data == "user:devapi_regen")
    async def cb_devapi_regen(cb: CallbackQuery):
        new_key = ctx.db.generate_user_api_key(cb.from_user.id)
        await cb.answer("🔑 New Developer API Key generated!", show_alert=True)
        await cb_devapi(cb)

    @router.callback_query(F.data == "user:referral")
    async def cb_referral(cb: CallbackQuery):
        u = ctx.db.get_user(cb.from_user.id)
        pct = ctx.db.get_setting_float("referral_commission_pct", 5.0)
        sym = ctx.currency()
        ref_link = f"https://t.me/{ctx.bot_username}?start=ref_{cb.from_user.id}" if ctx.bot_username else f"ref_{cb.from_user.id}"
        signup_bonus = ctx.db.get_setting_float("referral_signup_bonus_usd", 0.05)
        text = (
            f"👥 <b>Invite Friends &amp; Earn</b>\n\n"
            f"Share your unique personal referral link and earn:\n"
            f"• 💵 <b>Per Referral Signup Bonus:</b> <b>{fmt_money(signup_bonus, sym)} USDT</b> (added instantly to balance)\n"
            f"• 📈 <b>Lifetime Commission:</b> <b>{pct:.1f}%</b> on every deposit &amp; order!\n\n"
            f"🔗 <b>Your Unique Referral Link:</b>\n<code>{esc(ref_link)}</code>\n\n"
            f"📊 <b>Your Stats:</b>\n"
            f"• Invited Users: <b>{u['referral_count']}</b>\n"
            f"• Main Balance: <b>{fmt_money(u['balance'], sym)} USDT</b>\n"
            f"• Pending Commission: <b>{fmt_money(u['referral_balance'], sym)} USDT</b>"
        )
        rows = []
        if float(u["referral_balance"]) >= 0.01:
            rows.append([InlineKeyboardButton(text="💸 Transfer Earnings to Main Balance", callback_data="user:ref_claim")])
        rows.append([InlineKeyboardButton(text="« Main Menu", callback_data="user:home")])
        await safe_edit_or_send(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data == "user:ref_claim")
    async def cb_ref_claim(cb: CallbackQuery):
        moved = ctx.db.transfer_referral_to_main(cb.from_user.id)
        if moved > 0:
            await cb.answer(f"✅ Transferred {fmt_money(moved, ctx.currency())} to your Store Balance!", show_alert=True)
        else:
            await cb.answer("No referral earnings to transfer.", show_alert=True)
        await cb_referral(cb)

    @router.callback_query(F.data == "user:help")
    async def cb_help(cb: CallbackQuery):
        help_text = ctx.db.get_setting("help_text", "Use /shop to browse and /balance to top up.")
        await safe_edit_or_send(
            cb,
            help_text,
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛍️ Open Shop", callback_data="shop:page:1")],
                [InlineKeyboardButton(text="« Main Menu", callback_data="user:home")],
            ]),
        )
        await cb.answer()

    @router.callback_query(F.data == "user:support")
    async def cb_support(cb: CallbackQuery):
        sup_u = ctx.db.get_setting("support_username", "@Support")
        sup_handle = sup_u.lstrip("@")
        text = (
            f"💬 <b>Customer Support</b>\n\n"
            f"🚨 <b>Direct Support:</b> {esc(sup_u)}\n"
            f"You can also open a tracked Support Ticket with screenshots for faster resolution."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎫 Open Support Ticket", callback_data="tkt:new")],
            [InlineKeyboardButton(text=f"🚨 Message {sup_u} ↗", url=f"https://t.me/{sup_handle}")],
            [InlineKeyboardButton(text="« Main Menu", callback_data="user:home")],
        ])
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    # --- /tickets (Customer Support Ticket System) ---
    async def render_tickets_hub(target: Union[CallbackQuery, Message], user_id: int):
        active_cnt, total_cnt = ctx.db.count_user_tickets(user_id)
        sup_u = ctx.db.get_setting("support_username", "@Support")
        sup_handle = sup_u.lstrip("@")
        text = (
            f"🎫 <b>Customer Support Tickets</b>\n\n"
            f"Need help with an order, deposit, or account issue?\n"
            f"Submit a support ticket with text and screenshots for quick assistance from our admin team.\n\n"
            f"📊 <b>Your Tickets:</b> <code>{active_cnt}</code> active open • <code>{total_cnt}</code> total\n"
            f"🚨 <b>Direct Support:</b> {esc(sup_u)}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎫 Open New Ticket", callback_data="tkt:new")],
            [InlineKeyboardButton(text="📂 My Tickets", callback_data="tkt:list")],
            [InlineKeyboardButton(text=f"🚨 Direct Support ({sup_u}) ↗", url=f"https://t.me/{sup_handle}")],
            [InlineKeyboardButton(text="« Main Menu", callback_data="user:home")],
        ])
        await safe_edit_or_send(target, text, kb)

    @router.message(Command("tickets"))
    async def cmd_tickets(message: Message, state: FSMContext, bot: Bot):
        await state.clear()
        if not await guard_customer_access(bot, ctx, message.from_user.id, message):
            return
        await render_tickets_hub(message, message.from_user.id)

    @router.callback_query(F.data == "tkt:menu")
    async def cb_tkt_menu(cb: CallbackQuery, state: FSMContext):
        await state.clear()
        await render_tickets_hub(cb, cb.from_user.id)
        await cb.answer()

    @router.callback_query(F.data == "tkt:new")
    async def cb_tkt_new(cb: CallbackQuery, state: FSMContext):
        await state.set_state(UserStates.waiting_ticket_subject)
        await safe_edit_or_send(
            cb,
            "🎫 <b>Open New Support Ticket</b>\n\nStep 1/2: Enter a short subject (e.g. <code>Order ORD-1234 issue</code>):",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="tkt:menu")]]),
        )
        await cb.answer()

    @router.message(UserStates.waiting_ticket_subject)
    async def msg_tkt_subject(message: Message, state: FSMContext):
        subj = (message.text or "").strip()
        if len(subj) < 3:
            await message.answer("⚠️ Please enter a descriptive subject (at least 3 characters).")
            return
        await state.update_data(subject=subj)
        await state.set_state(UserStates.waiting_ticket_body)
        await message.answer(
            "Step 2/2: Describe your issue in detail (you may also send a screenshot with caption):"
        )

    @router.message(UserStates.waiting_ticket_body)
    async def msg_tkt_body(message: Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        body = (message.text or message.caption or "").strip()
        photo_id = message.photo[-1].file_id if message.photo else ""
        if not body and not photo_id:
            await message.answer("⚠️ Please send text or a screenshot.")
            return
        await state.clear()
        tid, tcode = ctx.db.create_ticket(message.from_user.id, data["subject"], body or "[Screenshot]", photo_id)
        await message.answer(
            f"✅ <b>Support Ticket Created!</b>\n\n"
            f"• Ticket ID: <code>{tcode}</code>\n"
            f"• Subject: {esc(data['subject'])}\n\n"
            f"Our team will reply shortly.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📂 View My Tickets", callback_data="tkt:list")],
                [InlineKeyboardButton(text="🏠 Main Menu", callback_data="user:home")],
            ]),
        )
        if ctx.owner_id:
            try:
                await bot.send_message(
                    ctx.owner_id,
                    f"🎫 <b>New Support Ticket {tcode}</b>\n"
                    f"• User: <code>{message.from_user.id}</code>\n"
                    f"• Subject: {esc(data['subject'])}\n"
                    f"• Message: {esc(body[:400])}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="💬 Open Ticket", callback_data=f"adm:tkt_view:{tid}")]]
                    ),
                )
            except Exception:
                pass

    @router.callback_query(F.data == "tkt:list")
    async def cb_tkt_list(cb: CallbackQuery):
        tickets = ctx.db.list_user_tickets(cb.from_user.id)
        if not tickets:
            await safe_edit_or_send(
                cb,
                "📂 You have no support tickets yet.",
                InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🎫 Open New Ticket", callback_data="tkt:new")],
                    [InlineKeyboardButton(text="« Back", callback_data="tkt:menu")],
                ]),
            )
            await cb.answer()
            return
        rows = []
        for t in tickets:
            ico = "🟢" if t["status"] == "open" else ("💬" if t["status"] == "answered" else "🔒")
            rows.append([
                InlineKeyboardButton(
                    text=f"{ico} {t['ticket_code']} — {t['subject'][:28]}",
                    callback_data=f"tkt:view:{t['id']}",
                )
            ])
        rows.append([InlineKeyboardButton(text="« Back", callback_data="tkt:menu")])
        await safe_edit_or_send(cb, "📂 <b>Your Support Tickets</b>", InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data.startswith("tkt:view:"))
    async def cb_tkt_view(cb: CallbackQuery):
        tid = int(cb.data.split(":")[2])
        t = ctx.db.get_ticket(tid)
        if not t or int(t["user_id"]) != cb.from_user.id:
            await cb.answer("Ticket not found.", show_alert=True)
            return
        msgs = ctx.db.get_ticket_messages(tid)
        lines = [
            f"🎫 <b>{esc(t['ticket_code'])} — {esc(t['subject'])}</b>",
            f"Status: <b>{esc(t['status']).upper()}</b>\n",
        ]
        for m in msgs[-8:]:
            who = "👤 You" if m["sender_role"] == "user" else "🛠️ Admin"
            lines.append(f"<b>{who}</b> ({m['created_at'][5:16]}):\n{esc(m['text'])}\n")
        rows = []
        if t["status"] != "closed":
            rows.append([InlineKeyboardButton(text="💬 Add Reply", callback_data=f"tkt:reply:{tid}")])
        rows.append([InlineKeyboardButton(text="« Back to Tickets", callback_data="tkt:list")])
        await safe_edit_or_send(cb, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data.startswith("tkt:reply:"))
    async def cb_tkt_reply(cb: CallbackQuery, state: FSMContext):
        tid = int(cb.data.split(":")[2])
        await state.set_state(UserStates.waiting_ticket_reply)
        await state.update_data(ticket_id=tid)
        await safe_edit_or_send(
            cb,
            "💬 Send your reply message (or screenshot with caption):",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data=f"tkt:view:{tid}")]]),
        )
        await cb.answer()

    @router.message(UserStates.waiting_ticket_reply)
    async def msg_tkt_reply(message: Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        tid = int(data["ticket_id"])
        text = (message.text or message.caption or "[Screenshot]").strip()
        photo_id = message.photo[-1].file_id if message.photo else ""
        await state.clear()
        ctx.db.add_ticket_message(tid, message.from_user.id, "user", text, photo_id)
        await message.answer(
            "✅ Reply added to your ticket.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="📂 View Ticket", callback_data=f"tkt:view:{tid}")]]
            ),
        )
        if ctx.owner_id:
            try:
                await bot.send_message(
                    ctx.owner_id,
                    f"💬 <b>Customer Reply on Ticket #{tid}</b>\n{esc(text[:400])}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="💬 Reply", callback_data=f"adm:tkt_view:{tid}")]]
                    ),
                )
            except Exception:
                pass

    return router


# ==============================================================================
# 11. FULL TELEGRAM ADMIN PANEL ROUTER (WITH RBAC SCOPES)
# ==============================================================================

def build_admin_router(ctx: BotAppContext) -> Router:
    router = Router(name="admin_router")

    async def require_scope(cb: CallbackQuery, scope: str) -> bool:
        if ctx.db.has_scope(cb.from_user.id, ctx.owner_id, scope):
            return True
        await cb.answer(f"⛔ Unauthorized: Missing '{scope}' scope", show_alert=True)
        return False

    # --- /admin Entry & Dashboard ---
    @router.message(Command("admin"))
    async def cmd_admin(message: Message, state: FSMContext):
        await state.clear()
        ctx.maybe_autobind_owner(message.from_user.id, message.from_user.username or "")
        if not ctx.db.is_admin_or_staff(message.from_user.id, ctx.owner_id):
            await message.answer("⛔ Unauthorized: Admin access required.")
            return
        text, kb = build_admin_dashboard(ctx)
        await safe_edit_or_send(message, text, kb)

    @router.callback_query(F.data == "adm:home")
    async def cb_adm_home(cb: CallbackQuery, state: FSMContext):
        await state.clear()
        ctx.maybe_autobind_owner(cb.from_user.id, cb.from_user.username or "")
        if not ctx.db.is_admin_or_staff(cb.from_user.id, ctx.owner_id):
            await cb.answer("⛔ Unauthorized: Admin access required.", show_alert=True)
            return
        text, kb = build_admin_dashboard(ctx)
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    # ==========================================================================
    # 11.1 CATALOG & MARGINS
    # ==========================================================================
    async def render_admin_catalog(target: Union[CallbackQuery, Message], page: int = 1, query: str = ""):
        margin = ctx.db.get_setting_float("default_margin_pct", 35.0)
        prods = ctx.db.search_products(query, limit=100) if query else ctx.db.list_products(include_inactive=True)
        per_page = 10
        total_pages = max(1, (len(prods) + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        chunk = prods[(page - 1) * per_page : page * per_page]
        hdr = f"🔍 Search: <b>{esc(query)}</b> ({len(prods)} found)" if query else f"Total Products: <b>{len(prods)}</b> (Page {page}/{total_pages})"
        text = (
            f"📦 <b>Catalog &amp; Individual Product Pricing</b>\n\n"
            f"• Global Default Markup: <b>+{margin:.1f}%</b>\n"
            f"• {hdr}\n\n"
            f"Tap any product below to change its Fixed Price ($) or Custom Margin (%):"
        )
        rows = [
            [
                InlineKeyboardButton(text="📈 Set Global Markup %", callback_data="adm:set_global_margin"),
                InlineKeyboardButton(text="🔍 Search Product", callback_data="adm:cat_search"),
            ],
            [InlineKeyboardButton(text="🔄 Sync All Suppliers", callback_data="adm:sync_all")],
        ]
        for p in chunk:
            sell = calculate_sell_price(float(p["cost_price"]), margin, p["margin_override_pct"], p["fixed_price"])
            st = "🟢" if p["is_active"] else "⚫"
            rows.append([
                InlineKeyboardButton(
                    text=f"{st} {p['name'][:22]} | ${p['cost_price']:.2f} → ${sell:.2f}",
                    callback_data=f"adm:prod_edit:{p['id']}",
                )
            ])
        if not query:
            nav = []
            if page > 1:
                nav.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"adm:cat_p:{page - 1}"))
            nav.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="noop"))
            if page < total_pages:
                nav.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"adm:cat_p:{page + 1}"))
            rows.append(nav)
        else:
            rows.append([InlineKeyboardButton(text="« Show All Products", callback_data="adm:catalog")])
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])
        await safe_edit_or_send(target, text, InlineKeyboardMarkup(inline_keyboard=rows))

    @router.callback_query(F.data == "adm:catalog")
    async def cb_adm_catalog(cb: CallbackQuery):
        if not await require_scope(cb, "catalog"):
            return
        await render_admin_catalog(cb, page=1)
        await safe_cb_answer(cb)

    @router.callback_query(F.data.startswith("adm:cat_p:"))
    async def cb_adm_cat_page(cb: CallbackQuery):
        if not await require_scope(cb, "catalog"):
            return
        await render_admin_catalog(cb, page=int(cb.data.split(":")[2]))
        await safe_cb_answer(cb)

    @router.callback_query(F.data == "adm:cat_search")
    async def cb_adm_cat_search(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "catalog"):
            return
        await state.set_state(AdminStates.waiting_catalog_search)
        await safe_edit_or_send(
            cb,
            "🔍 <b>Search Product to Edit Price</b>\n\nType the product or brand name (e.g. <code>ChatGPT</code>, <code>Spotify</code>, <code>Claude</code>):",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:catalog")]]),
        )
        await safe_cb_answer(cb)

    @router.message(AdminStates.waiting_catalog_search)
    async def msg_adm_cat_search(message: Message, state: FSMContext):
        q = (message.text or "").strip()
        await state.clear()
        await render_admin_catalog(message, page=1, query=q)

    @router.callback_query(F.data == "adm:set_global_margin")
    async def cb_set_global_margin(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "catalog"):
            return
        await state.set_state(AdminStates.waiting_global_margin)
        await safe_edit_or_send(
            cb,
            "📈 <b>Set Global Reseller Markup %</b>\n\n"
            "Enter the profit percentage added on top of wholesale cost (e.g. <code>35</code> for +35%, or <code>150</code> for +150%):",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:catalog")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_global_margin)
    async def msg_global_margin(message: Message, state: FSMContext):
        raw = (message.text or "").strip().replace("%", "").replace("+", "").replace(",", "").replace(" ", "")
        try:
            val = float(raw)
            if val < 0 or val > 5000:
                raise ValueError
        except ValueError:
            await message.answer("⚠️ Enter a valid percentage number between 0 and 5000 (e.g. <code>30</code> or <code>50</code>).", parse_mode=ParseMode.HTML)
            return
        ctx.db.set_setting("default_margin_pct", val)
        ctx.db.log_audit(message.from_user.id, "set_global_margin", f"{val}%")
        await state.clear()
        sample_sell = calculate_sell_price(10.0, val)
        await message.answer(
            f"✅ <b>Global Profit Margin Updated!</b>\n\n"
            f"• New Markup: <b>+{val:.1f}%</b>\n"
            f"• Example: $10.00 wholesale item → sells at <b>${sample_sell:.2f}</b> (+${sample_sell - 10.0:.2f} profit)",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")],
                [InlineKeyboardButton(text="📦 Open Catalog", callback_data="adm:catalog")],
            ]),
        )

    @router.callback_query(F.data.startswith("adm:prod_edit:"))
    async def cb_adm_prod_edit(cb: CallbackQuery):
        if not await require_scope(cb, "catalog"):
            return
        pid = int(cb.data.split(":")[2])
        p = ctx.db.get_product(pid)
        if not p:
            await cb.answer("Product not found.", show_alert=True)
            return
        gm = ctx.db.get_setting_float("default_margin_pct", 35.0)
        sell = calculate_sell_price(float(p["cost_price"]), gm, p["margin_override_pct"], p["fixed_price"])
        text = (
            f"📦 <b>Edit Product #{p['id']}</b>\n\n"
            f"• <b>Name:</b> {esc(p['name'])}\n"
            f"• <b>Brand:</b> {esc(p['brand'])}\n"
            f"• <b>Type:</b> {'Custom Local' if p['is_custom'] else 'Upstream Supplier'}\n"
            f"• <b>Wholesale Cost:</b> ${p['cost_price']:.2f}\n"
            f"• <b>Margin Override:</b> {p['margin_override_pct'] if p['margin_override_pct'] is not None else 'Global'}\n"
            f"• <b>Fixed Sell Price:</b> {fmt_money(p['fixed_price']) if p['fixed_price'] else 'Auto'}\n"
            f"• <b>Effective Retail Price:</b> <b>${sell:.2f}</b>\n"
            f"• <b>Stock:</b> {p['stock']} | <b>Status:</b> {'Active' if p['is_active'] else 'Hidden'}"
        )
        rows = [
            [
                InlineKeyboardButton(text="📈 Set Custom Margin %", callback_data=f"adm:pmargin:{pid}"),
                InlineKeyboardButton(text="💵 Set Fixed Price $", callback_data=f"adm:pfixed:{pid}"),
            ],
            [
                InlineKeyboardButton(
                    text="👁️ Hide Product" if p["is_active"] else "👁️ Show Product",
                    callback_data=f"adm:ptoggle:{pid}",
                ),
                InlineKeyboardButton(text="🗑️ Delete", callback_data=f"adm:pdel:{pid}"),
            ],
        ]
        if p["is_custom"]:
            rows.append([InlineKeyboardButton(text="➕ Add Stock Lines", callback_data=f"adm:cstock:{pid}")])
        rows.append([InlineKeyboardButton(text="« Back to Catalog", callback_data="adm:catalog")])
        await safe_edit_or_send(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data.startswith("adm:ptoggle:"))
    async def cb_adm_ptoggle(cb: CallbackQuery):
        if not await require_scope(cb, "catalog"):
            return
        pid = int(cb.data.split(":")[2])
        ctx.db.toggle_product_active(pid)
        await cb_adm_prod_edit(cb)

    @router.callback_query(F.data.startswith("adm:pdel:"))
    async def cb_adm_pdel(cb: CallbackQuery):
        if not await require_scope(cb, "catalog"):
            return
        pid = int(cb.data.split(":")[2])
        ctx.db.delete_product(pid)
        await cb.answer("🗑️ Product deleted.", show_alert=True)
        await cb_adm_catalog(cb)

    @router.callback_query(F.data.startswith("adm:pmargin:"))
    async def cb_adm_pmargin(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "catalog"):
            return
        pid = int(cb.data.split(":")[2])
        await state.set_state(AdminStates.waiting_prod_margin)
        await state.update_data(product_id=pid)
        await safe_edit_or_send(
            cb,
            "📈 Send custom markup % for this product (or send <code>reset</code> to use Global markup):",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data=f"adm:prod_edit:{pid}")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_prod_margin)
    async def msg_adm_pmargin(message: Message, state: FSMContext):
        pid = int((await state.get_data())["product_id"])
        raw = (message.text or "").strip().lower()
        if raw == "reset":
            ctx.db.update_product_pricing(pid, margin_override_pct=None, fixed_price=None)
        else:
            try:
                val = float(raw.rstrip("%"))
                ctx.db.update_product_pricing(pid, margin_override_pct=val, fixed_price=None)
            except ValueError:
                await message.answer("⚠️ Send a number (e.g. 50) or <code>reset</code>.", parse_mode=ParseMode.HTML)
                return
        await state.clear()
        await message.answer(
            "✅ Product margin updated.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« View Product", callback_data=f"adm:prod_edit:{pid}")]]),
        )

    @router.callback_query(F.data.startswith("adm:pfixed:"))
    async def cb_adm_pfixed(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "catalog"):
            return
        pid = int(cb.data.split(":")[2])
        await state.set_state(AdminStates.waiting_prod_fixed)
        await state.update_data(product_id=pid)
        await safe_edit_or_send(
            cb,
            "💵 Send fixed retail price in USD (e.g. <code>9.99</code>) or <code>reset</code> for auto margin:",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data=f"adm:prod_edit:{pid}")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_prod_fixed)
    async def msg_adm_pfixed(message: Message, state: FSMContext):
        pid = int((await state.get_data())["product_id"])
        raw = (message.text or "").strip().lower()
        if raw == "reset":
            ctx.db.update_product_pricing(pid, margin_override_pct=None, fixed_price=None)
        else:
            try:
                val = float(raw.lstrip("$"))
                ctx.db.update_product_pricing(pid, margin_override_pct=None, fixed_price=val)
            except ValueError:
                await message.answer("⚠️ Send a valid USD price (e.g. 4.99) or <code>reset</code>.", parse_mode=ParseMode.HTML)
                return
        await state.clear()
        await message.answer(
            "✅ Fixed retail price updated.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« View Product", callback_data=f"adm:prod_edit:{pid}")]]),
        )

    @router.callback_query(F.data == "adm:sync_all")
    async def cb_adm_sync_all(cb: CallbackQuery, bot: Bot):
        if not await require_scope(cb, "catalog"):
            return
        suppliers = ctx.db.list_suppliers(only_active=True)
        if not suppliers:
            await cb.answer("No active suppliers configured. Add one in External Suppliers.", show_alert=True)
            return
        await cb.answer("🔄 Syncing suppliers...")
        lines = ["🔄 <b>Supplier Sync Report</b>\n"]
        for s in suppliers:
            ok, cnt, restocked, err = await ctx.supplier_client.sync_catalog(ctx.db, s)
            if ok:
                lines.append(f"✅ <b>{esc(s['name'])}</b>: {cnt} items synced ({len(restocked)} restocked)")
                if restocked:
                    asyncio.create_task(dispatch_restock_batch(bot, ctx, restocked))
            else:
                lines.append(f"❌ <b>{esc(s['name'])}</b>: {esc(err)}")
        await safe_edit_or_send(
            cb,
            "\n".join(lines),
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Back to Catalog", callback_data="adm:catalog")]]),
        )

    # ==========================================================================
    # 11.2 CUSTOM LOCAL PRODUCTS & BULK STOCK
    # ==========================================================================
    @router.callback_query(F.data == "adm:custom")
    async def cb_adm_custom(cb: CallbackQuery):
        if not await require_scope(cb, "custom_products"):
            return
        prods = ctx.db.list_products(only_custom=True, include_inactive=True)
        text = (
            f"🏪 <b>Custom Local Products ({len(prods)})</b>\n\n"
            f"Sell your own keys/accounts directly without any upstream supplier.\n"
            f"Tap a product to add stock lines or edit:"
        )
        rows = [[InlineKeyboardButton(text="➕ Create Custom Product", callback_data="adm:custom_new")]]
        for p in prods[:12]:
            rows.append([
                InlineKeyboardButton(
                    text=f"{p['brand']} | {p['name'][:20]} | ${p['fixed_price'] or 0:.2f} | Stock: {p['stock']}",
                    callback_data=f"adm:prod_edit:{p['id']}",
                )
            ])
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])
        await safe_edit_or_send(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data == "adm:custom_new")
    async def cb_adm_custom_new(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "custom_products"):
            return
        await state.set_state(AdminStates.waiting_custom_prod_data)
        await safe_edit_or_send(
            cb,
            "➕ <b>Create Custom Product</b>\n\n"
            "Send product details separated by <code>|</code> in one message:\n"
            "<code>Brand | Product Name | SellPrice | CostPrice | Warranty | Description</code>\n\n"
            "Example:\n"
            "<code>ChatGPT | ChatGPT Plus 1 Month Private | 8.50 | 4.00 | 30 Days | Instant login details</code>",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:custom")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_custom_prod_data)
    async def msg_custom_prod_data(message: Message, state: FSMContext):
        parts = [x.strip() for x in (message.text or "").split("|")]
        if len(parts) < 3:
            await message.answer("⚠️ Format: <code>Brand | Name | SellPrice | CostPrice | Warranty | Description</code>", parse_mode=ParseMode.HTML)
            return
        brand, name = parts[0], parts[1]
        try:
            sell_price = float(parts[2].lstrip("$"))
            cost_price = float(parts[3].lstrip("$")) if len(parts) > 3 and parts[3] else 0.0
        except ValueError:
            await message.answer("⚠️ SellPrice and CostPrice must be valid numbers.")
            return
        warranty = parts[4] if len(parts) > 4 else "Standard Warranty"
        desc = parts[5] if len(parts) > 5 else ""
        pid = ctx.db.create_custom_product(brand, name, sell_price, cost_price, warranty, desc)
        await state.clear()
        await message.answer(
            f"✅ Custom product <b>{esc(name)}</b> created (ID #{pid})!\nNow add credential stock lines to make it purchasable.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Add Stock Lines Now", callback_data=f"adm:cstock:{pid}")],
                [InlineKeyboardButton(text="« Custom Products", callback_data="adm:custom")],
            ]),
        )

    @router.callback_query(F.data.startswith("adm:cstock:"))
    async def cb_adm_cstock(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "custom_products"):
            return
        pid = int(cb.data.split(":")[2])
        await state.set_state(AdminStates.waiting_custom_stock_lines)
        await state.update_data(product_id=pid)
        await safe_edit_or_send(
            cb,
            "📥 <b>Add Bulk Stock Lines</b>\n\n"
            "Paste your accounts/keys below — <b>one deliverable item per line</b>:\n"
            "<code>email1@mail.com:pass1\nemail2@mail.com:pass2</code>",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data=f"adm:prod_edit:{pid}")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_custom_stock_lines)
    async def msg_cstock_lines(message: Message, state: FSMContext, bot: Bot):
        pid = int((await state.get_data())["product_id"])
        lines = (message.text or "").splitlines()
        added, restocked = ctx.db.add_local_stock_lines(pid, lines)
        await state.clear()
        if added > 0:
            await notify_restock(bot, ctx, pid, added)
        await message.answer(
            f"✅ Added <b>{added}</b> stock unit(s) to product #{pid}.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« View Product", callback_data=f"adm:prod_edit:{pid}")]]),
        )

    # ==========================================================================
    # 11.3 ORDERS HISTORY & REFUNDS
    # ==========================================================================
    @router.callback_query(F.data.startswith("adm:orders:"))
    async def cb_adm_orders(cb: CallbackQuery):
        if not await require_scope(cb, "orders"):
            return
        page = int(cb.data.split(":")[2])
        orders = ctx.db.get_recent_orders(limit=60)
        per_page = 8
        total_pages = max(1, (len(orders) + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        chunk = orders[(page - 1) * per_page : page * per_page]
        sym = ctx.currency()

        rows = [[InlineKeyboardButton(text="🔍 Search by Order Code", callback_data="adm:ord_search")]]
        for o in chunk:
            st = "✅" if o["status"] == "completed" else ("↩️" if o["status"] == "refunded" else "❌")
            rows.append([
                InlineKeyboardButton(
                    text=f"{st} {o['order_code']} | {fmt_money(o['total_paid'], sym)} (+{fmt_money(o['net_profit'], sym)})",
                    callback_data=f"adm:ord_view:{o['id']}",
                )
            ])
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"adm:orders:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="noop"))
        if page < total_pages:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"adm:orders:{page + 1}"))
        rows.append(nav)
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])
        await safe_edit_or_send(cb, f"📋 <b>Orders History ({len(orders)} recent)</b>", InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data == "adm:ord_search")
    async def cb_adm_ord_search(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "orders"):
            return
        await state.set_state(AdminStates.waiting_order_search)
        await safe_edit_or_send(
            cb,
            "🔍 Enter Order Code (e.g. <code>ORD-AB12CD34</code>):",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:orders:1")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_order_search)
    async def msg_adm_ord_search(message: Message, state: FSMContext):
        await state.clear()
        o = ctx.db.get_order_by_code(message.text or "")
        if not o:
            await message.answer("⚠️ Order not found.")
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📋 Open Order Details", callback_data=f"adm:ord_view:{o['id']}")]])
        await message.answer(f"Found order <code>{esc(o['order_code'])}</code>.", parse_mode=ParseMode.HTML, reply_markup=kb)

    @router.callback_query(F.data.startswith("adm:ord_view:"))
    async def cb_adm_ord_view(cb: CallbackQuery):
        if not await require_scope(cb, "orders"):
            return
        oid = int(cb.data.split(":")[2])
        o = ctx.db.get_order_by_id(oid)
        if not o:
            await cb.answer("Order not found.", show_alert=True)
            return
        sym = ctx.currency()
        text = (
            f"📋 <b>Admin Order Detail — {esc(o['order_code'])}</b>\n\n"
            f"• <b>User ID:</b> <code>{o['user_id']}</code>\n"
            f"• <b>Product:</b> {esc(o['product_name'])} × {o['quantity']}\n"
            f"• <b>Cost/Unit:</b> {fmt_money(o['cost_unit_price'], sym)} | <b>Sell/Unit:</b> {fmt_money(o['sell_unit_price'], sym)}\n"
            f"• <b>Total Paid:</b> {fmt_money(o['total_paid'], sym)} | <b>Net Profit:</b> <b>{fmt_money(o['net_profit'], sym)}</b>\n"
            f"• <b>Supplier Order ID:</b> <code>{esc(o['supplier_order_id'] or 'N/A')}</code>\n"
            f"• <b>Status:</b> {esc(o['status']).upper()} | <b>Source:</b> {esc(o['source'])}\n"
            f"• <b>Date:</b> {esc(o['created_at'])}\n\n"
            f"<pre>{esc(o['delivered_data'][:1500])}</pre>"
        )
        rows = []
        if o["status"] == "completed":
            rows.append([InlineKeyboardButton(text="↩️ Refund to User Balance", callback_data=f"adm:ord_refund:{oid}")])
        rows.append([InlineKeyboardButton(text="« Back to Orders", callback_data="adm:orders:1")])
        await safe_edit_or_send(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data.startswith("adm:ord_refund:"))
    async def cb_adm_ord_refund(cb: CallbackQuery, bot: Bot):
        if not await require_scope(cb, "orders"):
            return
        oid = int(cb.data.split(":")[2])
        o = ctx.db.get_order_by_id(oid)
        ok, msg = ctx.db.refund_order(oid)
        await cb.answer(msg, show_alert=True)
        if ok and o:
            ctx.db.log_audit(cb.from_user.id, "refund_order", o["order_code"])
            try:
                await bot.send_message(
                    int(o["user_id"]),
                    f"↩️ Your order <code>{esc(o['order_code'])}</code> was refunded ({fmt_money(o['total_paid'], ctx.currency())} added to your Store Balance).",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        await cb_adm_ord_view(cb)

    # ==========================================================================
    # 11.4 DEPOSITS LOG & 1-CLICK APPROVAL
    # ==========================================================================
    @router.callback_query(F.data.startswith("adm:deposits:"))
    async def cb_adm_deposits(cb: CallbackQuery):
        if not await require_scope(cb, "deposits"):
            return
        flt = cb.data.split(":")[2]
        deps = ctx.db.list_deposits(status=None if flt == "all" else flt, limit=25)
        sym = ctx.currency()
        rows = [[
            InlineKeyboardButton(text="⏳ Pending", callback_data="adm:deposits:pending"),
            InlineKeyboardButton(text="✅ Confirmed", callback_data="adm:deposits:confirmed"),
            InlineKeyboardButton(text="📑 All", callback_data="adm:deposits:all"),
        ]]
        for d in deps[:12]:
            ico = "⏳" if d["status"] == "pending" else ("✅" if d["status"] == "confirmed" else "❌")
            rows.append([
                InlineKeyboardButton(
                    text=f"{ico} {d['deposit_code']} | {fmt_money(d['amount_usd'], sym)} | {d['gateway']}",
                    callback_data=f"adm:dep_view:{d['id']}",
                )
            ])
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])
        await safe_edit_or_send(cb, f"📑 <b>Deposits Log ({flt.upper()}) — {len(deps)} records</b>", InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data.startswith("adm:dep_view:"))
    async def cb_adm_dep_view(cb: CallbackQuery):
        if not await require_scope(cb, "deposits"):
            return
        dep_id = int(cb.data.split(":")[2])
        d = ctx.db.get_deposit(dep_id)
        if not d:
            await cb.answer("Deposit not found.", show_alert=True)
            return
        text = (
            f"💳 <b>Deposit #{d['id']} — {esc(d['deposit_code'])}</b>\n\n"
            f"• <b>User ID:</b> <code>{d['user_id']}</code>\n"
            f"• <b>Gateway:</b> {esc(d['gateway'])}\n"
            f"• <b>Amount:</b> <b>{fmt_money(d['amount_usd'], ctx.currency())} USD</b>\n"
            f"• <b>Tx Reference:</b> <code>{esc(d['tx_reference'] or 'None')}</code>\n"
            f"• <b>Status:</b> {esc(d['status']).upper()}\n"
            f"• <b>Created:</b> {esc(d['created_at'])}"
        )
        rows = []
        if d["status"] == "pending":
            rows.append([
                InlineKeyboardButton(text="✅ Approve & Credit", callback_data=f"adm:dep_ok:{dep_id}"),
                InlineKeyboardButton(text="❌ Reject", callback_data=f"adm:dep_no:{dep_id}"),
            ])
        rows.append([InlineKeyboardButton(text="« Back to Deposits", callback_data="adm:deposits:pending")])
        await safe_edit_or_send(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data.startswith("adm:dep_ok:"))
    async def cb_adm_dep_ok(cb: CallbackQuery, bot: Bot):
        if not await require_scope(cb, "deposits"):
            return
        dep_id = int(cb.data.split(":")[2])
        ok, dep = ctx.db.confirm_deposit(dep_id)
        if not ok:
            await cb.answer("Deposit already processed.", show_alert=True)
            return
        ctx.db.log_audit(cb.from_user.id, "approve_deposit", dep["deposit_code"])
        await cb.answer("✅ Deposit approved and balance credited!", show_alert=True)
        try:
            u_cred = ctx.db.get_user(int(dep["user_id"]))
            bal_now = float(u_cred["balance"]) if u_cred else float(dep["amount_usd"])
            await bot.send_message(
                int(dep["user_id"]),
                f"🎉 <b>Payment Successfully! Top-Up Completed!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"• Reference: <code>{esc(dep['deposit_code'])}</code>\n"
                f"• Credited: <b>+{fmt_money(dep['amount_usd'], ctx.currency())} USDT</b>\n"
                f"• New Balance: <b>{fmt_money(bal_now, ctx.currency())} USDT</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🛍️ Open Shop Now", callback_data="shop:page:1")],
                    [InlineKeyboardButton(text="🏠 Main Menu", callback_data="user:home")],
                ]),
            )
        except Exception:
            pass
        await cb_adm_dep_view(cb)

    @router.callback_query(F.data.startswith("adm:dep_no:"))
    async def cb_adm_dep_no(cb: CallbackQuery, bot: Bot):
        if not await require_scope(cb, "deposits"):
            return
        dep_id = int(cb.data.split(":")[2])
        dep = ctx.db.get_deposit(dep_id)
        if ctx.db.reject_deposit(dep_id) and dep:
            ctx.db.log_audit(cb.from_user.id, "reject_deposit", dep["deposit_code"])
            await cb.answer("❌ Deposit rejected.", show_alert=True)
            try:
                await bot.send_message(
                    int(dep["user_id"]),
                    f"❌ Your deposit <code>{esc(dep['deposit_code'])}</code> ({fmt_money(dep['amount_usd'], ctx.currency())}) was rejected. Contact support if this is a mistake.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        else:
            await cb.answer("Deposit already processed.", show_alert=True)
        await cb_adm_dep_view(cb)

    # ==========================================================================
    # 11.5 USERS MANAGEMENT
    # ==========================================================================
    async def render_users_directory(cb: CallbackQuery, state: FSMContext, page: int = 1):
        await state.set_state(AdminStates.waiting_user_search)
        per_page = 10
        users_list, total = ctx.db.list_users_paginated(page=page, per_page=per_page)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        sym = ctx.currency()
        now_dt = datetime.now(timezone.utc)

        online_cnt = 0
        rows: List[List[InlineKeyboardButton]] = []
        for u in users_list:
            try:
                last_dt = datetime.strptime(u["last_active"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                is_online = (now_dt - last_dt).total_seconds() <= 900  # active within 15 min
            except Exception:
                is_online = False
            if is_online:
                online_cnt += 1
            st_ico = "🚫" if u["is_banned"] else ("🟢" if is_online else "⚫")
            uname = f"@{u['username']}" if u["username"] else (u["full_name"] or str(u["user_id"]))
            label = f"{st_ico} {uname[:18]} | {fmt_money(u['balance'], sym)} | #{u['user_id']}"
            rows.append([InlineKeyboardButton(text=label, callback_data=f"adm:uopen:{u['user_id']}")])

        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"adm:users_p:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="noop"))
        if page < total_pages:
            nav.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"adm:users_p:{page + 1}"))
        rows.append(nav)
        rows.append([
            InlineKeyboardButton(text="📢 Notice All Users", callback_data="adm:bc_mode:both"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"adm:users_p:{page}"),
        ])
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])

        text = (
            f"👥 <b>Users Management ({total} Total • 🟢 {online_cnt} Online)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🟢 Online (15m)  •  ⚫ Offline  •  🚫 Banned\n\n"
            f"Tap any user below to manage, <b>OR type a Telegram ID / @username</b> directly:"
        )
        await safe_edit_or_send(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))

    @router.callback_query(F.data == "adm:users")
    async def cb_adm_users(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "users"):
            return
        await render_users_directory(cb, state, 1)
        await safe_cb_answer(cb)

    @router.callback_query(F.data.startswith("adm:users_p:"))
    async def cb_adm_users_page(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "users"):
            return
        page = int(cb.data.split(":")[2])
        await render_users_directory(cb, state, page)
        await safe_cb_answer(cb)

    @router.callback_query(F.data.startswith("adm:uopen:"))
    async def cb_adm_uopen(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "users"):
            return
        await state.clear()
        uid = int(cb.data.split(":")[2])
        await render_admin_user_card(cb, uid)
        await safe_cb_answer(cb)

    async def render_admin_user_card(target: Union[CallbackQuery, Message], uid: int):
        u = ctx.db.get_user(uid)
        if not u:
            await safe_edit_or_send(target, "⚠️ User not found.")
            return
        sym = ctx.currency()
        text = (
            f"👤 <b>User Management — {u['user_id']}</b>\n\n"
            f"• <b>Name:</b> {esc(u['full_name'])}\n"
            f"• <b>Username:</b> @{esc(u['username'] or 'None')}\n"
            f"• <b>Balance:</b> <b>{fmt_money(u['balance'], sym)}</b>\n"
            f"• <b>Deposited:</b> {fmt_money(u['total_deposited'], sym)} | <b>Spent:</b> {fmt_money(u['total_spent'], sym)}\n"
            f"• <b>Referrals:</b> {u['referral_count']} ({fmt_money(u['referral_balance'], sym)})\n"
            f"• <b>Status:</b> {'🚫 BANNED' if u['is_banned'] else '🟢 Active'}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💰 Add / Deduct Balance", callback_data=f"adm:ubal:{uid}"),
                InlineKeyboardButton(text="🧹 Reset to $0", callback_data=f"adm:uzero:{uid}"),
            ],
            [InlineKeyboardButton(text="💬 Send Direct Message", callback_data=f"adm:udm:{uid}")],
            [InlineKeyboardButton(
                text="✅ Unban User" if u["is_banned"] else "🚫 Ban User",
                callback_data=f"adm:uban:{uid}",
            )],
            [InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")],
        ])
        await safe_edit_or_send(target, text, kb)

    @router.message(AdminStates.waiting_user_search)
    async def msg_adm_user_search(message: Message, state: FSMContext):
        u = ctx.db.find_user(message.text or "")
        if not u:
            await message.answer("⚠️ No user matched that ID or username. Try again or tap Back.")
            return
        await state.clear()
        await render_admin_user_card(message, int(u["user_id"]))

    @router.callback_query(F.data.startswith("adm:ubal:"))
    async def cb_adm_ubal(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "users"):
            return
        uid = int(cb.data.split(":")[2])
        await state.set_state(AdminStates.waiting_user_balance_delta)
        await state.update_data(target_user_id=uid)
        await safe_edit_or_send(
            cb,
            f"💰 Send amount to add or deduct for user <code>{uid}</code>:\n"
            f"• Positive adds funds: <code>+10.00</code> or <code>10</code>\n"
            f"• Negative deducts funds: <code>-5.50</code>",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:home")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_user_balance_delta)
    async def msg_adm_ubal(message: Message, state: FSMContext, bot: Bot):
        uid = int((await state.get_data())["target_user_id"])
        raw_num = (message.text or "").strip().replace("$", "").replace(",", "").replace(" ", "")
        try:
            delta = float(raw_num)
        except ValueError:
            await message.answer("⚠️ Send a valid number like <code>15</code> or <code>-5</code> or <code>-1000</code>.", parse_mode=ParseMode.HTML)
            return
        # Prevent negative balance when deducting
        u_now = ctx.db.get_user(uid)
        if u_now and delta < 0 and (float(u_now["balance"]) + delta) < 0:
            delta = -float(u_now["balance"])
        ok, new_bal = ctx.db.adjust_balance(uid, delta, is_deposit=(delta > 0))
        await state.clear()
        if ok:
            ctx.db.log_audit(message.from_user.id, "adjust_user_balance", f"uid={uid} delta={delta}")
            try:
                sign = "+" if delta >= 0 else ""
                await bot.send_message(
                    uid,
                    f"💰 Your store balance was adjusted by <b>{sign}{fmt_money(delta, ctx.currency())}</b>.\n"
                    f"New Balance: <b>{fmt_money(new_bal, ctx.currency())}</b>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        await render_admin_user_card(message, uid)

    @router.callback_query(F.data.startswith("adm:uzero:"))
    async def cb_adm_uzero(cb: CallbackQuery):
        if not await require_scope(cb, "users"):
            return
        uid = int(cb.data.split(":")[2])
        u = ctx.db.get_user(uid)
        if u and float(u["balance"]) != 0:
            ctx.db.adjust_balance(uid, -float(u["balance"]))
            ctx.db.log_audit(cb.from_user.id, "reset_user_balance", f"uid={uid}")
        await safe_cb_answer(cb, "✅ Balance reset to $0.00!", show_alert=True)
        await render_admin_user_card(cb, uid)

    @router.callback_query(F.data.startswith("adm:uban:"))
    async def cb_adm_uban(cb: CallbackQuery):
        if not await require_scope(cb, "users"):
            return
        uid = int(cb.data.split(":")[2])
        u = ctx.db.get_user(uid)
        if not u:
            return
        ctx.db.set_user_ban(uid, not bool(u["is_banned"]), "Admin action")
        await cb.answer("User ban status updated.", show_alert=True)
        await render_admin_user_card(cb, uid)

    # ==========================================================================
    # 11.6 SUPPORT TICKETS (ADMIN QUEUE)
    # ==========================================================================
    @router.callback_query(F.data == "adm:tickets")
    async def cb_adm_tickets(cb: CallbackQuery):
        if not await require_scope(cb, "tickets"):
            return
        tickets = ctx.db.list_all_tickets(only_open=False)
        rows = []
        for t in tickets[:15]:
            ico = "🟢" if t["status"] == "open" else ("💬" if t["status"] == "answered" else "🔒")
            rows.append([
                InlineKeyboardButton(
                    text=f"{ico} {t['ticket_code']} | U:{t['user_id']} | {t['subject'][:18]}",
                    callback_data=f"adm:tkt_view:{t['id']}",
                )
            ])
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])
        await safe_edit_or_send(cb, f"🎫 <b>Support Tickets Queue ({len(tickets)})</b>", InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data.startswith("adm:tkt_view:"))
    async def cb_adm_tkt_view(cb: CallbackQuery):
        if not await require_scope(cb, "tickets"):
            return
        tid = int(cb.data.split(":")[2])
        t = ctx.db.get_ticket(tid)
        if not t:
            await cb.answer("Ticket not found.", show_alert=True)
            return
        msgs = ctx.db.get_ticket_messages(tid)
        lines = [
            f"🎫 <b>Ticket {esc(t['ticket_code'])}</b> (User: <code>{t['user_id']}</code>)",
            f"<b>Subject:</b> {esc(t['subject'])} | <b>Status:</b> {esc(t['status']).upper()}\n",
        ]
        for m in msgs[-8:]:
            who = "👤 Customer" if m["sender_role"] == "user" else "🛠️ Admin"
            lines.append(f"<b>{who}</b> ({m['created_at'][5:16]}):\n{esc(m['text'])}\n")
        rows = []
        if t["status"] != "closed":
            rows.append([
                InlineKeyboardButton(text="💬 Reply", callback_data=f"adm:tkt_reply:{tid}"),
                InlineKeyboardButton(text="🔒 Close Ticket", callback_data=f"adm:tkt_close:{tid}"),
            ])
        rows.append([InlineKeyboardButton(text="« Back to Tickets", callback_data="adm:tickets")])
        await safe_edit_or_send(cb, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data.startswith("adm:tkt_reply:"))
    async def cb_adm_tkt_reply(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "tickets"):
            return
        tid = int(cb.data.split(":")[2])
        await state.set_state(AdminStates.waiting_ticket_admin_reply)
        await state.update_data(ticket_id=tid)
        await safe_edit_or_send(
            cb,
            f"💬 Send your admin reply for Ticket #{tid} (text or photo with caption):",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data=f"adm:tkt_view:{tid}")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_ticket_admin_reply)
    async def msg_adm_tkt_reply(message: Message, state: FSMContext, bot: Bot):
        tid = int((await state.get_data())["ticket_id"])
        text = (message.text or message.caption or "[Image]").strip()
        photo_id = message.photo[-1].file_id if message.photo else ""
        await state.clear()
        ctx.db.add_ticket_message(tid, message.from_user.id, "admin", text, photo_id)
        t = ctx.db.get_ticket(tid)
        if t:
            try:
                notify_txt = f"💬 <b>Support Reply on {esc(t['ticket_code'])}</b>\n\n{esc(text)}"
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📂 View & Reply", callback_data=f"tkt:view:{tid}")]])
                if photo_id:
                    await bot.send_photo(int(t["user_id"]), photo_id, caption=notify_txt, reply_markup=kb, parse_mode=ParseMode.HTML)
                else:
                    await bot.send_message(int(t["user_id"]), notify_txt, reply_markup=kb, parse_mode=ParseMode.HTML)
            except Exception:
                pass
        await message.answer(
            "✅ Reply sent to customer.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« View Ticket", callback_data=f"adm:tkt_view:{tid}")]]),
        )

    @router.callback_query(F.data.startswith("adm:tkt_close:"))
    async def cb_adm_tkt_close(cb: CallbackQuery):
        if not await require_scope(cb, "tickets"):
            return
        tid = int(cb.data.split(":")[2])
        ctx.db.close_ticket(tid)
        await cb.answer("🔒 Ticket closed.", show_alert=True)
        await cb_adm_tkt_view(cb)

    # ==========================================================================
    # 11.7 PROMO CODES / COUPONS
    # ==========================================================================
    @router.callback_query(F.data == "adm:coupons")
    async def cb_adm_coupons(cb: CallbackQuery):
        if not await require_scope(cb, "coupons"):
            return
        coupons = ctx.db.list_coupons()
        rows = [[InlineKeyboardButton(text="➕ Create Promo Code", callback_data="adm:coupon_new")]]
        for c in coupons[:15]:
            val_txt = f"{c['discount_value']:.0f}%" if c["discount_type"] == "percent" else f"${c['discount_value']:.2f}"
            rows.append([
                InlineKeyboardButton(
                    text=f"🗑️ {c['code']} ({val_txt} | Used {c['used_count']}/{c['max_uses'] or '∞'})",
                    callback_data=f"adm:coupon_del:{c['code']}",
                )
            ])
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])
        await safe_edit_or_send(cb, f"🏷️ <b>Promo Codes ({len(coupons)})</b>\nTap any code to delete:", InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data == "adm:coupon_new")
    async def cb_adm_coupon_new(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "coupons"):
            return
        await state.set_state(AdminStates.waiting_coupon_create)
        await safe_edit_or_send(
            cb,
            "🏷️ <b>Create Promo Code</b>\n\n"
            "Send format: <code>CODE | percent/fixed | VALUE | MIN_ORDER | MAX_USES</code>\n"
            "Example: <code>SAVE15 | percent | 15 | 5 | 100</code>",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:coupons")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_coupon_create)
    async def msg_adm_coupon_create(message: Message, state: FSMContext):
        parts = [x.strip() for x in (message.text or "").split("|")]
        if len(parts) < 3:
            await message.answer("⚠️ Format: <code>CODE | percent | 15 | 5 | 100</code>", parse_mode=ParseMode.HTML)
            return
        code = parts[0].upper()
        dtype = "fixed" if parts[1].lower().startswith("f") else "percent"
        try:
            dval = float(parts[2])
            min_ord = float(parts[3]) if len(parts) > 3 and parts[3] else 0.0
            max_uses = int(parts[4]) if len(parts) > 4 and parts[4] else 0
        except ValueError:
            await message.answer("⚠️ Invalid numeric values.")
            return
        ctx.db.create_coupon(code, dtype, dval, min_ord, max_uses)
        await state.clear()
        await message.answer(
            f"✅ Promo code <code>{esc(code)}</code> created!",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Promo Codes", callback_data="adm:coupons")]]),
        )

    @router.callback_query(F.data.startswith("adm:coupon_del:"))
    async def cb_adm_coupon_del(cb: CallbackQuery):
        if not await require_scope(cb, "coupons"):
            return
        code = cb.data.split(":")[2]
        ctx.db.delete_coupon(code)
        await cb.answer(f"Deleted {code}.", show_alert=True)
        await cb_adm_coupons(cb)

    # ==========================================================================
    # 11.8 BROADCAST
    # ==========================================================================
    @router.callback_query(F.data == "adm:broadcast")
    async def cb_adm_broadcast(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "broadcast"):
            return
        await state.clear()
        total = len(ctx.db.get_all_active_user_ids())
        ch_id = ctx.db.get_setting("order_log_channel_id", "@ARPONTOLS")
        await safe_edit_or_send(
            cb,
            f"📢 <b>Notice &amp; Messaging Hub</b>\n\n"
            f"• Active Bot Users: <b>{total}</b>\n"
            f"• Linked Channel: <code>{esc(ch_id)}</code>\n\n"
            f"Choose where you want to send your message or notice:",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📣 Notice: Bot Users + Channel", callback_data="adm:bc_mode:both")],
                [InlineKeyboardButton(text="👥 Broadcast: All Bot Users Only", callback_data="adm:bc_mode:users")],
                [InlineKeyboardButton(text="👤 Message Single User (ID / @username)", callback_data="adm:dm_start")],
                [InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")],
            ]),
        )
        await cb.answer()

    @router.callback_query(F.data.startswith("adm:bc_mode:"))
    async def cb_adm_bc_mode(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "broadcast"):
            return
        mode = cb.data.split(":")[2]
        await state.set_state(AdminStates.waiting_broadcast_content)
        await state.update_data(bc_mode=mode)
        label = "All Bot Users + Official Channel" if mode == "both" else "All Active Bot Users"
        await safe_edit_or_send(
            cb,
            f"📢 <b>Send Notice ({label})</b>\n\n"
            f"Send your message (text or photo with caption, HTML supported):",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:broadcast")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_broadcast_content)
    async def msg_adm_broadcast(message: Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        mode = data.get("bc_mode", "both")
        await state.clear()
        uids = ctx.db.get_all_active_user_ids()
        status_msg = await message.answer(f"📢 Sending notice to {len(uids)} user(s)...")
        sent, failed = 0, 0
        ch_status = "Skipped"
        photo_id = message.photo[-1].file_id if message.photo else ""
        body = message.caption if photo_id else (message.text or "")

        if mode == "both":
            ch_id = ctx.db.get_setting("order_log_channel_id", "@ARPONTOLS").strip()
            if ch_id:
                try:
                    if photo_id:
                        await bot.send_photo(ch_id, photo_id, caption=body, parse_mode=ParseMode.HTML)
                    else:
                        await bot.send_message(ch_id, body, parse_mode=ParseMode.HTML)
                    ch_status = f"✅ Posted to {esc(ch_id)}"
                except Exception as exc:
                    ch_status = f"❌ Channel error: {esc(str(exc)[:80])}"

        for uid in uids:
            try:
                if photo_id:
                    await bot.send_photo(uid, photo_id, caption=body, parse_mode=ParseMode.HTML)
                else:
                    await bot.send_message(uid, body, parse_mode=ParseMode.HTML)
                sent += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)

        await status_msg.edit_text(
            f"✅ <b>Notice Delivery Complete</b>\n"
            f"• Bot Users Delivered: <b>{sent}</b>\n"
            f"• Failed/Blocked: <b>{failed}</b>\n"
            f"• Channel: {ch_status}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Admin Panel", callback_data="adm:home")]]),
        )

    @router.callback_query(F.data == "adm:dm_start")
    async def cb_adm_dm_start(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "broadcast"):
            return
        await state.set_state(AdminStates.waiting_dm_target)
        await safe_edit_or_send(
            cb,
            "👤 <b>Message Single User</b>\n\nSend the user's <b>Telegram ID</b> or <b>@username</b>:",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:broadcast")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_dm_target)
    async def msg_adm_dm_target(message: Message, state: FSMContext):
        raw = (message.text or "").strip()
        u = ctx.db.find_user(raw)
        if u:
            target_uid = int(u["user_id"])
            label = f"{u['full_name']} (@{u['username'] or 'N/A'} | ID: {target_uid})"
        elif raw.isdigit():
            target_uid = int(raw)
            label = f"ID: {target_uid}"
        else:
            await message.answer("⚠️ User not found in bot database. Enter a numeric Telegram User ID or a registered @username.")
            return
        await state.set_state(AdminStates.waiting_dm_content)
        await state.update_data(dm_uid=target_uid, dm_label=label)
        await message.answer(
            f"💬 <b>Recipient:</b> {esc(label)}\n\nNow send the message (text or photo with caption) to deliver directly from the bot:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:broadcast")]]),
        )

    @router.callback_query(F.data.startswith("adm:udm:"))
    async def cb_adm_udm(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "users"):
            return
        uid = int(cb.data.split(":")[2])
        u = ctx.db.get_user(uid)
        label = f"{u['full_name']} (ID: {uid})" if u else f"ID: {uid}"
        await state.set_state(AdminStates.waiting_dm_content)
        await state.update_data(dm_uid=uid, dm_label=label)
        await safe_edit_or_send(
            cb,
            f"💬 <b>Direct Message to {esc(label)}</b>\n\nSend the message (text or photo with caption) to deliver from the bot:",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:home")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_dm_content)
    async def msg_adm_dm_content(message: Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        target_uid = int(data["dm_uid"])
        label = data.get("dm_label", str(target_uid))
        await state.clear()
        photo_id = message.photo[-1].file_id if message.photo else ""
        body = message.caption if photo_id else (message.text or "")
        header = f"📩 <b>Message from {esc(ctx.store_name())} Admin:</b>\n\n{body}"
        try:
            if photo_id:
                await bot.send_photo(target_uid, photo_id, caption=header, parse_mode=ParseMode.HTML)
            else:
                await bot.send_message(target_uid, header, parse_mode=ParseMode.HTML)
            await message.answer(
                f"✅ Message delivered to <b>{esc(label)}</b>!",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Admin Panel", callback_data="adm:home")]]),
            )
        except Exception as exc:
            await message.answer(f"❌ Could not deliver message: {esc(exc)}", parse_mode=ParseMode.HTML)

    # ==========================================================================
    # 11.9 PAYMENT GATEWAYS (LIVE API KEY & WALLET CONFIGURATION)
    # ==========================================================================
    @router.callback_query(F.data == "adm:payments")
    async def cb_adm_payments(cb: CallbackQuery):
        if not await require_scope(cb, "payments"):
            return
        gws = ctx.db.list_gateways(only_enabled=False)
        rows = []
        for g in gws:
            st = "🟢 ON" if g["is_enabled"] else "🔴 OFF"
            rows.append([
                InlineKeyboardButton(text=f"{st} | {g['display_name']}", callback_data=f"adm:gw_view:{g['code']}")
            ])
        bdt_rate = ctx.db.get_setting_float("bdt_per_usd", 125.0)
        rows.append([InlineKeyboardButton(text=f"🇧🇩 Set USD→BDT Rate (৳{bdt_rate:.0f})", callback_data="adm:setk:bdt_per_usd")])
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])
        await safe_edit_or_send(
            cb,
            "💳 <b>Payment Gateways Manager</b>\n\nTap any gateway to enable/disable or configure its API Keys / Wallet / Number:",
            InlineKeyboardMarkup(inline_keyboard=rows),
        )
        await cb.answer()

    @router.callback_query(F.data.startswith("adm:gw_view:"))
    async def cb_adm_gw_view(cb: CallbackQuery):
        if not await require_scope(cb, "payments"):
            return
        code = cb.data.split(":")[2]
        g = ctx.db.get_gateway(code)
        if not g:
            return
        gcfg = json.loads(g["config_json"] or "{}")
        masked_lines = []
        for k, v in gcfg.items():
            s = str(v)
            shown = (s[:6] + "..." + s[-4:]) if len(s) > 14 else (s or "<i>not set</i>")
            masked_lines.append(f"• <code>{esc(k)}</code>: {shown}")
        text = (
            f"💳 <b>{esc(g['display_name'])}</b>\n"
            f"• Status: <b>{'🟢 ENABLED' if g['is_enabled'] else '🔴 DISABLED'}</b>\n"
            f"• Min Deposit: <b>${float(g['min_amount']):.2f} USD</b>  •  Fee: <b>{float(g['fee_pct']):.2f}%</b>\n\n"
            f"<b>Current Numbers / Keys / Config:</b>\n" + "\n".join(masked_lines)
        )
        rows = [
            [InlineKeyboardButton(text="🔄 Toggle ON / OFF", callback_data=f"adm:gw_toggle:{code}")],
            [InlineKeyboardButton(text="🔑 Edit Number / ID / Wallet / API Key", callback_data=f"adm:gw_edit:{code}")],
            [
                InlineKeyboardButton(text="💵 Edit Min Deposit ($)", callback_data=f"adm:gw_meta:{code}:min"),
                InlineKeyboardButton(text="📊 Edit Fee (%)", callback_data=f"adm:gw_meta:{code}:fee"),
            ],
            [InlineKeyboardButton(text="✏️ Rename Gateway Title", callback_data=f"adm:gw_meta:{code}:name")],
            [InlineKeyboardButton(text="« Back to Gateways", callback_data="adm:payments")],
        ]
        await safe_edit_or_send(cb, text, InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data.startswith("adm:gw_toggle:"))
    async def cb_adm_gw_toggle(cb: CallbackQuery):
        if not await require_scope(cb, "payments"):
            return
        code = cb.data.split(":")[2]
        ctx.db.toggle_gateway(code)
        await cb_adm_gw_view(cb)

    @router.callback_query(F.data.startswith("adm:gw_edit:"))
    async def cb_adm_gw_edit(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "payments"):
            return
        code = cb.data.split(":")[2]
        g = ctx.db.get_gateway(code)
        gcfg = json.loads(g["config_json"] or "{}")
        keys_help = "\n".join(f"<code>{k}=YOUR_VALUE</code>" for k in gcfg.keys())
        await state.set_state(AdminStates.waiting_gateway_config)
        await state.update_data(gateway_code=code)
        quick_hint = {
            "bkash_manual": "Just send your bKash number directly (e.g. <code>017XXXXXXXX</code>)",
            "nagad_manual": "Just send your Nagad number directly (e.g. <code>018XXXXXXXX</code>)",
            "binance_pay": "Just send your Binance Pay ID directly (e.g. <code>123456789</code>) OR <code>api_key=...</code> + <code>secret_key=...</code>",
            "usdt_bep20": "Just send your BEP-20 Wallet Address directly (e.g. <code>0x...</code>)",
            "cryptobot": "Just send your CryptoBot API Token directly",
            "nowpayments": "Just send your NOWPayments API Key directly",
            "wallet_manual": "Send your full custom payment instructions (bKash/Nagad/Bank/UAE etc.)",
        }.get(code, "Send key=value lines")
        await safe_edit_or_send(
            cb,
            f"🔑 <b>Configure {esc(g['display_name'])}</b>\n\n"
            f"👉 {quick_hint}\n\n"
            f"<i>Or send exact key=value format:</i>\n{keys_help}",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data=f"adm:gw_view:{code}")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_gateway_config)
    async def msg_adm_gw_cfg(message: Message, state: FSMContext):
        code = (await state.get_data())["gateway_code"]
        raw = (message.text or "").strip()
        updates = {}
        for ln in raw.splitlines():
            if "=" in ln:
                k, v = ln.split("=", 1)
                updates[k.strip()] = v.strip()
        # Smart shortcut: if admin sends just a plain number/ID/address without key=, map to primary field!
        if not updates and raw:
            primary_map = {
                "bkash_manual": "number",
                "nagad_manual": "number",
                "binance_pay": "pay_id",
                "usdt_bep20": "wallet_address",
                "cryptobot": "api_token",
                "nowpayments": "api_key",
                "wallet_manual": "instructions",
            }
            if code in primary_map:
                updates[primary_map[code]] = raw
        if not updates:
            await message.answer("⚠️ Send a value or <code>key=value</code> line.", parse_mode=ParseMode.HTML)
            return
        ctx.db.update_gateway_config(code, updates)
        await state.clear()
        await message.answer(
            "✅ Gateway configuration updated!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« View Gateway", callback_data=f"adm:gw_view:{code}")]]),
        )

    @router.callback_query(F.data.startswith("adm:gw_meta:"))
    async def cb_adm_gw_meta(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "payments"):
            return
        _, _, code, field = cb.data.split(":")
        await state.set_state(AdminStates.waiting_gateway_meta)
        await state.update_data(gateway_code=code, meta_field=field)
        prompts = {
            "min": "💵 Send new <b>Minimum Deposit (USD)</b> (e.g. <code>0.50</code> or <code>1.00</code>):",
            "fee": "📊 Send new <b>Gateway Fee (%)</b> (e.g. <code>0</code> or <code>1.85</code>):",
            "name": "✏️ Send new <b>Display Title</b> for this gateway (e.g. <code>📱 bKash Personal</code>):",
        }
        await safe_edit_or_send(
            cb,
            prompts.get(field, "Send new value:"),
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data=f"adm:gw_view:{code}")]]),
        )
        await safe_cb_answer(cb)

    @router.message(AdminStates.waiting_gateway_meta)
    async def msg_adm_gw_meta(message: Message, state: FSMContext):
        data = await state.get_data()
        code, field = data["gateway_code"], data["meta_field"]
        raw = (message.text or "").strip()
        if field == "name":
            if not raw:
                await message.answer("⚠️ Title cannot be empty.")
                return
            ctx.db.update_gateway_meta(code, display_name=raw)
        elif field in ("min", "fee"):
            try:
                val = float(raw.replace("$", "").replace("%", "").replace(",", ""))
            except ValueError:
                await message.answer("⚠️ Please enter a valid number.")
                return
            if field == "min":
                ctx.db.update_gateway_meta(code, min_amount=val)
            else:
                ctx.db.update_gateway_meta(code, fee_pct=val)
        await state.clear()
        await message.answer(
            "✅ Gateway setting updated permanently in database!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« View Gateway", callback_data=f"adm:gw_view:{code}")]]),
        )

    # ==========================================================================
    # 11.10 STORE SETTINGS & SUPPORT SETTINGS
    # ==========================================================================
    @router.callback_query(F.data.in_({"adm:settings", "adm:support"}))
    async def cb_adm_settings(cb: CallbackQuery):
        scope = "support" if cb.data == "adm:support" else "settings"
        if not await require_scope(cb, scope):
            return
        fields = [
            ("store_name", "🏪 Store Name"),
            ("owner_username", "👑 Owner @username"),
            ("support_username", "🚨 Support @username"),
            ("channel_url", "📺 Channel URL"),
            ("min_deposit_usd", "💵 Min Deposit ($)"),
            ("bdt_per_usd", "🇧🇩 BDT per USD Rate"),
            ("restock_channel_id", "👁️ Restock Alert Channel ID"),
            ("order_log_channel_id", "📢 Auto Order Log Channel ID"),
            ("help_text", "📖 Help Guide Text"),
        ]
        rows = [[InlineKeyboardButton(text=lbl, callback_data=f"adm:setk:{k}")] for k, lbl in fields]
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])
        await safe_edit_or_send(cb, "⚙️ <b>Store &amp; Support Settings</b>\nTap a setting to update it live:", InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data.startswith("adm:setk:"))
    async def cb_adm_setk(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "settings"):
            return
        key = cb.data.split(":")[2]
        cur = ctx.db.get_setting(key, "")
        await state.set_state(AdminStates.waiting_setting_value)
        await state.update_data(setting_key=key)
        await safe_edit_or_send(
            cb,
            f"✏️ <b>Update Setting:</b> <code>{esc(key)}</code>\nCurrent: <code>{esc(cur[:200])}</code>\n\nSend the new value:",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:settings")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_setting_value)
    async def msg_adm_setk(message: Message, state: FSMContext):
        key = (await state.get_data())["setting_key"]
        ctx.db.set_setting(key, (message.text or "").strip())
        await state.clear()
        await message.answer(
            f"✅ Setting <code>{esc(key)}</code> updated!",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Settings", callback_data="adm:settings")]]),
        )

    # ==========================================================================
    # 11.11 ANALYTICS & CSV EXPORT
    # ==========================================================================
    @router.callback_query(F.data == "adm:analytics")
    async def cb_adm_analytics(cb: CallbackQuery):
        if not await require_scope(cb, "analytics"):
            return
        s = ctx.db.get_dashboard_stats()
        sym = ctx.currency()
        roi = (s["total_profit"] / s["total_revenue"] * 100.0) if s["total_revenue"] > 0 else 0.0
        top_lines = "\n".join(
            f"  {i+1}. {esc(tp['product_name'][:28])} — {tp['qty']} sold | {fmt_money(tp['rev'], sym)} (+{fmt_money(tp['prof'], sym)})"
            for i, tp in enumerate(s["top_products"])
        ) or "  <i>No completed sales yet.</i>"

        text = (
            f"📊 <b>Analytics &amp; Profit Margin Report</b>\n\n"
            f"📅 <b>Today:</b> {s['today_orders']} orders | Rev {fmt_money(s['today_revenue'], sym)} | Profit <b>{fmt_money(s['today_profit'], sym)}</b>\n"
            f"🗓️ <b>Last 7 Days:</b> Rev {fmt_money(s['week_revenue'], sym)} | Profit <b>{fmt_money(s['week_profit'], sym)}</b>\n"
            f"🏆 <b>All-Time:</b> {s['total_orders']} orders | Rev {fmt_money(s['total_revenue'], sym)} | Net Profit <b>{fmt_money(s['total_profit'], sym)}</b> ({roi:.1f}%)\n\n"
            f"🔥 <b>Top Selling Products:</b>\n{top_lines}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Export Orders CSV", callback_data="adm:export_csv")],
            [InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")],
        ])
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    @router.callback_query(F.data == "adm:export_csv")
    async def cb_adm_export_csv(cb: CallbackQuery):
        if not await require_scope(cb, "analytics"):
            return
        csv_bytes = ctx.db.export_orders_csv()
        doc = BufferedInputFile(csv_bytes, filename=f"arpontols_orders_{int(time.time())}.csv")
        await cb.message.answer_document(doc, caption="📊 Full Orders & Profit Export (CSV)")
        await cb.answer("✅ CSV sent!")

    # ==========================================================================
    # 11.12 TEAM & STAFF ROLES (RBAC SCOPES)
    # ==========================================================================
    @router.callback_query(F.data == "adm:staff")
    async def cb_adm_staff(cb: CallbackQuery):
        if not await require_scope(cb, "staff"):
            return
        staff_list = ctx.db.list_staff()
        rows = [[InlineKeyboardButton(text="➕ Add Staff Member", callback_data="adm:staff_add")]]
        for st in staff_list:
            scopes = json.loads(st["scopes_json"] or "[]")
            rows.append([
                InlineKeyboardButton(
                    text=f"🛡️ {st['user_id']} ({st['role_name']} • {len(scopes)} scopes)",
                    callback_data=f"adm:staff_edit:{st['user_id']}",
                )
            ])
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])
        await safe_edit_or_send(cb, f"🛡️ <b>Team &amp; Staff Roles ({len(staff_list)})</b>\nTap a member to toggle RBAC scopes:", InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data == "adm:staff_add")
    async def cb_adm_staff_add(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "staff"):
            return
        await state.set_state(AdminStates.waiting_staff_add)
        await safe_edit_or_send(
            cb,
            "➕ Send: <code>USER_ID | Role Name</code>\nExample: <code>123456789 | Support Agent</code>",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:staff")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_staff_add)
    async def msg_adm_staff_add(message: Message, state: FSMContext):
        parts = [x.strip() for x in (message.text or "").split("|")]
        if not parts or not parts[0].isdigit():
            await message.answer("⚠️ Start with a numeric Telegram User ID.")
            return
        uid = int(parts[0])
        role = parts[1] if len(parts) > 1 and parts[1] else "Staff"
        ctx.db.upsert_staff(uid, role, ["orders", "tickets"], message.from_user.id)
        await state.clear()
        await message.answer(
            f"✅ Staff <code>{uid}</code> ({esc(role)}) added with default scopes.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⚙️ Edit Scopes", callback_data=f"adm:staff_edit:{uid}")]]),
        )

    @router.callback_query(F.data.startswith("adm:staff_edit:"))
    async def cb_adm_staff_edit(cb: CallbackQuery):
        if not await require_scope(cb, "staff"):
            return
        uid = int(cb.data.split(":")[2])
        st = ctx.db.get_staff(uid)
        if not st:
            return
        active_scopes = set(json.loads(st["scopes_json"] or "[]"))
        rows = []
        pair: List[InlineKeyboardButton] = []
        for skey, slabel in ALL_RBAC_SCOPES:
            mark = "✅" if skey in active_scopes else "⬜"
            pair.append(InlineKeyboardButton(text=f"{mark} {skey}", callback_data=f"adm:stog:{uid}:{skey}"))
            if len(pair) == 2:
                rows.append(pair)
                pair = []
        if pair:
            rows.append(pair)
        rows.append([InlineKeyboardButton(text="🗑️ Remove Staff Member", callback_data=f"adm:sdel:{uid}")])
        rows.append([InlineKeyboardButton(text="« Back to Team", callback_data="adm:staff")])
        await safe_edit_or_send(cb, f"🛡️ <b>RBAC Scopes for {uid} ({esc(st['role_name'])})</b>", InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data.startswith("adm:stog:"))
    async def cb_adm_stog(cb: CallbackQuery):
        if not await require_scope(cb, "staff"):
            return
        _, _, uid_s, skey = cb.data.split(":")
        uid = int(uid_s)
        st = ctx.db.get_staff(uid)
        if not st:
            return
        scopes = set(json.loads(st["scopes_json"] or "[]"))
        if skey in scopes:
            scopes.remove(skey)
        else:
            scopes.add(skey)
        ctx.db.upsert_staff(uid, st["role_name"], list(scopes), cb.from_user.id)
        await cb_adm_staff_edit(cb)

    @router.callback_query(F.data.startswith("adm:sdel:"))
    async def cb_adm_sdel(cb: CallbackQuery):
        if not await require_scope(cb, "staff"):
            return
        uid = int(cb.data.split(":")[2])
        ctx.db.delete_staff(uid)
        await cb.answer("Removed staff member.", show_alert=True)
        await cb_adm_staff(cb)

    # ==========================================================================
    # 11.13 FORCE-SUB HUB & STOCK WATCHER
    # ==========================================================================
    @router.callback_query(F.data == "adm:forcesub")
    async def cb_adm_forcesub(cb: CallbackQuery):
        if not await require_scope(cb, "forcesub"):
            return
        enabled = ctx.db.get_setting_bool("forcesub_enabled", False)
        chans = ctx.db.list_force_sub(only_active=False)
        rows = [
            [InlineKeyboardButton(text=f"Status: {'🟢 ON' if enabled else '🔴 OFF'} (Tap to Toggle)", callback_data="adm:fsub_toggle")],
            [
                InlineKeyboardButton(text="📢 Add Channel", callback_data="adm:fsub_add:channel"),
                InlineKeyboardButton(text="👥 Add Group", callback_data="adm:fsub_add:group"),
            ],
        ]
        for c in chans:
            rows.append([InlineKeyboardButton(text=f"🗑️ {c['title']} ({c['chat_id']})", callback_data=f"adm:fsub_del:{c['id']}")])
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])
        await safe_edit_or_send(cb, "📣 <b>Force-Sub Hub (Channels &amp; Groups)</b>\nAdd any number of Channels or Groups users must join before using the bot (make bot Admin in each):", InlineKeyboardMarkup(inline_keyboard=rows))
        await cb.answer()

    @router.callback_query(F.data == "adm:fsub_toggle")
    async def cb_fsub_toggle(cb: CallbackQuery):
        if not await require_scope(cb, "forcesub"):
            return
        cur = ctx.db.get_setting_bool("forcesub_enabled", False)
        ctx.db.set_setting("forcesub_enabled", "0" if cur else "1")
        await cb_adm_forcesub(cb)

    @router.callback_query(F.data.startswith("adm:fsub_add"))
    async def cb_fsub_add(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "forcesub"):
            return
        kind = "Group" if cb.data.endswith(":group") else "Channel"
        await state.set_state(AdminStates.waiting_forcesub_add)
        example = (
            "<code>-1001234567890 | Support Group | https://t.me/+InviteLink</code>"
            if kind == "Group"
            else "<code>@ARPONTOLS | Official Channel | https://t.me/ARPONTOLS</code>"
        )
        await safe_edit_or_send(
            cb,
            f"➕ <b>Add Required {kind}</b>\n\n"
            f"Make sure this bot is an <b>Admin</b> in the {kind.lower()}, then send:\n"
            f"<code>CHAT_ID_OR_@USERNAME | Title | Invite Link</code>\n\n"
            f"Example:\n{example}",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:forcesub")]]),
        )
        await safe_cb_answer(cb)

    @router.message(AdminStates.waiting_forcesub_add)
    async def msg_fsub_add(message: Message, state: FSMContext):
        parts = [x.strip() for x in (message.text or "").split("|")]
        if len(parts) < 3:
            await message.answer("⚠️ Format: <code>@channel | Title | https://t.me/channel</code>", parse_mode=ParseMode.HTML)
            return
        ctx.db.add_force_sub(parts[0], parts[1], parts[2])
        await state.clear()
        await message.answer(
            "✅ Force-Sub channel added!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Force-Sub Hub", callback_data="adm:forcesub")]]),
        )

    @router.callback_query(F.data.startswith("adm:fsub_del:"))
    async def cb_fsub_del(cb: CallbackQuery):
        if not await require_scope(cb, "forcesub"):
            return
        ctx.db.remove_force_sub(int(cb.data.split(":")[2]))
        await cb_adm_forcesub(cb)

    @router.callback_query(F.data == "adm:watcher")
    async def cb_adm_watcher(cb: CallbackQuery):
        if not await require_scope(cb, "stock_watcher"):
            return
        s = ctx.db.get_dashboard_stats()
        ch = ctx.db.get_setting("restock_channel_id", "") or "Not Set"
        text = (
            f"👁️ <b>Stock Watcher &amp; Restock Alerts</b>\n\n"
            f"• Active User Watch Subscriptions: <b>{s['watcher_count']}</b>\n"
            f"• Restock Broadcast Channel: <code>{esc(ch)}</code>\n\n"
            f"When an out-of-stock item is restocked (via supplier sync or custom stock), "
            f"the bot automatically sends an instant restock card with a direct Buy button!"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Scan & Sync Stock Now", callback_data="adm:sync_all")],
            [InlineKeyboardButton(text="⚙️ Set Restock Channel ID", callback_data="adm:setk:restock_channel_id")],
            [InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")],
        ])
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    # ==========================================================================
    # 11.14 EXTERNAL SUPPLIERS (UPSTREAM WHOLESALE BOT API MANAGER)
    # ==========================================================================
    @router.callback_query(F.data == "adm:suppliers")
    async def cb_adm_suppliers(cb: CallbackQuery):
        if not await require_scope(cb, "suppliers"):
            return
        sups = ctx.db.list_suppliers(only_active=False)
        rows = [[InlineKeyboardButton(text="➕ Add Wholesale Supplier API", callback_data="adm:sup_add")]]
        for s in sups:
            star = "⭐" if s["is_primary"] else ("🟢" if s["is_active"] else "🔴")
            rows.append([
                InlineKeyboardButton(
                    text=f"{star} {s['name']} | Bal: ${s['cached_balance']:.2f}",
                    callback_data=f"adm:sup_view:{s['id']}",
                )
            ])
        rows.append([InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")])
        await safe_edit_or_send(
            cb,
            "🔌 <b>External Wholesale Suppliers (Auto-Purchase APIs)</b>\n\n"
            "Connect your upstream wholesale Telegram bot / REST API here so customer orders auto-purchase with your profit markup!",
            InlineKeyboardMarkup(inline_keyboard=rows),
        )
        await cb.answer()

    @router.callback_query(F.data == "adm:sup_add")
    async def cb_adm_sup_add(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "suppliers"):
            return
        await state.set_state(AdminStates.waiting_supplier_add)
        await safe_edit_or_send(
            cb,
            "🔌 <b>Add External Wholesale Supplier API</b>\n\n"
            "Send details separated by <code>|</code>:\n"
            "<code>Name | Base URL | API Key | Auth Header (opt) | Auth Prefix (opt) | Products Path (opt) | Balance Path (opt) | Order Path (opt)</code>\n\n"
            "Minimal Example (3 fields):\n"
            "<code>DigiHub API | https://api.example.com/v1 | sk_live_123456</code>\n\n"
            "Custom Header Example:\n"
            "<code>DigiHub API | https://api.example.com/v1 | sk_live_123456 | X-API-Key |  | /products | /balance | /order</code>",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data="adm:suppliers")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_supplier_add)
    async def msg_adm_sup_add(message: Message, state: FSMContext):
        parts = [x.strip() for x in (message.text or "").split("|")]
        if len(parts) < 3:
            await message.answer("⚠️ At least 3 fields required: <code>Name | Base URL | API Key</code>", parse_mode=ParseMode.HTML)
            return
        name, base_url, api_key = parts[0], parts[1], parts[2]
        auth_h = parts[3] if len(parts) > 3 and parts[3] else "Authorization"
        raw_p = parts[4] if len(parts) > 4 else ("Bearer " if auth_h.lower() == "authorization" else "")
        auth_p = (raw_p + " ") if (raw_p and not raw_p.endswith(" ")) else raw_p
        if api_key.lower().startswith("bearer "):
            api_key = api_key[7:].strip()
        p_ep = parts[5] if len(parts) > 5 and parts[5] else "/products"
        b_ep = parts[6] if len(parts) > 6 and parts[6] else "/balance"
        o_ep = parts[7] if len(parts) > 7 and parts[7] else "/order"
        is_first = len(ctx.db.list_suppliers()) == 0
        sid = ctx.db.add_supplier(name, base_url, api_key, auth_h, auth_p, p_ep, b_ep, o_ep, is_primary=is_first)
        await state.clear()
        await message.answer(
            f"✅ Supplier <b>{esc(name)}</b> saved (ID #{sid})!\nTap below to test connection & sync products:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔌 Manage Supplier", callback_data=f"adm:sup_view:{sid}")]]),
        )

    @router.callback_query(F.data.startswith("adm:sup_view:"))
    async def cb_adm_sup_view(cb: CallbackQuery):
        if not await require_scope(cb, "suppliers"):
            return
        sid = int(cb.data.split(":")[2])
        s = ctx.db.get_supplier(sid)
        if not s:
            return
        masked_key = s["api_key"][:6] + "..." + s["api_key"][-4:] if len(s["api_key"]) > 12 else "***"
        text = (
            f"🔌 <b>Supplier #{s['id']} — {esc(s['name'])}</b>\n\n"
            f"• <b>Base URL:</b> <code>{esc(s['base_url'])}</code>\n"
            f"• <b>API Key:</b> <code>{esc(masked_key)}</code>\n"
            f"• <b>Auth Header:</b> <code>{esc(s['auth_header'])}</code> (Prefix: <code>{esc(s['auth_prefix'])}</code>)\n"
            f"• <b>Endpoints:</b> <code>{esc(s['products_endpoint'])}</code> | <code>{esc(s['balance_endpoint'])}</code> | <code>{esc(s['order_endpoint'])}</code>\n"
            f"• <b>Reseller Balance:</b> <b>${s['cached_balance']:.2f} USDT</b>\n"
            f"• <b>Last Sync:</b> {esc(s['last_sync_at'] or 'Never')}\n"
            f"• <b>Primary:</b> {'Yes ⭐' if s['is_primary'] else 'No'} | <b>Active:</b> {'🟢' if s['is_active'] else '🔴'}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔄 Sync Catalog Now", callback_data=f"adm:sup_sync:{sid}"),
                InlineKeyboardButton(text="💰 Test Live Balance", callback_data=f"adm:sup_bal:{sid}"),
            ],
            [
                InlineKeyboardButton(text="🔑 Edit API Key / URL", callback_data=f"adm:sup_key:{sid}"),
                InlineKeyboardButton(text="⭐ Set Primary", callback_data=f"adm:sup_pri:{sid}"),
            ],
            [InlineKeyboardButton(text="🔁 Toggle ON/OFF", callback_data=f"adm:sup_tog:{sid}")],
            [InlineKeyboardButton(text="🗑️ Delete Supplier", callback_data=f"adm:sup_del:{sid}")],
            [InlineKeyboardButton(text="« Back to Suppliers", callback_data="adm:suppliers")],
        ])
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    @router.callback_query(F.data.startswith("adm:sup_bal:"))
    async def cb_adm_sup_bal(cb: CallbackQuery):
        if not await require_scope(cb, "suppliers"):
            return
        await safe_cb_answer(cb, "💰 Checking live supplier balance...")
        sid = int(cb.data.split(":")[2])
        s = ctx.db.get_supplier(sid)
        if not s:
            return
        ok, bal, err = await ctx.supplier_client.fetch_balance(s)
        if ok:
            ctx.db.update_supplier_balance(sid, bal)
            note = f"✅ <b>Live Balance:</b> ${bal:.2f} USDT"
        else:
            note = f"❌ <b>Balance Check Error:</b> {esc(err[:250])}"
        await cb.message.answer(note, parse_mode=ParseMode.HTML)
        await cb_adm_sup_view(cb)

    @router.callback_query(F.data.startswith("adm:sup_sync:"))
    async def cb_adm_sup_sync(cb: CallbackQuery, bot: Bot):
        if not await require_scope(cb, "suppliers"):
            return
        await safe_cb_answer(cb, "🔄 Syncing catalog from supplier...")
        sid = int(cb.data.split(":")[2])
        s = ctx.db.get_supplier(sid)
        if not s:
            return
        ok, cnt, restocked, err = await ctx.supplier_client.sync_catalog(ctx.db, s)
        if ok:
            if restocked:
                asyncio.create_task(dispatch_restock_batch(bot, ctx, restocked))
            await cb.message.answer(
                f"✅ <b>Catalog Synced!</b>\n• Supplier: <b>{esc(s['name'])}</b>\n• Total Products: <b>{cnt}</b>\n• Newly Restocked: <b>{len(restocked)}</b>",
                parse_mode=ParseMode.HTML,
            )
        else:
            await cb.message.answer(f"❌ <b>Sync Failed:</b> {esc(err[:300])}", parse_mode=ParseMode.HTML)
        await cb_adm_sup_view(cb)

    @router.callback_query(F.data.startswith("adm:sup_key:"))
    async def cb_adm_sup_key(cb: CallbackQuery, state: FSMContext):
        if not await require_scope(cb, "suppliers"):
            return
        sid = int(cb.data.split(":")[2])
        await state.set_state(AdminStates.waiting_supplier_key_edit)
        await state.update_data(supplier_id=sid)
        await safe_edit_or_send(
            cb,
            "🔑 <b>Update Supplier API Key</b>\n\n"
            "Send your new API Key (e.g. <code>csa_xxxxxxxx</code>)\n"
            "Or send <code>Base URL | API Key</code> to update both:",
            InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="« Cancel", callback_data=f"adm:sup_view:{sid}")]]),
        )
        await cb.answer()

    @router.message(AdminStates.waiting_supplier_key_edit)
    async def msg_adm_sup_key_edit(message: Message, state: FSMContext):
        sid = int((await state.get_data())["supplier_id"])
        raw = (message.text or "").strip()
        if "|" in raw:
            b_url, key = [x.strip() for x in raw.split("|", 1)]
            ctx.db.update_supplier_key(sid, key, base_url=b_url)
        else:
            ctx.db.update_supplier_key(sid, raw)
        await state.clear()
        await message.answer(
            "✅ Supplier API Key updated with <code>Authorization: Bearer &lt;key&gt;</code>! Now tap Sync Catalog Now.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔌 Open Supplier", callback_data=f"adm:sup_view:{sid}")]]),
        )

    @router.callback_query(F.data.startswith("adm:sup_pri:"))
    async def cb_adm_sup_pri(cb: CallbackQuery):
        if not await require_scope(cb, "suppliers"):
            return
        ctx.db.set_primary_supplier(int(cb.data.split(":")[2]))
        await cb_adm_sup_view(cb)

    @router.callback_query(F.data.startswith("adm:sup_tog:"))
    async def cb_adm_sup_tog(cb: CallbackQuery):
        if not await require_scope(cb, "suppliers"):
            return
        ctx.db.toggle_supplier(int(cb.data.split(":")[2]))
        await cb_adm_sup_view(cb)

    @router.callback_query(F.data.startswith("adm:sup_del:"))
    async def cb_adm_sup_del(cb: CallbackQuery):
        if not await require_scope(cb, "suppliers"):
            return
        ctx.db.delete_supplier(int(cb.data.split(":")[2]))
        await cb.answer("Supplier deleted.", show_alert=True)
        await cb_adm_suppliers(cb)

    # ==========================================================================
    # 11.15 REFERRALS, MAINTENANCE MODE & REFILL WHOLESALE CREDIT
    # ==========================================================================
    @router.callback_query(F.data == "adm:referrals")
    async def cb_adm_referrals(cb: CallbackQuery):
        if not await require_scope(cb, "referrals"):
            return
        pct = ctx.db.get_setting_float("referral_commission_pct", 5.0)
        bonus = ctx.db.get_setting_float("referral_signup_bonus_usd", 0.05)
        with ctx.db.connect() as conn:
            top = conn.execute(
                "SELECT user_id, username, referral_count, referral_balance FROM users WHERE referral_count > 0 ORDER BY referral_count DESC LIMIT 10"
            ).fetchall()
        lines = [
            f"👥 <b>Referral Program Settings</b>\n"
            f"• 💵 Per-Referral Signup Bonus: <b>${bonus:.2f} USD</b> (direct to balance)\n"
            f"• 📈 Deposit/Order Commission: <b>{pct:.1f}%</b>\n\n"
            f"<b>Top Referrers:</b>"
        ]
        for i, r in enumerate(top, 1):
            lines.append(f"  {i}. <code>{r['user_id']}</code> (@{esc(r['username'] or 'N/A')}) — {r['referral_count']} invites")
        if not top:
            lines.append("  <i>No referrals recorded yet.</i>")
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"💵 Set Per-Referral Bonus (${bonus:.2f})", callback_data="adm:setk:referral_signup_bonus_usd")],
            [InlineKeyboardButton(text=f"📈 Set Commission % ({pct:.1f}%)", callback_data="adm:setk:referral_commission_pct")],
            [InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")],
        ])
        await safe_edit_or_send(cb, "\n".join(lines), kb)
        await cb.answer()

    @router.callback_query(F.data == "adm:maintenance")
    async def cb_adm_maintenance(cb: CallbackQuery):
        if not await require_scope(cb, "maintenance"):
            return
        cur = ctx.db.get_setting_bool("maintenance_mode", False)
        ctx.db.set_setting("maintenance_mode", "0" if cur else "1")
        ctx.db.log_audit(cb.from_user.id, "toggle_maintenance", f"paused={not cur}")
        await cb.answer("⏸️ Store PAUSED." if not cur else "▶️ Store RESUMED!", show_alert=True)
        text, kb = build_admin_dashboard(ctx)
        await safe_edit_or_send(cb, text, kb)

    @router.callback_query(F.data == "adm:refill_credit")
    async def cb_adm_refill_credit(cb: CallbackQuery):
        if not await require_scope(cb, "suppliers"):
            return
        sup = ctx.db.get_primary_supplier()
        if not sup:
            await cb.answer("No primary wholesale supplier configured yet.", show_alert=True)
            return
        ok, bal, err = await ctx.supplier_client.fetch_balance(sup)
        if ok:
            ctx.db.update_supplier_balance(int(sup["id"]), bal)
            status_line = f"💰 Live Wholesale Balance: <b>${bal:.2f} USDT</b>"
        else:
            status_line = f"💰 Cached Balance: <b>${sup['cached_balance']:.2f} USDT</b>\n⚠️ Live check note: {esc(err)}"
        text = (
            f"⚡ <b>Refill Wholesale Credit</b>\n\n"
            f"• Supplier: <b>{esc(sup['name'])}</b>\n"
            f"• Endpoint: <code>{esc(sup['base_url'])}</code>\n"
            f"{status_line}\n\n"
            f"Keep sufficient credit in your upstream wholesale bot/account so customer orders auto-fulfill without interruption."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh Balance", callback_data="adm:refill_credit")],
            [InlineKeyboardButton(text="🔌 Manage Suppliers", callback_data="adm:suppliers")],
            [InlineKeyboardButton(text="« Back to Admin Panel", callback_data="adm:home")],
        ])
        await safe_edit_or_send(cb, text, kb)
        await safe_cb_answer(cb)

    return router


# ==============================================================================
# 12. STOCK WATCHER NOTIFIER & BACKGROUND WORKERS
# ==============================================================================

async def notify_order_channel(
    bot: Bot, ctx: BotAppContext, order_res: Dict[str, Any], product_id: int, buyer_label: str
) -> None:
    """Auto-post every completed order to the configured Telegram channel."""
    ch_id = ctx.db.get_setting("order_log_channel_id", "").strip()
    if not ch_id:
        return
    p = ctx.db.get_product(product_id)
    stock_left = int(p["stock"]) if p else 0
    ico = brand_icon(p["brand"]) if p else "🛒"
    sym = ctx.currency()
    # Privacy-mask buyer label (e.g. Ar***rt)
    clean = buyer_label.lstrip("@")
    masked = (clean[:2] + "***" + clean[-2:]) if len(clean) > 4 else (clean[:1] + "***")
    card_text = (
        f"🛒 <b>New Order Completed — {esc(ctx.store_name())}</b> ⚡\n\n"
        f"📦 <b>Product:</b> {ico} <b>{esc(order_res['product_name'])}</b>\n"
        f"🔢 <b>Quantity:</b> {order_res['quantity']} unit(s)\n"
        f"💵 <b>Amount Paid:</b> <code>{fmt_money(order_res['total_paid'], sym)}</code> USDT\n"
        f"👤 <b>Buyer:</b> <code>{esc(masked)}</code>\n"
        f"🧾 <b>Order ID:</b> <code>{esc(order_res['order_code'])}</code>\n"
        f"📊 <b>Remaining Stock:</b> {'🟢' if stock_left > 0 else '🔴'} <b>{stock_left} units</b>\n\n"
        f"👉 <i>Order now directly in our bot with instant delivery!</i>"
    )
    deep_url = f"https://t.me/{ctx.bot_username}?start=buy_{product_id}" if ctx.bot_username else ""
    btn_label = f"⚡ Buy {ico} {order_res['product_name'][:26]}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=btn_label, url=deep_url) if deep_url
        else InlineKeyboardButton(text=btn_label, callback_data=f"shop:prod:{product_id}:1:")
    ]])
    try:
        await bot.send_message(ch_id, card_text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except Exception as exc:
        logger.warning("Could not post live order alert to %s: %s", ch_id, exc)


async def notify_restock(bot: Bot, ctx: BotAppContext, product_id: int, added_units: int = 0) -> None:
    """Send DigiHub-style restock alert cards to subscribed watchers & restock channel."""
    p = ctx.db.get_product(product_id)
    if not p or int(p["stock"]) <= 0:
        return
    gm = ctx.db.get_setting_float("default_margin_pct", 35.0)
    price = calculate_sell_price(float(p["cost_price"]), gm, p["margin_override_pct"], p["fixed_price"])
    sym = ctx.currency()
    ico = brand_icon(p["brand"])
    added_line = f"➕ <b>Added:</b> +{added_units} units\n" if added_units > 0 else ""
    card_text = (
        f"🎉 {ico} <b>{esc(p['name'])}</b> is back in stock!\n\n"
        f"💵 <b>Price:</b> <code>{fmt_money(price, sym)}</code> USDT\n"
        f"{added_line}"
        f"📦 <b>Current Stock:</b> 🟢 <b>{p['stock']} units</b>\n\n"
        f"👉 <i>Order now directly in our bot before it sells out!</i>"
    )
    deep_url = f"https://t.me/{ctx.bot_username}?start=buy_{p['id']}" if ctx.bot_username else ""
    btn_label = f"⚡ Buy {ico} {p['name'][:26]}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=btn_label, url=deep_url) if deep_url
        else InlineKeyboardButton(text=btn_label, callback_data=f"shop:prod:{p['id']}:1:")
    ]])

    # 1. Notify individual subscribed users
    watchers = ctx.db.pop_stock_watchers(product_id)
    for uid in watchers:
        try:
            await bot.send_message(uid, card_text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception:
            pass

    # 2. Notify configured Restock Channel if set
    ch_id = ctx.db.get_setting("restock_channel_id", "").strip()
    if ch_id:
        try:
            await bot.send_message(ch_id, card_text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception as exc:
            logger.warning("Could not post restock alert to %s: %s", ch_id, exc)

    # 3. Also send Restock Notice to Owner in Bot
    if ctx.owner_id and ctx.owner_id not in watchers:
        try:
            await bot.send_message(ctx.owner_id, card_text, reply_markup=kb, parse_mode=ParseMode.HTML)
        except Exception:
            pass


async def dispatch_restock_batch(
    bot: Bot, ctx: BotAppContext, restocked: List[Tuple[int, int]], max_per_cycle: int = 10
) -> None:
    """Rate-limited restock dispatcher to avoid Telegram channel flood control."""
    for pid, added_qty in restocked[:max_per_cycle]:
        await notify_restock(bot, ctx, pid, added_qty)
        await asyncio.sleep(3.5)


async def background_supplier_sync_loop(bot: Bot, ctx: BotAppContext) -> None:
    interval = max(60, int(ctx.cfg.get("AUTO_SYNC_INTERVAL_SEC", 300)))
    while True:
        await asyncio.sleep(interval)
        for s in ctx.db.list_suppliers(only_active=True):
            try:
                ok, cnt, restocked, err = await ctx.supplier_client.sync_catalog(ctx.db, s)
                if ok:
                    logger.info("Auto-synced %s: %d items (%d restocked)", s["name"], cnt, len(restocked))
                    if restocked:
                        await dispatch_restock_batch(bot, ctx, restocked)
                else:
                    logger.warning("Auto-sync failed for %s: %s", s["name"], err)
            except Exception as exc:
                logger.exception("Supplier sync loop error: %s", exc)


async def background_deposit_verifier_loop(bot: Bot, ctx: BotAppContext) -> None:
    interval = max(20, int(ctx.cfg.get("DEPOSIT_POLL_INTERVAL_SEC", 45)))
    while True:
        await asyncio.sleep(interval)
        pending = ctx.db.list_deposits(status="pending", limit=30)
        for dep in pending:
            if dep["gateway"] not in ("usdt_bep20", "cryptobot", "binance_pay"):
                continue
            try:
                paid, _ = await ctx.payment_engine.verify_deposit(dep)
                if paid:
                    await bot.send_message(
                        int(dep["user_id"]),
                        f"✅ <b>Auto-Deposit Confirmed!</b>\n\n"
                        f"• Reference: <code>{esc(dep['deposit_code'])}</code>\n"
                        f"• Credited: <b>{fmt_money(dep['amount_usd'], ctx.currency())} USDT</b>",
                        parse_mode=ParseMode.HTML,
                    )
            except Exception:
                pass


# ==============================================================================
# 13. MAIN ENTRYPOINT & BUILT-IN SELF-TEST SUITE
# ==============================================================================

async def run_bot(cfg: Dict[str, Any]) -> None:
    if not AIOGRAM_AVAILABLE:
        print("\n[ERROR] 'aiogram' is not installed. Run:\n  pip install aiogram aiohttp\n")
        sys.exit(1)

    db = DatabaseManager(DB_FILE)
    db.bootstrap_from_config(cfg)
    ctx = BotAppContext(cfg, db)

    bot = Bot(
        token=cfg["BOT_TOKEN"],
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    me = await bot.get_me()
    ctx.bot_username = me.username or ""
    logger.info("Starting %s v2.5 FINAL (@%s) | Owner ID: %s", ctx.store_name(), ctx.bot_username, ctx.owner_id)

    # Register public Telegram Bot Menu commands (NO /admin for regular users!)
    user_commands = [
        BotCommand(command="start", description="🏠 Main Menu"),
        BotCommand(command="shop", description="🛍️ Browse Products & Catalog"),
        BotCommand(command="search", description="🔍 Search Products"),
        BotCommand(command="orders", description="📦 My Orders & Receipts"),
        BotCommand(command="balance", description="💳 Top Up Balance"),
        BotCommand(command="tickets", description="🎫 Support Tickets & Issues"),
    ]
    admin_commands = user_commands + [
        BotCommand(command="admin", description="🛡️ Admin Panel (Owner Only)"),
    ]
    await bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())

    # Show /admin ONLY in Owner & authorized Staff chat menus
    priv_ids = set()
    if ctx.owner_id:
        priv_ids.add(int(ctx.owner_id))
    for st in ctx.db.list_staff():
        priv_ids.add(int(st["user_id"]))
    for pid in priv_ids:
        try:
            await bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=pid))
        except Exception:
            pass

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(build_admin_router(ctx))
    dp.include_router(build_customer_router(ctx))

    # Start optional Reseller / Website REST API Server
    web_server: Optional[WebAPIServer] = None
    if cfg.get("ENABLE_WEB_API"):
        web_server = WebAPIServer(
            db, ctx.fulfillment,
            host=str(cfg.get("WEB_API_HOST", "0.0.0.0")),
            port=int(cfg.get("WEB_API_PORT", 8080)),
        )
        await web_server.start()

    sync_task = asyncio.create_task(background_supplier_sync_loop(bot, ctx))
    dep_task = asyncio.create_task(background_deposit_verifier_loop(bot, ctx))

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        sync_task.cancel()
        dep_task.cancel()
        if web_server:
            await web_server.stop()
        await bot.session.close()


def run_self_tests() -> None:
    """Offline self-test suite verifying DB, pricing, coupons, RBAC, local stock & refunds."""
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        test_db = DatabaseManager(Path(td) / "test.db")
        test_cfg = dict(DEFAULT_CONFIG)
        test_cfg["OWNER_ID"] = 999
        test_cfg["DEFAULT_MARGIN_PCT"] = 50.0
        test_db.bootstrap_from_config(test_cfg)

        # 1. User & referral creation
        u1, created1 = test_db.upsert_user(1001, "alice", "Alice")
        u2, created2 = test_db.upsert_user(1002, "bob", "Bob", referred_by=1001)
        assert created1 and created2
        assert test_db.get_user(1001)["referral_count"] == 1

        # 2. Pricing engine
        assert calculate_sell_price(10.0, 50.0) == 15.0
        assert calculate_sell_price(10.0, 50.0, margin_override_pct=150.0) == 25.0
        assert calculate_sell_price(10.0, 50.0, fixed_price=19.99) == 19.99

        # 3. Deposit confirmation + referral commission
        dep_id, _ = test_db.create_deposit(1002, "binance_pay", 100.0)
        ok_dep, _ = test_db.confirm_deposit(dep_id, "TX999")
        assert ok_dep
        assert abs(test_db.get_user(1002)["balance"] - 100.0) < 1e-6
        assert abs(test_db.get_user(1001)["referral_balance"] - 5.0) < 1e-6

        # 4. Custom product + local stock + coupon + purchase + refund
        pid = test_db.create_custom_product("ChatGPT", "ChatGPT Plus 1M", sell_price=20.0, cost_price=10.0)
        added, _ = test_db.add_local_stock_lines(pid, ["acc1@test.com:pass1", "acc2@test.com:pass2"])
        assert added == 2
        test_db.create_coupon("SAVE10", "percent", 10.0, min_order=5.0, max_uses=5)

        fulfillment = FulfillmentService(test_db, SupplierClient())
        ok_buy, res, err = asyncio.run(
            fulfillment.execute_purchase(1002, pid, quantity=1, coupon_code="SAVE10")
        )
        assert ok_buy, err
        assert res["total_paid"] == 18.0
        assert res["delivered_data"] == "acc1@test.com:pass1"
        assert test_db.get_product(pid)["stock"] == 1

        # Verify profit calculation (18.0 paid - 10.0 cost = 8.0 profit)
        ord_row = test_db.get_order_by_id(res["order_id"])
        assert abs(ord_row["net_profit"] - 8.0) < 1e-6

        # 5. Refund order
        ok_ref, _ = test_db.refund_order(res["order_id"])
        assert ok_ref
        assert abs(test_db.get_user(1002)["balance"] - 100.0) < 1e-6

        # 6. RBAC scopes
        test_db.upsert_staff(2001, "Support", ["tickets", "orders"], 999)
        assert test_db.has_scope(999, 999, "users") is True
        assert test_db.has_scope(2001, 999, "tickets") is True
        assert test_db.has_scope(2001, 999, "users") is False

        # 7. Supplier response parsers
        assert SupplierClient.parse_balance_response({"data": {"balance": 42.75}}) == 42.75
        parsed_prods = SupplierClient.parse_products_response({
            "products": [{"id": "p1", "name": "Claude Pro 1M", "price": 10.0, "stock": 5}]
        })
        assert len(parsed_prods) == 1 and parsed_prods[0]["external_id"] == "p1"
        sup_oid, deliv = SupplierClient.parse_order_response({"order_id": "SUP-1", "credentials": ["k1", "k2"]})
        assert sup_oid == "SUP-1" and deliv == "k1\nk2"

    print("[PASS] All ARPONTOLS clainds.py self-tests passed successfully!")


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        run_self_tests()
        sys.exit(0)
    if "--setup" in sys.argv:
        cfg_now = load_config(interactive=False)
        run_interactive_setup_wizard(cfg_now)
        sys.exit(0)

    config = load_config(interactive=True)
    asyncio.run(run_bot(config))