import aiosqlite
import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", Path(__file__).parent.parent / "data"))
DB_PATH = DATA_DIR / "finance.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_name    TEXT NOT NULL,
    account_number  TEXT NOT NULL UNIQUE,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS categories (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    display_order        INTEGER NOT NULL DEFAULT 0,
    color                TEXT,
    parent_id            INTEGER REFERENCES categories(id),
    source          TEXT NOT NULL DEFAULT 'manual',
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_name_toplevel
    ON categories(name) WHERE parent_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_name_sub
    ON categories(name, parent_id) WHERE parent_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS transactions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      INTEGER NOT NULL REFERENCES accounts(id),
    date            TEXT NOT NULL,
    type            TEXT,
    description     TEXT NOT NULL,
    amount          REAL NOT NULL,
    balance         REAL,
    category_id     INTEGER REFERENCES categories(id),
    manual_category INTEGER NOT NULL DEFAULT 0,
    mcc_code        TEXT,
    merchant_domain TEXT,
    import_hash     TEXT NOT NULL,
    imported_at     TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(import_hash)
);

CREATE TABLE IF NOT EXISTS tags (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    source          TEXT NOT NULL DEFAULT 'user',
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS transaction_tags (
    transaction_id  INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    tag_id          INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (transaction_id, tag_id)
);

CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category_id);
CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_description ON transactions(description);

CREATE TABLE IF NOT EXISTS categorization_rules (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id     INTEGER NOT NULL REFERENCES categories(id),
    keyword         TEXT NOT NULL,
    match_amount    REAL,
    priority        INTEGER NOT NULL DEFAULT 100,
    case_sensitive  INTEGER NOT NULL DEFAULT 0,
    comment         TEXT,
    is_subscription     INTEGER NOT NULL DEFAULT 0,
    subscription_period TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    match_amounts   TEXT,
    keywords        TEXT,
    tags            TEXT,
    exclude_amounts TEXT,
    exclude_keywords TEXT
);

CREATE INDEX IF NOT EXISTS idx_rules_priority ON categorization_rules(priority);

CREATE TABLE IF NOT EXISTS pay_periods (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    label           TEXT NOT NULL,
    start_date      TEXT NOT NULL,
    end_date        TEXT NOT NULL,
    pay_amount      REAL,
    UNIQUE(start_date)
);

CREATE INDEX IF NOT EXISTS idx_pay_periods_dates ON pay_periods(start_date, end_date);

CREATE TABLE IF NOT EXISTS settings (
    key             TEXT PRIMARY KEY,
    value           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS merchant_overrides (
    description_key TEXT PRIMARY KEY,
    domain          TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

DEFAULT_SETTINGS = [
    ("pay_day_keyword", ""),
    ("csv_date_format", "%d %b %Y"),
    ("currency_symbol", "£"),
    ("password_hash", ""),
    ("logodev_publishable_key", ""),
    ("logodev_secret_key", ""),
    ("onboarding_complete", ""),
]


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(str(DB_PATH))
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    db = await get_db()
    try:
        await db.executescript(SCHEMA)
        # Migrations for existing databases
        try:
            await db.execute(
                "ALTER TABLE categorization_rules ADD COLUMN case_sensitive INTEGER NOT NULL DEFAULT 0"
            )
            await db.commit()
        except Exception:
            pass  # Column already exists
        try:
            await db.execute(
                "ALTER TABLE categories ADD COLUMN parent_id INTEGER REFERENCES categories(id)"
            )
            await db.commit()
        except Exception:
            pass  # Column already exists
        try:
            await db.execute(
                "ALTER TABLE categorization_rules ADD COLUMN match_amounts TEXT"
            )
            await db.commit()
        except Exception:
            pass  # Column already exists
        try:
            await db.execute(
                "ALTER TABLE categorization_rules ADD COLUMN comment TEXT"
            )
            await db.commit()
        except Exception:
            pass  # Column already exists
        try:
            await db.execute("ALTER TABLE categorization_rules ADD COLUMN is_subscription INTEGER NOT NULL DEFAULT 0")
            await db.commit()
        except Exception:
            pass  # Column already exists
        try:
            await db.execute("ALTER TABLE categorization_rules ADD COLUMN subscription_period TEXT")
            await db.commit()
        except Exception:
            pass  # Column already exists
        try:
            await db.execute(
                "ALTER TABLE categories ADD COLUMN color TEXT"
            )
            await db.commit()
        except Exception:
            pass  # Column already exists
        try:
            await db.execute(
                "ALTER TABLE categorization_rules ADD COLUMN keywords TEXT"
            )
            await db.commit()
        except Exception:
            pass  # Column already exists
        try:
            await db.execute(
                "ALTER TABLE categorization_rules ADD COLUMN tags TEXT"
            )
            await db.commit()
        except Exception:
            pass  # Column already exists
        try:
            await db.execute(
                "ALTER TABLE transactions ADD COLUMN matched_rule_id INTEGER REFERENCES categorization_rules(id)"
            )
            await db.commit()
        except Exception:
            pass  # Column already exists
        try:
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_transactions_matched_rule ON transactions(matched_rule_id)"
            )
            await db.commit()
        except Exception:
            pass
        # Migrate existing single match_amount → match_amounts JSON array
        try:
            await db.execute(
                "UPDATE categorization_rules SET match_amounts = '[' || match_amount || ']' "
                "WHERE match_amount IS NOT NULL AND match_amounts IS NULL"
            )
            await db.commit()
        except Exception:
            pass
        # Add exclude_amounts and exclude_keywords columns if missing
        try:
            await db.execute(
                "ALTER TABLE categorization_rules ADD COLUMN exclude_amounts TEXT"
            )
            await db.commit()
        except Exception:
            pass  # Column already exists
        try:
            await db.execute(
                "ALTER TABLE categorization_rules ADD COLUMN exclude_keywords TEXT"
            )
            await db.commit()
        except Exception:
            pass  # Column already exists
        # Migrate categories: replace global UNIQUE(name) with per-scope partial indexes
        try:
            old_idx = await db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='index' AND name='sqlite_autoindex_categories_1'"
            )
            if await old_idx.fetchone():
                await db.executescript("""
                    PRAGMA foreign_keys=OFF;
                    BEGIN;
                    CREATE TABLE categories_new (
                        id            INTEGER PRIMARY KEY AUTOINCREMENT,
                        name          TEXT NOT NULL,
                        display_order INTEGER NOT NULL DEFAULT 0,
                        color         TEXT,
                        parent_id     INTEGER REFERENCES categories(id),
                        created_at    TEXT NOT NULL DEFAULT (datetime('now'))
                    );
                    INSERT INTO categories_new SELECT id, name, display_order, color, parent_id, created_at FROM categories;
                    DROP TABLE categories;
                    ALTER TABLE categories_new RENAME TO categories;
                    CREATE UNIQUE INDEX idx_categories_name_toplevel ON categories(name) WHERE parent_id IS NULL;
                    CREATE UNIQUE INDEX idx_categories_name_sub ON categories(name, parent_id) WHERE parent_id IS NOT NULL;
                    COMMIT;
                    PRAGMA foreign_keys=ON;
                """)
        except Exception:
            pass
        try:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS anomaly_dismissals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id INTEGER NOT NULL UNIQUE,
                    dismissed_at TEXT DEFAULT (datetime('now'))
                )
            """)
            await db.commit()
        except Exception:
            pass
        # mcc_code on transactions
        try:
            await db.execute("ALTER TABLE transactions ADD COLUMN mcc_code TEXT")
            await db.commit()
        except Exception:
            pass
        # source on categories ('manual' | 'mcc')
        try:
            await db.execute("ALTER TABLE categories ADD COLUMN source TEXT NOT NULL DEFAULT 'manual'")
            await db.commit()
        except Exception:
            pass
        # tags table
        try:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS tags (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    name       TEXT NOT NULL UNIQUE,
                    source     TEXT NOT NULL DEFAULT 'user',
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
            """)
            await db.commit()
        except Exception:
            pass
        # transaction_tags junction
        try:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS transaction_tags (
                    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
                    tag_id         INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                    PRIMARY KEY (transaction_id, tag_id)
                )
            """)
            await db.commit()
        except Exception:
            pass
        try:
            await db.execute("ALTER TABLE transactions ADD COLUMN merchant_domain TEXT")
            await db.commit()
        except Exception:
            pass  # Column already exists
        for key, value in DEFAULT_SETTINGS:
            await db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                (key, value),
            )
        await db.commit()
    finally:
        await db.close()
