# Finch — Personal Finance Tracker

Finch is a self-hosted personal finance tracker built for households that import bank transactions via CSV. It automatically categorises spending using configurable keyword rules, tracks subscriptions, visualises trends across pay periods, and helps you understand where your money is going — without sending your data anywhere.

---

## Screenshots

See [Screenshots.md](Screenshots.md) for a visual tour of the app.

---

## Features

### Analysis Dashboard
- **Overview** — Income, spending, and surplus cards for the current period; category breakdown donut chart; upcoming subscriptions; recent transactions; anomaly detection
- **Trends** — Category spending trend (stacked bar or multi-line chart); income vs outgoing; surplus trend; subscriptions vs discretionary spending — all filterable over 6, 12, 24 periods or custom timeframes.
- **Spending** — Category breakdown table with transaction counts and percentages; spending-by-tag breakdown
- **Subscriptions** — Full subscription list with monthly/annual cost projections, last-seen dates, and paid/N/A/overdue status; active subscription count and total monthly cost cards
- **Runway** — Days-until-payday tracker; where-you-stand breakdown (income received, subscriptions paid, discretionary spent); daily allowance; subscription checklist for the current period

### Transactions
- Paginated, filterable transaction list — filter by account, category, period, or free-text search
- CSV import with configurable date format and flexible column mapping (amount, debit/credit split, account name, account number, etc.)
- Click-to-edit category on any transaction
- Merchant logo enrichment via [logo.dev](https://logo.dev)
- Re-categorise all transactions in one click

### Categorisation Rules
- Keyword-based rules with AND matching and negative keywords
- Ability to specify amounts for particular rules to split multiple subscriptions from the same company
- Per-rule: priority, case sensitivity, amount matching, exclusion keywords/amounts, tags, subscription flag, comments
- Rules are applied automatically on import; editing or deleting a rule triggers a full recategorisation

### Categories
- Hierarchical parent → sub-category structure
- Custom/automatic colours and display order
- Shows rule count per category

### Settings
- Pay day keyword and income keyword list (defines pay period boundaries)
- CSV date format and column mapping
- Currency symbol
- logo.dev API key configuration
- Database export and import (SQLite)

### General
- Single-user authentication with scrypt password hashing
- Privacy mode — blurs all amounts on screen
- Dark / light theme toggle
- Fully responsive layout

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.12, FastAPI, Uvicorn |
| Database | SQLite (via aiosqlite) |
| Templates | Jinja2 |
| Frontend JS | Alpine.js v3, HTMX |
| Charts | Chart.js |
| Container | Docker (multi-arch: amd64 + arm64) |

---

## Installation

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)

### Quick Start

**1. Create a directory for Finch and download the compose file:**

```bash
mkdir finch && cd finch
curl -o docker-compose.yml https://raw.githubusercontent.com/jarrah31/finch/main/docker-compose.yml
```

**2. Create a `.env` file from the example:**

```bash
curl -o .env.example https://raw.githubusercontent.com/jarrah31/finch/main/.env.example
cp .env.example .env
```

Edit `.env` and fill in the required values:

```env
# Match these to your host user so files in ./data are owned correctly
PUID=1000
PGID=1000

# Generate a strong secret key:
# python3 -c "import secrets; print(secrets.token_hex(32))"
SECRET_KEY=your-secret-key-here
```

> **Important:** `SECRET_KEY` must be set before first run. If it changes, all existing sessions are invalidated.

**3. Start the container:**

```bash
docker compose up -d
```

**4. Open the app and complete setup:**

Navigate to `http://localhost:8000`. You will be prompted to set a password on first run.

---

## Docker Compose Reference

The default `docker-compose.yml`:

```yaml
services:
  finch:
    image: ghcr.io/jarrah31/finch:latest
    container_name: finch
    restart: unless-stopped
    ports:
      - "8000:8000"
    volumes:
      - ./data:/data
    environment:
      - PUID=${PUID:-1000}
      - PGID=${PGID:-1000}
      - SECRET_KEY=${SECRET_KEY}
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
```

All persistent data (database, secrets) is stored in `./data` on the host.

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SECRET_KEY` | Yes | — | Secret key for session signing. Generate with `python3 -c "import secrets; print(secrets.token_hex(32))"` |
| `PUID` | No | `1000` | Host user ID — files written to `/data` will be owned by this UID |
| `PGID` | No | `1000` | Host group ID — files written to `/data` will be owned by this GID |
| `DATA_DIR` | No | `/data` | Path inside the container where the database and secrets are stored |

---

## Updating

```bash
docker compose pull
docker compose up -d
```

Your data in `./data` is preserved across updates.

---

## First-Run Setup

After starting the container:

1. Open `http://localhost:8000` — you'll be redirected to `/setup` to create a password
2. Go to **Settings** and configure:
   - **Pay day keyword** — a string that appears in your income transactions (e.g. `SALARY`, `PAYROLL`)
   - **Income keywords** — additional keywords that identify income (e.g. `HMRC`, `BACS`)
   - **CSV date format** — the date format used by your bank's CSV export (default: `%d %b %Y`)
   - **CSV column mapping** — map your bank's CSV headers to the fields Finch expects (Date, Description, Amount or Debit/Credit, Balance, Account Name, Account Number)
   - **Currency symbol** — defaults to `£`
3. Go to **Transactions** and click **Import CSV** to import your first bank export

---

## Importing Transactions

1. Export transactions from your bank as CSV
2. Go to **Transactions → Import CSV**
3. Finch will parse the file using your configured column mapping and date format
4. Transactions are automatically categorised using your rules
5. Any uncategorised transactions can be manually assigned a category

---

## CSV Column Mapping

If your bank's CSV headers don't match Finch's defaults, go to **Settings → CSV Column Mapping**:

1. Upload a sample CSV — Finch will detect the headers automatically
2. Map each header to the correct field (Date, Description, Amount, Debit, Credit, Type, Balance, Account Name, Account Number)
3. Choose whether your CSV uses a single Amount column or separate Debit/Credit columns
4. Save — the mapping is applied to all future imports

---

## Building from Source

```bash
git clone https://github.com/jarrah31/finch.git
cd finch
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
uvicorn app.main:app --reload
```

---

## Docker Image

Pre-built multi-arch images (amd64 + arm64) are published to the GitHub Container Registry on every tagged release:

```
ghcr.io/jarrah31/finch:latest
ghcr.io/jarrah31/finch:v1.0.0
```

---

## License

MIT
