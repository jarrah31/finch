from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from starlette.middleware.sessions import SessionMiddleware

from .auth import SECRET_KEY, AuthMiddleware
from .database import init_db, get_db
from .routers import transactions, categories, rules, settings, analysis, pages
from .routers import auth as auth_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    # Determine whether first-run password setup is still required
    db = await get_db()
    try:
        cursor = await db.execute("SELECT value FROM settings WHERE key = 'password_hash'")
        row = await cursor.fetchone()
        app.state.setup_required = not (row and row["value"])
    finally:
        await db.close()
    yield


app = FastAPI(title="Finance", lifespan=lifespan)

# Middleware — order matters: SessionMiddleware (added last) wraps everything
# so request.session is populated before AuthMiddleware inspects it.
app.add_middleware(AuthMiddleware)
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, session_cookie="session", https_only=False)

# Templates
templates_dir = Path(__file__).parent / "templates"
app.state.templates = Jinja2Templates(directory=str(templates_dir))

# Inject css_version (mtime-based) as a global template variable so the
# browser always fetches a fresh stylesheet after deployments.
_css_path = Path(__file__).parent / "static" / "app.css"
app.state.templates.env.globals["css_version"] = int(_css_path.stat().st_mtime) if _css_path.exists() else 1

# Add custom filters
def currency_filter(value):
    if value is None:
        return "£0.00"
    return f"£{value:,.2f}"

def abs_currency_filter(value):
    if value is None:
        return "£0.00"
    return f"£{abs(value):,.2f}"

app.state.templates.env.filters["currency"] = currency_filter
app.state.templates.env.filters["abs_currency"] = abs_currency_filter

# Static files
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Routers
app.include_router(auth_router.router)
app.include_router(pages.router)
app.include_router(transactions.router)
app.include_router(categories.router)
app.include_router(rules.router)
app.include_router(settings.router)
app.include_router(analysis.router)
