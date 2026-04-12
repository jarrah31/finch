from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from ..database import get_db
from ..auth import hash_password, verify_password

router = APIRouter()


# ── Login ─────────────────────────────────────────────────────────────────────

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str = "/"):
    if request.session.get("authenticated"):
        return RedirectResponse("/")
    return request.app.state.templates.TemplateResponse(
        request, "login.html", {"next": next, "error": None}
    )


@router.post("/login")
async def login_post(
    request: Request,
    password: str = Form(...),
    next: str = Form(default="/"),
):
    db = await get_db()
    try:
        cursor = await db.execute("SELECT value FROM settings WHERE key = 'password_hash'")
        row = await cursor.fetchone()
        stored = row["value"] if row else ""
        if stored and verify_password(password, stored):
            request.session["authenticated"] = True
            # Prevent open-redirect: only allow same-origin next paths
            safe_next = next if (next.startswith("/") and not next.startswith("//")) else "/"
            return RedirectResponse(safe_next, status_code=303)
        return request.app.state.templates.TemplateResponse(
            request, "login.html",
            {"next": next, "error": "Incorrect password. Please try again."},
            status_code=401,
        )
    finally:
        await db.close()


# ── Logout ────────────────────────────────────────────────────────────────────

@router.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


# ── First-run setup ───────────────────────────────────────────────────────────

@router.get("/setup", response_class=HTMLResponse)
async def setup_page(request: Request):
    # If already configured, send to login
    if not getattr(request.app.state, "setup_required", True):
        return RedirectResponse("/login")
    return request.app.state.templates.TemplateResponse(
        request, "setup.html", {"error": None}
    )


@router.post("/setup")
async def setup_post(
    request: Request,
    password: str = Form(...),
    confirm: str = Form(...),
):
    if password != confirm:
        return request.app.state.templates.TemplateResponse(
            request, "setup.html",
            {"error": "Passwords do not match."},
            status_code=400,
        )
    if len(password) < 8:
        return request.app.state.templates.TemplateResponse(
            request, "setup.html",
            {"error": "Password must be at least 8 characters."},
            status_code=400,
        )
    db = await get_db()
    try:
        hashed = hash_password(password)
        await db.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('password_hash', ?)",
            (hashed,),
        )
        await db.commit()
    finally:
        await db.close()
    request.app.state.setup_required = False
    request.session["authenticated"] = True
    return RedirectResponse("/onboarding", status_code=303)


# ── Account / change password ─────────────────────────────────────────────────

@router.get("/account", response_class=HTMLResponse)
async def account_page(request: Request):
    return request.app.state.templates.TemplateResponse(
        request, "account.html", {"success": False, "error": None}
    )


class PasswordChange(BaseModel):
    current_password: str
    new_password: str
    confirm_password: str


@router.post("/api/account/password")
async def change_password(request: Request, data: PasswordChange):
    from fastapi.responses import JSONResponse
    db = await get_db()
    try:
        cursor = await db.execute("SELECT value FROM settings WHERE key = 'password_hash'")
        row = await cursor.fetchone()
        stored = row["value"] if row else ""
        if not stored or not verify_password(data.current_password, stored):
            return JSONResponse({"detail": "Current password is incorrect."}, status_code=400)
        if len(data.new_password) < 8:
            return JSONResponse({"detail": "New password must be at least 8 characters."}, status_code=400)
        if data.new_password != data.confirm_password:
            return JSONResponse({"detail": "New passwords do not match."}, status_code=400)
        hashed = hash_password(data.new_password)
        await db.execute("UPDATE settings SET value = ? WHERE key = 'password_hash'", (hashed,))
        await db.commit()
        return {"ok": True}
    finally:
        await db.close()
