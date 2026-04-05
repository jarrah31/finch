from datetime import datetime, timedelta

import aiosqlite


async def get_pay_day_keyword(db: aiosqlite.Connection) -> str:
    cursor = await db.execute("SELECT value FROM settings WHERE key = 'pay_day_keyword'")
    row = await cursor.fetchone()
    return row[0] if row else ""


async def recompute_pay_periods(db: aiosqlite.Connection):
    """Detect pay days and recompute all pay periods."""
    keyword = await get_pay_day_keyword(db)
    if not keyword:
        return

    # Find all pay day transactions (incoming, matching keyword)
    cursor = await db.execute(
        """SELECT date, amount FROM transactions
           WHERE UPPER(description) LIKE '%' || UPPER(?) || '%'
             AND amount > 0
           ORDER BY date ASC""",
        (keyword,),
    )
    pay_days = [dict(row) for row in await cursor.fetchall()]

    if not pay_days:
        return

    # Clear existing pay periods
    await db.execute("DELETE FROM pay_periods")

    # Create pay periods from consecutive pairs
    for i in range(len(pay_days)):
        start_date = pay_days[i]["date"]
        pay_amount = pay_days[i]["amount"]

        if i + 1 < len(pay_days):
            end_date = pay_days[i + 1]["date"]
        else:
            end_date = "9999-12-31"

        # Label based on the last day of the period (end_date - 1 day),
        # so a period from 26 Jan to 26 Feb is called "Feb 2026" because
        # that's the month where most transactions occur.
        if end_date == "9999-12-31":
            label = "Current"
        else:
            last_day = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=1)
            label = last_day.strftime("%b %Y")

        await db.execute(
            """INSERT OR REPLACE INTO pay_periods (label, start_date, end_date, pay_amount)
               VALUES (?, ?, ?, ?)""",
            (label, start_date, end_date, pay_amount),
        )

    await db.commit()
