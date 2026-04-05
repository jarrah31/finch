import json
import aiosqlite

from .mcc import get_or_create_mcc_category, apply_mcc_tag


async def get_rules(db: aiosqlite.Connection) -> list[dict]:
    """Get all categorization rules sorted by priority, with transaction match counts."""
    # AND-match helper: all keywords must appear in description.
    # Uses double-negation: "all match" = "no keyword is absent".
    # Falls back to legacy single keyword column when keywords JSON is empty/null.
    kw_match = """
        CASE WHEN cr.keywords IS NOT NULL AND cr.keywords != '[]' THEN
            NOT EXISTS (
                SELECT 1 FROM json_each(cr.keywords) je
                WHERE UPPER(t.description) NOT LIKE '%' || UPPER(je.value) || '%'
            )
        ELSE
            LOWER(t.description) LIKE '%' || LOWER(cr.keyword) || '%'
        END
    """
    amount_match = """
        (cr.match_amounts IS NULL OR cr.match_amounts = '[]'
         OR EXISTS (
             SELECT 1 FROM json_each(cr.match_amounts) je
             WHERE ABS(t.amount - je.value) < 0.005
         ))
    """
    amount_exclude = """
        (cr.exclude_amounts IS NULL OR cr.exclude_amounts = '[]'
         OR NOT EXISTS (
             SELECT 1 FROM json_each(cr.exclude_amounts) je
             WHERE ABS(t.amount - je.value) < 0.005
         ))
    """
    kw_exclude = """
        (cr.exclude_keywords IS NULL OR cr.exclude_keywords = '[]'
         OR NOT EXISTS (
             SELECT 1 FROM json_each(cr.exclude_keywords) je
             WHERE UPPER(t.description) LIKE '%' || UPPER(je.value) || '%'
         ))
    """
    match_clause = f"({kw_match}) AND {amount_match} AND {amount_exclude} AND {kw_exclude}"
    cursor = await db.execute(
        f"""SELECT cr.id, cr.keyword, cr.keywords, cr.match_amounts, cr.exclude_amounts,
                  cr.exclude_keywords, cr.priority, cr.case_sensitive,
                  cr.comment, cr.is_subscription, cr.subscription_period, cr.tags, cr.category_id, c.name as category_name,
                  (SELECT COUNT(*) FROM transactions t WHERE {match_clause}) as tx_count,
                  (SELECT MIN(t.date) FROM transactions t WHERE {match_clause}) as tx_min_date,
                  (SELECT MAX(t.date) FROM transactions t WHERE {match_clause}) as tx_max_date
           FROM categorization_rules cr
           JOIN categories c ON cr.category_id = c.id
           ORDER BY cr.priority ASC, cr.id ASC"""
    )
    rows = []
    for row in await cursor.fetchall():
        d = dict(row)
        raw = d.get("match_amounts")
        d["match_amounts"] = json.loads(raw) if raw else []
        raw_exc_amt = d.get("exclude_amounts")
        d["exclude_amounts"] = json.loads(raw_exc_amt) if raw_exc_amt else []
        raw_kw = d.get("keywords")
        d["keywords"] = json.loads(raw_kw) if raw_kw else [d["keyword"]]
        raw_exc_kw = d.get("exclude_keywords")
        d["exclude_keywords"] = json.loads(raw_exc_kw) if raw_exc_kw else []
        raw_tags = d.get("tags")
        d["tags"] = json.loads(raw_tags) if raw_tags else []
        rows.append(d)
    return rows


def match_transaction(description: str, amount: float, rules: list[dict]) -> dict | None:
    """Match a transaction against rules. Returns the matched rule dict or None."""
    for rule in rules:
        keywords = rule.get("keywords") or [rule["keyword"]]
        if rule.get("case_sensitive"):
            matched = all(kw in description for kw in keywords)
        else:
            desc_upper = description.upper()
            matched = all(kw.upper() in desc_upper for kw in keywords)
        if not matched:
            continue

        # Check exclude keywords — if any exclusion keyword appears, skip this rule
        exclude_kws = rule.get("exclude_keywords") or []
        if exclude_kws:
            if rule.get("case_sensitive"):
                if any(ekw in description for ekw in exclude_kws):
                    continue
            else:
                if any(ekw.upper() in desc_upper for ekw in exclude_kws):
                    continue

        # Check exclude amounts — if amount matches an exclusion, skip this rule
        exclude_amounts = rule.get("exclude_amounts") or []
        if exclude_amounts and any(abs(amount - ea) < 0.005 for ea in exclude_amounts):
            continue

        # Check include amounts
        amounts = rule.get("match_amounts") or []
        if not amounts:
            return rule
        elif any(abs(amount - ma) < 0.005 for ma in amounts):
            return rule

    return None


async def categorize_transaction(
    db: aiosqlite.Connection,
    tx_id: int,
    description: str,
    amount: float,
    rules: list[dict],
    mcc_code: str | None = None,
) -> int | None:
    """Categorize a single transaction and update it in the database.

    MCC logic:
    - Rule matches → rule category wins; MCC stored as tag (if mcc_code present)
    - No rule match + mcc_code → MCC category assigned
    """
    rule = match_transaction(description, amount, rules)
    if rule is not None:
        await db.execute(
            "UPDATE transactions SET category_id = ?, matched_rule_id = ?, manual_category = 0 WHERE id = ?",
            (rule["category_id"], rule["id"], tx_id),
        )
        if mcc_code:
            await apply_mcc_tag(db, tx_id, mcc_code)
        return rule["category_id"]

    if mcc_code:
        mcc_cat_id = await get_or_create_mcc_category(db, mcc_code)
        if mcc_cat_id:
            await db.execute(
                "UPDATE transactions SET category_id = ?, manual_category = 0 WHERE id = ?",
                (mcc_cat_id, tx_id),
            )
            return mcc_cat_id

    return None


async def categorize_uncategorized(db: aiosqlite.Connection) -> int:
    """Run categorization on all uncategorized transactions. Returns count categorized."""
    rules = await get_rules(db)

    cursor = await db.execute(
        "SELECT id, description, amount, mcc_code FROM transactions WHERE category_id IS NULL"
    )
    transactions = await cursor.fetchall()

    count = 0
    for tx in transactions:
        rule = match_transaction(tx["description"], tx["amount"], rules)
        if rule is not None:
            await db.execute(
                "UPDATE transactions SET category_id = ?, matched_rule_id = ?, manual_category = 0 WHERE id = ?",
                (rule["category_id"], rule["id"], tx["id"]),
            )
            if tx["mcc_code"]:
                await apply_mcc_tag(db, tx["id"], tx["mcc_code"])
            count += 1
        elif tx["mcc_code"]:
            mcc_cat_id = await get_or_create_mcc_category(db, tx["mcc_code"])
            if mcc_cat_id:
                await db.execute(
                    "UPDATE transactions SET category_id = ?, manual_category = 0 WHERE id = ?",
                    (mcc_cat_id, tx["id"]),
                )
                count += 1

    await db.commit()
    return count


async def recategorize_all(db: aiosqlite.Connection) -> int:
    """Re-run categorization on all non-manual transactions."""
    # Clear non-manual categories and matched rule
    await db.execute(
        "UPDATE transactions SET category_id = NULL, matched_rule_id = NULL WHERE manual_category = 0"
    )
    await db.commit()
    return await categorize_uncategorized(db)
