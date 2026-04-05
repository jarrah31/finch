import json
from fastapi import APIRouter
from ..database import get_db
from ..models import RuleCreate, RuleUpdate, RuleTest
from ..services.categorizer import get_rules, match_transaction, categorize_uncategorized, recategorize_all

router = APIRouter()


@router.get("/api/rules")
async def list_rules():
    db = await get_db()
    try:
        return await get_rules(db)
    finally:
        await db.close()


@router.post("/api/rules")
async def create_rule(data: RuleCreate):
    db = await get_db()
    try:
        amounts_json = json.dumps(data.match_amounts) if data.match_amounts else None
        exc_amounts_json = json.dumps(data.exclude_amounts) if data.exclude_amounts else None
        exc_keywords_json = json.dumps(data.exclude_keywords) if data.exclude_keywords else None
        # Merge keyword + keywords into one deduplicated list
        kw_list = data.keywords if data.keywords else ([data.keyword] if data.keyword else [])
        primary_kw = kw_list[0] if kw_list else data.keyword
        keywords_json = json.dumps(kw_list) if len(kw_list) > 1 else None
        tags_json = json.dumps(data.tags) if data.tags else None
        cursor = await db.execute(
            "INSERT INTO categorization_rules (category_id, keyword, keywords, match_amounts, exclude_amounts, exclude_keywords, priority, case_sensitive, comment, is_subscription, subscription_period, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (data.category_id, primary_kw, keywords_json, amounts_json, exc_amounts_json, exc_keywords_json, data.priority, int(data.case_sensitive), data.comment or None, int(data.is_subscription), data.subscription_period or None, tags_json),
        )
        await db.commit()
        rule_id = cursor.lastrowid
        categorized = await categorize_uncategorized(db)
        return {"id": rule_id, "categorized": categorized}
    finally:
        await db.close()


@router.patch("/api/rules/{rule_id}")
async def update_rule(rule_id: int, data: RuleUpdate):
    db = await get_db()
    try:
        updates = []
        params = []
        if data.category_id is not None:
            updates.append("category_id = ?")
            params.append(data.category_id)
        if data.keyword is not None:
            updates.append("keyword = ?")
            params.append(data.keyword)
        if data.keywords is not None:
            primary = data.keywords[0] if data.keywords else ''
            updates.append("keyword = ?")
            params.append(primary)
            updates.append("keywords = ?")
            params.append(json.dumps(data.keywords) if len(data.keywords) > 1 else None)
        if data.match_amounts is not None:
            updates.append("match_amounts = ?")
            params.append(json.dumps(data.match_amounts) if data.match_amounts else None)
        if data.exclude_amounts is not None:
            updates.append("exclude_amounts = ?")
            params.append(json.dumps(data.exclude_amounts) if data.exclude_amounts else None)
        if data.exclude_keywords is not None:
            updates.append("exclude_keywords = ?")
            params.append(json.dumps(data.exclude_keywords) if data.exclude_keywords else None)
        if data.priority is not None:
            updates.append("priority = ?")
            params.append(data.priority)
        if data.case_sensitive is not None:
            updates.append("case_sensitive = ?")
            params.append(int(data.case_sensitive))
        if data.comment is not None:
            updates.append("comment = ?")
            params.append(data.comment or None)
        if data.is_subscription is not None:
            updates.append("is_subscription = ?")
            params.append(int(data.is_subscription))
        if 'subscription_period' in data.model_fields_set:
            updates.append("subscription_period = ?")
            params.append(data.subscription_period or None)
        if data.tags is not None:
            updates.append("tags = ?")
            params.append(json.dumps(data.tags) if data.tags else None)

        if updates:
            params.append(rule_id)
            await db.execute(
                f"UPDATE categorization_rules SET {', '.join(updates)} WHERE id = ?",
                params,
            )
            await db.commit()
            await recategorize_all(db)
        return {"ok": True}
    finally:
        await db.close()


@router.delete("/api/rules/{rule_id}")
async def delete_rule(rule_id: int):
    db = await get_db()
    try:
        await db.execute("DELETE FROM categorization_rules WHERE id = ?", (rule_id,))
        await db.commit()
        await recategorize_all(db)
        return {"ok": True}
    finally:
        await db.close()


@router.post("/api/rules/test")
async def test_rule(data: RuleTest):
    db = await get_db()
    try:
        rules = await get_rules(db)
        rule = match_transaction(data.description, data.amount, rules)
        if rule:
            return {
                "matched": True,
                "category_name": rule["category_name"],
                "category_id": rule["category_id"],
                "keyword": rule["keyword"],
                "priority": rule["priority"],
                "match_amounts": rule["match_amounts"],
                "case_sensitive": bool(rule["case_sensitive"]),
            }
        return {"matched": False}
    finally:
        await db.close()
