from fastapi import APIRouter
from ..database import get_db
from ..models import CategoryCreate, CategoryUpdate

router = APIRouter()


@router.get("/api/categories")
async def list_categories():
    db = await get_db()
    try:
        cursor = await db.execute(
            """SELECT c.*, COUNT(cr.id) as rule_count, p.name as parent_name
               FROM categories c
               LEFT JOIN categories p ON p.id = c.parent_id
               LEFT JOIN categorization_rules cr ON cr.category_id = c.id
               GROUP BY c.id
               ORDER BY COALESCE(c.parent_id, c.id), c.parent_id IS NOT NULL, c.name"""
        )
        return [dict(row) for row in await cursor.fetchall()]
    finally:
        await db.close()


@router.post("/api/categories")
async def create_category(data: CategoryCreate):
    db = await get_db()
    try:
        cursor = await db.execute(
            "INSERT INTO categories (name, display_order, color, parent_id) VALUES (?, ?, ?, ?)",
            (data.name, data.display_order, data.color or None, data.parent_id),
        )
        await db.commit()
        new_id = cursor.lastrowid
        parent_name = None
        if data.parent_id is not None:
            pcursor = await db.execute("SELECT name FROM categories WHERE id = ?", (data.parent_id,))
            prow = await pcursor.fetchone()
            if prow:
                parent_name = prow["name"]
        return {
            "id": new_id,
            "name": data.name,
            "parent_id": data.parent_id,
            "display_order": data.display_order,
            "color": data.color or None,
            "parent_name": parent_name,
        }
    finally:
        await db.close()


@router.patch("/api/categories/{cat_id}")
async def update_category(cat_id: int, data: CategoryUpdate):
    db = await get_db()
    try:
        updates = []
        params = []
        if data.name is not None:
            updates.append("name = ?")
            params.append(data.name)
        if data.display_order is not None:
            updates.append("display_order = ?")
            params.append(data.display_order)
        if 'parent_id' in data.model_fields_set:
            updates.append("parent_id = ?")
            params.append(data.parent_id)
        if 'color' in data.model_fields_set:
            updates.append("color = ?")
            params.append(data.color or None)

        if updates:
            params.append(cat_id)
            await db.execute(
                f"UPDATE categories SET {', '.join(updates)} WHERE id = ?", params
            )
            await db.commit()
        return {"ok": True}
    finally:
        await db.close()


@router.delete("/api/categories/{cat_id}")
async def delete_category(cat_id: int):
    db = await get_db()
    try:
        await db.execute("UPDATE categories SET parent_id = NULL WHERE parent_id = ?", (cat_id,))
        await db.execute("UPDATE transactions SET category_id = NULL WHERE category_id = ?", (cat_id,))
        await db.execute("DELETE FROM categorization_rules WHERE category_id = ?", (cat_id,))
        await db.execute("DELETE FROM categories WHERE id = ?", (cat_id,))
        await db.commit()
        return {"ok": True}
    finally:
        await db.close()
