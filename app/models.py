from pydantic import BaseModel
from typing import Optional


class CategoryCreate(BaseModel):
    name: str
    display_order: int = 0
    color: Optional[str] = None
    parent_id: Optional[int] = None


class CategoryUpdate(BaseModel):
    name: Optional[str] = None
    display_order: Optional[int] = None
    color: Optional[str] = None
    parent_id: Optional[int] = None


class RuleCreate(BaseModel):
    category_id: int
    keyword: str
    keywords: list[str] = []
    match_amounts: list[float] = []
    exclude_amounts: list[float] = []
    exclude_keywords: list[str] = []
    priority: int = 100
    case_sensitive: bool = False
    comment: Optional[str] = None
    is_subscription: bool = False
    subscription_period: Optional[str] = None
    tags: list[str] = []


class RuleUpdate(BaseModel):
    category_id: Optional[int] = None
    keyword: Optional[str] = None
    keywords: Optional[list[str]] = None
    match_amounts: Optional[list[float]] = None
    exclude_amounts: Optional[list[float]] = None
    exclude_keywords: Optional[list[str]] = None
    priority: Optional[int] = None
    case_sensitive: Optional[bool] = None
    comment: Optional[str] = None
    is_subscription: Optional[bool] = None
    subscription_period: Optional[str] = None
    tags: Optional[list[str]] = None


class TransactionCategoryUpdate(BaseModel):
    category_id: Optional[int] = None


class SettingsUpdate(BaseModel):
    pay_day_keyword: Optional[str] = None
    csv_date_format: Optional[str] = None
    currency_symbol: Optional[str] = None
    income_keywords: Optional[list[str]] = None
    logodev_publishable_key: Optional[str] = None
    logodev_secret_key: Optional[str] = None
    csv_column_mapping: Optional[dict] = None
    onboarding_complete: Optional[str] = None


class MerchantOverrideUpdate(BaseModel):
    description_key: str
    domain: Optional[str] = None  # None = clear override


class RuleTest(BaseModel):
    description: str
    amount: float
