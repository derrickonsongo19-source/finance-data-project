# api/models.py
from pydantic import BaseModel
from typing import List, Optional
from datetime import date, datetime
from enum import Enum

class Category(str, Enum):
    FOOD = "food"
    TRANSPORT = "transport"
    ENTERTAINMENT = "entertainment"
    UTILITIES = "utilities"
    SHOPPING = "shopping"
    HEALTH = "health"
    INCOME = "income"
    OTHER = "other"

class TransactionBase(BaseModel):
    date: date
    amount: float
    description: str
    category: Category
    account: str

class TransactionCreate(TransactionBase):
    pass

class Transaction(TransactionBase):
    id: int
    created_at: datetime
    
    class Config:
        from_attributes = True

class FinancialSummary(BaseModel):
    total_balance: float
    monthly_income: float
    monthly_expenses: float
    top_categories: List[dict]
    last_updated: datetime

class SpendingPrediction(BaseModel):
    month: str
    predicted_amount: float
    category: str
    confidence: float

class BudgetRecommendation(BaseModel):
    category: str
    current_spending: float
    recommended_limit: float
    percentage_change: float
    reason: str
