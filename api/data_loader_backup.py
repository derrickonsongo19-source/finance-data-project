# api/data_loader.py
import os
import polars as pl
import duckdb
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DataLoader:
    def __init__(self):
        self.duckdb_path = os.getenv("DUCKDB_PATH", "../data/gold/finance.db")
        self.conn = duckdb.connect(self.duckdb_path)
        
    def get_financial_summary(self) -> Dict[str, Any]:
        """Get current financial summary"""
        try:
            # Get current month transactions
            query = """
            SELECT 
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
                ABS(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END)) as expenses,
                SUM(amount) as balance
            FROM gold.transactions
            WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE)
            AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)
            """
            
            result = self.conn.execute(query).fetchone()
            
            if result:
                income, expenses, balance = result
            else:
                income, expenses, balance = 0, 0, 0
            
            # Get top categories
            top_cats_query = """
            SELECT 
                category,
                ABS(SUM(amount)) as total_spent
            FROM gold.transactions 
            WHERE amount < 0 
            AND date >= CURRENT_DATE - INTERVAL 30 DAYS
            GROUP BY category
            ORDER BY total_spent DESC
            LIMIT 5
            """
            
            top_categories = self.conn.execute(top_cats_query).fetchall()
            
            return {
                "total_balance": float(balance) if balance else 0.0,
                "monthly_income": float(income) if income else 0.0,
                "monthly_expenses": float(expenses) if expenses else 0.0,
                "top_categories": [
                    {"category": cat, "amount": float(amt)}
                    for cat, amt in top_categories
                ],
                "last_updated": datetime.now()
            }
            
        except Exception as e:
            print(f"Error getting financial summary: {e}")
            return {
                "total_balance": 0.0,
                "monthly_income": 0.0,
                "monthly_expenses": 0.0,
                "top_categories": [],
                "last_updated": datetime.now()
            }
    
    def get_recent_transactions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent transactions"""
        try:
            query = f"""
            SELECT 
                id,
                date,
                amount,
                description,
                category,
                account,
                created_at
            FROM gold.transactions
            ORDER BY date DESC, created_at DESC
            LIMIT {limit}
            """
            
            transactions = self.conn.execute(query).fetchall()
            
            return [
                {
                    "id": t[0],
                    "date": t[1].isoformat() if isinstance(t[1], date) else t[1],
                    "amount": float(t[2]),
                    "description": t[3],
                    "category": t[4],
                    "account": t[5],
                    "created_at": t[6].isoformat() if isinstance(t[6], datetime) else t[6]
                }
                for t in transactions
            ]
        except Exception as e:
            print(f"Error getting transactions: {e}")
            return []
    
    def get_spending_by_category(self, days: int = 30) -> Dict[str, float]:
        """Get spending by category for the last N days"""
        try:
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            query = f"""
            SELECT 
                category,
                ABS(SUM(amount)) as total_spent
            FROM gold.transactions
            WHERE amount < 0 
            AND date >= '{start_date}'
            GROUP BY category
            ORDER BY total_spent DESC
            """
            
            results = self.conn.execute(query).fetchall()
            return {cat: float(amt) for cat, amt in results}
            
        except Exception as e:
            print(f"Error getting spending by category: {e}")
            return {}
    
    def get_historical_data(self, months: int = 6) -> List[Dict[str, Any]]:
        """Get historical monthly data"""
        try:
            query = f"""
            SELECT 
                DATE_TRUNC('month', date) as month,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
                ABS(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END)) as expenses,
                SUM(amount) as balance
            FROM gold.transactions
            WHERE date >= CURRENT_DATE - INTERVAL '{months} months'
            GROUP BY DATE_TRUNC('month', date)
            ORDER BY month DESC
            """
            
            results = self.conn.execute(query).fetchall()
            return [
                {
                    "month": row[0].strftime("%Y-%m"),
                    "income": float(row[1]) if row[1] else 0.0,
                    "expenses": float(row[2]) if row[2] else 0.0,
                    "balance": float(row[3]) if row[3] else 0.0
                }
                for row in results
            ]
            
        except Exception as e:
            print(f"Error getting historical data: {e}")
            return []
    
    def close(self):
        """Close database connection"""
        self.conn.close()

# Singleton instance
data_loader = DataLoader()
