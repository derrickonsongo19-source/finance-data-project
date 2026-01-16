# api/data_loader_corrected.py - Corrected for actual table names
import os
import polars as pl
import duckdb
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DuckDBAnalytics:
    def close(self):
        """Close connection"""
        self.conn.close()
    """DuckDB Analytics Engine for enhanced queries"""
    
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)
        self.setup_analytics_views()
    
    def setup_analytics_views(self):
        """Create advanced analytics views based on actual tables"""
        print("🔧 Setting up DuckDB analytics views for actual schema...")
        
        # 1. Create unified transactions view from silver_transactions
        self.conn.execute("""
            CREATE OR REPLACE VIEW analytics_transactions AS
            SELECT 
                id,
                date,
                amount,
                category,
                account,
                CURRENT_TIMESTAMP as created_at,  -- Using current timestamp since we don't have created_at
                CASE 
                    WHEN amount < 0 THEN 'Expense'
                    WHEN amount > 0 THEN 'Income'
                    ELSE 'Neutral'
                END as transaction_type,
                ABS(amount) as absolute_amount,
                EXTRACT(YEAR FROM date) as year,
                EXTRACT(MONTH FROM date) as month,
                EXTRACT(DAY FROM date) as day,
                EXTRACT(DOW FROM date) as day_of_week,
                EXTRACT(WEEK FROM date) as week_number
            FROM silver_transactions
            WHERE date IS NOT NULL
        """)
        
        # 2. Monthly summary with trends
        self.conn.execute("""
            CREATE OR REPLACE VIEW monthly_summary AS
            SELECT
                year,
                month,
                COUNT(*) as transaction_count,
                SUM(amount) as net_flow,
                SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) as total_expenses,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_income,
                AVG(amount) as avg_transaction,
                MIN(amount) as min_transaction,
                MAX(amount) as max_transaction,
                STDDEV(amount) as std_dev_transaction
            FROM analytics_transactions
            GROUP BY year, month
            ORDER BY year DESC, month DESC
        """)
        
        # 3. Category analytics
        self.conn.execute("""
            CREATE OR REPLACE VIEW category_analytics AS
            SELECT
                category,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount,
                MIN(amount) as min_amount,
                MAX(amount) as max_amount,
                SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as expense_count,
                SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) as income_count
            FROM analytics_transactions
            GROUP BY category
            ORDER BY ABS(SUM(amount)) DESC
        """)
        
        # 4. Running balance view
        self.conn.execute("""
            CREATE OR REPLACE VIEW running_analytics AS
            SELECT
                date,
                amount,
                category,
                account,
                SUM(amount) OVER (ORDER BY date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_balance,
                SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) 
                    OVER (ORDER BY date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_expenses,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) 
                    OVER (ORDER BY date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_income,
                ROW_NUMBER() OVER (ORDER BY date, id) as transaction_number
            FROM analytics_transactions
            ORDER BY date, id
        """)
        
        print("✅ DuckDB analytics views created for actual schema!")

class EnhancedDataLoader:
    def __init__(self):
        self.duckdb_path = os.getenv("DUCKDB_PATH", "finance_data.db")
        self.conn = duckdb.connect(self.duckdb_path)
        self.analytics = DuckDBAnalytics(self.duckdb_path)
        
    def get_financial_summary(self) -> Dict[str, Any]:
        """Enhanced financial summary using actual tables"""
        try:
            # Get basic summary from silver_transactions
            query = """
            SELECT 
                (SELECT SUM(amount) FROM silver_transactions) as total_balance,
                (SELECT SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) 
                 FROM silver_transactions 
                 WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE)
                   AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)) as monthly_income,
                (SELECT ABS(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END))
                 FROM silver_transactions 
                 WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE)
                   AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)) as monthly_expenses
            """
            
            result = self.conn.execute(query).fetchone()
            total_balance, monthly_income, monthly_expenses = result or (0, 0, 0)
            
            # Get category breakdown
            category_query = """
            SELECT 
                category,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM silver_transactions
            GROUP BY category
            ORDER BY ABS(SUM(amount)) DESC
            LIMIT 10
            """
            
            categories = self.conn.execute(category_query).fetchall()
            categories_data = []
            for cat, count, total, avg in categories:
                categories_data.append({
                    "category": cat,
                    "transaction_count": count,
                    "total_amount": float(total) if total else 0.0,
                    "avg_amount": float(avg) if avg else 0.0
                })
            
            return {
                "total_balance": float(total_balance) if total_balance else 0.0,
                "monthly_income": float(monthly_income) if monthly_income else 0.0,
                "monthly_expenses": float(monthly_expenses) if monthly_expenses else 0.0,
                "savings_rate": ((float(monthly_income) - float(monthly_expenses)) / float(monthly_income) * 100 
                                if monthly_income and float(monthly_income) > 0 else 0.0),
                "category_breakdown": categories_data,
                "analytics_timestamp": datetime.now().isoformat(),
                "data_points": {
                    "total_transactions": int(self.conn.execute("SELECT COUNT(*) FROM silver_transactions").fetchone()[0]),
                    "categories_count": len(categories_data)
                }
            }
            
        except Exception as e:
            print(f"Error in financial summary: {e}")
            return self._get_basic_financial_summary()
    
    def _get_basic_financial_summary(self) -> Dict[str, Any]:
        """Basic fallback summary"""
        return {
            "total_balance": 0.0,
            "monthly_income": 0.0,
            "monthly_expenses": 0.0,
            "savings_rate": 0.0,
            "category_breakdown": [],
            "analytics_timestamp": datetime.now().isoformat(),
            "data_points": {"total_transactions": 0, "categories_count": 0}
        }
    
    def get_recent_transactions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent transactions"""
        try:
            query = f"""
            SELECT 
                id,
                date,
                amount,
                category,
                account
            FROM silver_transactions
            ORDER BY date DESC
            LIMIT {limit}
            """
            
            transactions = self.conn.execute(query).fetchall()
            
            return [
                {
                    "id": t[0],
                    "date": t[1].isoformat() if isinstance(t[1], date) else t[1],
                    "amount": float(t[2]),
                    "category": t[3],
                    "account": t[4]
                }
                for t in transactions
            ]
        except Exception as e:
            print(f"Error getting transactions: {e}")
            return []
    
    def get_spending_by_category(self, days: int = 30) -> Dict[str, Any]:
        """Get spending by category with basic analysis"""
        try:
            # Get current period spending
            current_query = f"""
            SELECT 
                category,
                ABS(SUM(amount)) as current_spent
            FROM silver_transactions
            WHERE amount < 0 
            AND date >= CURRENT_DATE - INTERVAL '{days}' DAYS
            GROUP BY category
            """
            
            results = self.conn.execute(current_query).fetchall()
            
            spending_data = {}
            for category, current in results:
                spending_data[category] = {
                    "current": float(current) if current else 0.0,
                    "previous": 0.0,  # We don't have previous data in this simple version
                    "change": 0.0,
                    "pct_change": 0.0
                }
            
            return spending_data
            
        except Exception as e:
            print(f"Error in spending analysis: {e}")
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
            FROM silver_transactions
            WHERE date >= CURRENT_DATE - INTERVAL '{months}' months
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
            print(f"Error in historical data: {e}")
            return []
    
    def execute_custom_analytics(self, query_name: str) -> Dict[str, Any]:
        """Execute predefined analytics queries"""
        queries = {
            "current_month_detailed": """
                SELECT 
                    category,
                    CASE WHEN amount < 0 THEN 'Expense' ELSE 'Income' END as transaction_type,
                    COUNT(*) as count,
                    SUM(amount) as total,
                    AVG(amount) as average
                FROM silver_transactions
                WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE) 
                  AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)
                GROUP BY category, CASE WHEN amount < 0 THEN 'Expense' ELSE 'Income' END
                ORDER BY ABS(SUM(amount)) DESC
            """,
            "category_breakdown": """
                SELECT
                    category,
                    COUNT(*) as transaction_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount
                FROM silver_transactions
                GROUP BY category
                ORDER BY ABS(SUM(amount)) DESC
            """
        }
        
        if query_name not in queries:
            return {
                "query": query_name,
                "error": f"Unknown query. Available: {list(queries.keys())}",
                "data": [],
                "row_count": 0,
                "columns": []
            }
        
        try:
            result = self.conn.execute(queries[query_name]).fetchall()
            columns = [desc[0] for desc in self.conn.description]
            
            data = []
            for row in result:
                row_dict = {}
                for i, col in enumerate(columns):
                    value = row[i]
                    # Convert types for JSON serialization
                    if isinstance(value, (datetime, date)):
                        row_dict[col] = value.isoformat()
                    elif isinstance(value, (int, float)):
                        row_dict[col] = float(value)
                    else:
                        row_dict[col] = value
                data.append(row_dict)
            
            return {
                "query": query_name,
                "data": data,
                "row_count": len(data),
                "columns": columns
            }
        except Exception as e:
            return {
                "query": query_name,
                "error": str(e),
                "data": [],
                "row_count": 0,
                "columns": []
            }
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            tables = self.conn.execute("SHOW TABLES").fetchall()
            
            stats = {}
            for table in tables:
                table_name = table[0]
                try:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    stats[table_name] = {
                        "type": "table",
                        "row_count": count
                    }
                except:
                    stats[table_name] = {
                        "type": "table",
                        "row_count": "unknown"
                    }
            
            return {
                "database": self.duckdb_path,
                "tables_count": len(tables),
                "objects": stats,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "database": self.duckdb_path,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def close(self):
        """Close database connections"""
        self.analytics.close()
        self.conn.close()

# Singleton instance
data_loader = EnhancedDataLoader()
