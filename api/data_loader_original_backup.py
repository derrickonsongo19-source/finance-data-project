# api/data_loader.py - Enhanced with DuckDB Analytics
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
    """DuckDB Analytics Engine for enhanced queries"""
    
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)
        self.setup_analytics_views()
    
    def setup_analytics_views(self):
        """Create advanced analytics views"""
        print("🔧 Setting up DuckDB analytics views...")
        
        # 1. Enhanced transactions view with analytics columns
        self.conn.execute("""
            CREATE OR REPLACE VIEW analytics_transactions AS
            SELECT 
                id,
                date,
                amount,
                description,
                category,
                account,
                created_at,
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
            FROM gold.transactions
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
        
        # 3. Category analytics with percentages
        self.conn.execute("""
            CREATE OR REPLACE VIEW category_analytics AS
            WITH category_totals AS (
                SELECT
                    category,
                    transaction_type,
                    COUNT(*) as transaction_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount
                FROM analytics_transactions
                GROUP BY category, transaction_type
            )
            SELECT
                category,
                transaction_type,
                transaction_count,
                total_amount,
                avg_amount,
                ROUND(transaction_count * 100.0 / SUM(transaction_count) OVER (PARTITION BY transaction_type), 2) as type_percentage
            FROM category_totals
            ORDER BY ABS(total_amount) DESC
        """)
        
        # 4. Running balance and cumulative views
        self.conn.execute("""
            CREATE OR REPLACE VIEW running_analytics AS
            SELECT
                date,
                amount,
                category,
                account,
                description,
                SUM(amount) OVER (ORDER BY date, created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_balance,
                SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) 
                    OVER (ORDER BY date, created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_expenses,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) 
                    OVER (ORDER BY date, created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_income
            FROM analytics_transactions
            ORDER BY date, created_at
        """)
        
        # 5. Weekly patterns analysis
        self.conn.execute("""
            CREATE OR REPLACE VIEW weekly_patterns AS
            SELECT
                day_of_week,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount,
                SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) as total_expenses,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_income
            FROM analytics_transactions
            GROUP BY day_of_week
            ORDER BY day_of_week
        """)
        
        print("✅ DuckDB analytics views created!")
    
    def execute_analytics_query(self, query_name: str, **params) -> pl.DataFrame:
        """Execute predefined analytics queries"""
        queries = {
            "current_month_detailed": """
                SELECT 
                    category,
                    transaction_type,
                    COUNT(*) as count,
                    SUM(amount) as total,
                    AVG(amount) as average
                FROM analytics_transactions
                WHERE year = EXTRACT(YEAR FROM CURRENT_DATE) 
                  AND month = EXTRACT(MONTH FROM CURRENT_DATE)
                GROUP BY category, transaction_type
                ORDER BY ABS(SUM(amount)) DESC
            """,
            "spending_trends": """
                WITH monthly_trends AS (
                    SELECT
                        year,
                        month,
                        SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) as expenses,
                        LAG(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END)) 
                            OVER (ORDER BY year, month) as prev_expenses
                    FROM analytics_transactions
                    GROUP BY year, month
                )
                SELECT
                    year,
                    month,
                    expenses,
                    prev_expenses,
                    expenses - prev_expenses as change,
                    ROUND((expenses - prev_expenses) * 100.0 / NULLIF(ABS(prev_expenses), 0), 2) as pct_change
                FROM monthly_trends
                ORDER BY year DESC, month DESC
                LIMIT 12
            """,
            "category_breakdown": """
                SELECT
                    category,
                    SUM(CASE WHEN transaction_type = 'Expense' THEN absolute_amount ELSE 0 END) as total_expenses,
                    SUM(CASE WHEN transaction_type = 'Income' THEN absolute_amount ELSE 0 END) as total_income,
                    COUNT(CASE WHEN transaction_type = 'Expense' THEN 1 END) as expense_count,
                    COUNT(CASE WHEN transaction_type = 'Income' THEN 1 END) as income_count,
                    ROUND(AVG(CASE WHEN transaction_type = 'Expense' THEN absolute_amount END), 2) as avg_expense,
                    ROUND(AVG(CASE WHEN transaction_type = 'Income' THEN absolute_amount END), 2) as avg_income
                FROM analytics_transactions
                GROUP BY category
                ORDER BY total_expenses DESC
            """,
            "account_analysis": """
                SELECT
                    account,
                    COUNT(*) as transaction_count,
                    SUM(amount) as net_flow,
                    SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) as expenses,
                    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
                    ROUND(AVG(amount), 2) as avg_transaction
                FROM analytics_transactions
                GROUP BY account
                ORDER BY ABS(SUM(amount)) DESC
            """
        }
        
        if query_name not in queries:
            raise ValueError(f"Unknown query: {query_name}")
        
        query = queries[query_name]
        result = self.conn.execute(query).df()
        return pl.from_pandas(result)
    
    def execute_custom_sql(self, sql: str) -> pl.DataFrame:
        """Execute custom SQL query"""
        try:
            result = self.conn.execute(sql).df()
            return pl.from_pandas(result)
        except Exception as e:
            raise ValueError(f"SQL Error: {str(e)}")
    
    def get_query_plan(self, sql: str):
        """Get query execution plan"""
        return self.conn.execute(f"EXPLAIN {sql}").df()
    
    def close(self):
        """Close connection"""
        self.conn.close()

class EnhancedDataLoader:
    def __init__(self):
        self.duckdb_path = os.getenv("DUCKDB_PATH", "finance_data.db")
        self.conn = duckdb.connect(self.duckdb_path)
        self.analytics = DuckDBAnalytics(self.duckdb_path)
        
    def get_financial_summary(self) -> Dict[str, Any]:
        """Enhanced financial summary with analytics"""
        try:
            # Get basic summary using DuckDB
            query = """
            SELECT 
                (SELECT SUM(amount) FROM gold.transactions) as total_balance,
                (SELECT SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) 
                 FROM gold.transactions 
                 WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE)
                   AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)) as monthly_income,
                (SELECT ABS(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END))
                 FROM gold.transactions 
                 WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE)
                   AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)) as monthly_expenses
            """
            
            result = self.conn.execute(query).fetchone()
            total_balance, monthly_income, monthly_expenses = result
            
            # Get enhanced analytics
            category_breakdown = self.analytics.execute_analytics_query("category_breakdown")
            weekly_patterns = self.analytics.execute_custom_sql("SELECT * FROM weekly_patterns")
            
            # Convert to Python data structures
            categories_data = []
            for row in category_breakdown.iter_rows(named=True):
                categories_data.append({
                    "category": row["category"],
                    "total_expenses": row["total_expenses"],
                    "total_income": row["total_income"],
                    "expense_count": row["expense_count"],
                    "income_count": row["income_count"],
                    "avg_expense": row["avg_expense"],
                    "avg_income": row["avg_income"]
                })
            
            weekly_data = []
            for row in weekly_patterns.iter_rows(named=True):
                weekly_data.append({
                    "day_of_week": row["day_of_week"],
                    "transaction_count": row["transaction_count"],
                    "total_amount": row["total_amount"],
                    "avg_amount": row["avg_amount"]
                })
            
            # Get spending trends
            spending_trends = self.analytics.execute_analytics_query("spending_trends")
            trends_data = []
            for row in spending_trends.iter_rows(named=True):
                trends_data.append({
                    "year": row["year"],
                    "month": row["month"],
                    "expenses": row["expenses"],
                    "change": row["change"],
                    "pct_change": row["pct_change"]
                })
            
            return {
                "total_balance": float(total_balance) if total_balance else 0.0,
                "monthly_income": float(monthly_income) if monthly_income else 0.0,
                "monthly_expenses": float(monthly_expenses) if monthly_expenses else 0.0,
                "savings_rate": ((float(monthly_income) - float(monthly_expenses)) / float(monthly_income) * 100 
                                if monthly_income and float(monthly_income) > 0 else 0.0),
                "category_breakdown": categories_data[:10],  # Top 10 categories
                "weekly_patterns": weekly_data,
                "spending_trends": trends_data,
                "analytics_timestamp": datetime.now().isoformat(),
                "data_points": {
                    "total_transactions": int(self.conn.execute("SELECT COUNT(*) FROM gold.transactions").fetchone()[0]),
                    "categories_count": len(categories_data),
                    "months_of_data": len(trends_data)
                }
            }
            
        except Exception as e:
            print(f"Error in enhanced financial summary: {e}")
            # Fallback to basic summary
            return self._get_basic_financial_summary()
    
    def _get_basic_financial_summary(self) -> Dict[str, Any]:
        """Basic fallback summary"""
        query = """
        SELECT 
            SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
            ABS(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END)) as expenses,
            SUM(amount) as balance
        FROM gold.transactions
        WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE)
        AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE)
        """
        
        try:
            result = self.conn.execute(query).fetchone()
            income, expenses, balance = result or (0, 0, 0)
            
            return {
                "total_balance": float(balance) if balance else 0.0,
                "monthly_income": float(income) if income else 0.0,
                "monthly_expenses": float(expenses) if expenses else 0.0,
                "category_breakdown": [],
                "weekly_patterns": [],
                "spending_trends": [],
                "analytics_timestamp": datetime.now().isoformat(),
                "data_points": {"total_transactions": 0, "categories_count": 0, "months_of_data": 0}
            }
        except:
            return {
                "total_balance": 0.0,
                "monthly_income": 0.0,
                "monthly_expenses": 0.0,
                "savings_rate": 0.0,
                "category_breakdown": [],
                "weekly_patterns": [],
                "spending_trends": [],
                "analytics_timestamp": datetime.now().isoformat(),
                "data_points": {"total_transactions": 0, "categories_count": 0, "months_of_data": 0}
            }
    
    def get_recent_transactions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent transactions with enhanced data"""
        try:
            query = f"""
            SELECT 
                t.*,
                a.running_balance,
                a.cumulative_expenses,
                a.cumulative_income
            FROM gold.transactions t
            LEFT JOIN running_analytics a ON t.id = a.id
            ORDER BY t.date DESC, t.created_at DESC
            LIMIT {limit}
            """
            
            transactions = self.conn.execute(query).fetchall()
            columns = [desc[0] for desc in self.conn.description]
            
            return [
                {
                    col: (
                        val.isoformat() if isinstance(val, (datetime, date)) 
                        else float(val) if isinstance(val, (int, float)) and col in ['amount', 'running_balance', 'cumulative_expenses', 'cumulative_income']
                        else val
                    )
                    for col, val in zip(columns, row)
                }
                for row in transactions
            ]
        except Exception as e:
            print(f"Error getting enhanced transactions: {e}")
            # Fallback to basic query
            return self._get_basic_recent_transactions(limit)
    
    def _get_basic_recent_transactions(self, limit: int) -> List[Dict[str, Any]]:
        """Basic fallback for transactions"""
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
        
        try:
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
        except:
            return []
    
    def get_spending_by_category(self, days: int = 30) -> Dict[str, float]:
        """Enhanced spending by category with trends"""
        try:
            # Get current period spending
            current_query = f"""
            SELECT 
                category,
                ABS(SUM(amount)) as current_spent
            FROM gold.transactions
            WHERE amount < 0 
            AND date >= CURRENT_DATE - INTERVAL '{days}' DAYS
            GROUP BY category
            """
            
            # Get previous period for comparison
            prev_query = f"""
            SELECT 
                category,
                ABS(SUM(amount)) as previous_spent
            FROM gold.transactions
            WHERE amount < 0 
            AND date >= CURRENT_DATE - INTERVAL '{days * 2}' DAYS
            AND date < CURRENT_DATE - INTERVAL '{days}' DAYS
            GROUP BY category
            """
            
            current_results = dict(self.conn.execute(current_query).fetchall())
            prev_results = dict(self.conn.execute(prev_query).fetchall())
            
            # Combine with trend data
            enhanced_results = {}
            for category, current in current_results.items():
                previous = prev_results.get(category, 0)
                change = current - previous if previous > 0 else 0
                pct_change = (change / previous * 100) if previous > 0 else 0
                
                enhanced_results[category] = {
                    "current": float(current),
                    "previous": float(previous),
                    "change": float(change),
                    "pct_change": float(pct_change)
                }
            
            return enhanced_results
            
        except Exception as e:
            print(f"Error in enhanced spending analysis: {e}")
            return self._get_basic_spending_by_category(days)
    
    def _get_basic_spending_by_category(self, days: int) -> Dict[str, float]:
        """Basic fallback for spending by category"""
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
            """
            
            results = self.conn.execute(query).fetchall()
            return {cat: float(amt) for cat, amt in results}
        except:
            return {}
    
    def get_historical_data(self, months: int = 6) -> List[Dict[str, Any]]:
        """Enhanced historical data with DuckDB window functions"""
        try:
            query = f"""
            WITH monthly_data AS (
                SELECT 
                    DATE_TRUNC('month', date) as month,
                    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
                    ABS(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END)) as expenses,
                    SUM(amount) as balance
                FROM gold.transactions
                WHERE date >= CURRENT_DATE - INTERVAL '{months}' months
                GROUP BY DATE_TRUNC('month', date)
            )
            SELECT
                month,
                income,
                expenses,
                balance,
                income + expenses as cash_flow,
                ROUND(income / NULLIF(income + expenses, 0) * 100, 2) as savings_rate,
                LAG(balance) OVER (ORDER BY month) as prev_balance,
                balance - LAG(balance) OVER (ORDER BY month) as balance_change
            FROM monthly_data
            ORDER BY month DESC
            """
            
            results = self.conn.execute(query).fetchall()
            return [
                {
                    "month": row[0].strftime("%Y-%m"),
                    "income": float(row[1]) if row[1] else 0.0,
                    "expenses": float(row[2]) if row[2] else 0.0,
                    "balance": float(row[3]) if row[3] else 0.0,
                    "cash_flow": float(row[4]) if row[4] else 0.0,
                    "savings_rate": float(row[5]) if row[5] else 0.0,
                    "balance_change": float(row[7]) if row[7] else 0.0
                }
                for row in results
            ]
            
        except Exception as e:
            print(f"Error in enhanced historical data: {e}")
            return self._get_basic_historical_data(months)
    
    def _get_basic_historical_data(self, months: int) -> List[Dict[str, Any]]:
        """Basic fallback for historical data"""
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
        
        try:
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
        except:
            return []
    
    def execute_custom_analytics(self, query_name: str) -> Dict[str, Any]:
        """Execute predefined analytics queries"""
        try:
            result_df = self.analytics.execute_analytics_query(query_name)
            
            # Convert to Python data structures
            data = []
            for row in result_df.iter_rows(named=True):
                data.append(dict(row))
            
            return {
                "query": query_name,
                "data": data,
                "row_count": len(data),
                "columns": list(result_df.columns) if len(data) > 0 else []
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
            views = self.conn.execute("SHOW VIEWS").fetchall()
            
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
            
            for view in views:
                view_name = view[0]
                stats[view_name] = {
                    "type": "view",
                    "row_count": "N/A"
                }
            
            return {
                "database": self.duckdb_path,
                "tables_count": len(tables),
                "views_count": len(views),
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

# Singleton instance with enhanced capabilities
data_loader = EnhancedDataLoader()
