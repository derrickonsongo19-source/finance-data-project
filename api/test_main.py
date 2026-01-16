from fastapi import FastAPI
import duckdb
import os

app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "finance-api"}

@app.get("/transactions")
async def get_transactions():
    try:
        conn = duckdb.connect()
        result = conn.execute("SELECT * FROM 'data/silver/transactions_clean.parquet'").fetchall()
        conn.close()
        return {
            "count": len(result),
            "transactions": result[:10],  # First 10 only
            "status": "success"
        }
    except Exception as e:
        return {"error": str(e), "status": "error"}

@app.get("/summary")
async def get_summary():
    try:
        conn = duckdb.connect()
        result = conn.execute("""
            SELECT 
                category,
                COUNT(*) as count,
                SUM(amount) as total,
                AVG(amount) as average
            FROM 'data/silver/transactions_clean.parquet'
            GROUP BY category
        """).fetchall()
        conn.close()
        return {
            "summary": result,
            "status": "success"
        }
    except Exception as e:
        return {"error": str(e), "status": "error"}

if __name__ == "__main__":
    import uvicorn
    print("Starting FastAPI server on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
