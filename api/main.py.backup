# api/main_enhanced.py - Enhanced with DuckDB Analytics
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv
from datetime import datetime
import uvicorn

# Import models and enhanced data loader
from models import (
    FinancialSummary, 
    SpendingPrediction, 
    BudgetRecommendation,
    Transaction
)
from data_loader import data_loader
from ml.predict import MLPredictor

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="Personal Finance Analytics API with DuckDB",
    description="Real-time personal finance analytics with DuckDB engine and ML insights",
    version="2.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize ML predictor
ml_predictor = MLPredictor()

@app.get("/")
async def root():
    """Root endpoint with enhanced analytics"""
    return {
        "message": "Personal Finance Analytics API with DuckDB",
        "version": "2.0.0",
        "engine": "DuckDB + Polars",
        "endpoints": {
            "summary": "/summary",
            "analytics": "/analytics/{query_name}",
            "transactions": "/transactions",
            "spending": "/spending/category",
            "predictions": "/predict/spending",
            "budget": "/recommend/budget",
            "historical": "/historical",
            "database": "/database/stats",
            "health": "/health"
        },
        "analytics_queries": [
            "current_month_detailed",
            "spending_trends", 
            "category_breakdown",
            "account_analysis"
        ]
    }

@app.get("/health")
async def health_check():
    """Enhanced health check with DuckDB status"""
    try:
        db_stats = data_loader.get_database_stats()
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service": "finance-api-duckdb",
            "database": {
                "connected": "error" not in db_stats,
                "tables": db_stats.get("tables_count", 0),
                "views": db_stats.get("views_count", 0)
            }
        }
    except Exception as e:
        return {
            "status": "degraded",
            "timestamp": datetime.now().isoformat(),
            "service": "finance-api-duckdb",
            "error": str(e)
        }

@app.get("/summary", response_model=FinancialSummary)
async def get_financial_summary():
    """Enhanced financial summary with DuckDB analytics"""
    try:
        summary_data = data_loader.get_financial_summary()
        return FinancialSummary(**summary_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching summary: {str(e)}")

@app.get("/analytics/{query_name}")
async def get_analytics(query_name: str):
    """Execute predefined DuckDB analytics queries"""
    try:
        result = data_loader.execute_custom_analytics(query_name)
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analytics error: {str(e)}")

@app.get("/transactions", response_model=List[Transaction])
async def get_recent_transactions(limit: int = Query(10, ge=1, le=100)):
    """Get recent transactions with running analytics"""
    try:
        transactions = data_loader.get_recent_transactions(limit)
        return transactions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching transactions: {str(e)}")

@app.get("/spending/category")
async def get_spending_by_category(days: int = Query(30, ge=1, le=365)):
    """Enhanced spending by category with trend analysis"""
    try:
        spending_data = data_loader.get_spending_by_category(days)
        
        # Calculate totals
        total_current = sum(item["current"] for item in spending_data.values() if isinstance(item, dict))
        total_previous = sum(item["previous"] for item in spending_data.values() if isinstance(item, dict))
        total_change = total_current - total_previous
        total_pct_change = (total_change / total_previous * 100) if total_previous > 0 else 0
        
        return {
            "period_days": days,
            "total_current": total_current,
            "total_previous": total_previous,
            "total_change": total_change,
            "total_pct_change": total_pct_change,
            "spending_by_category": spending_data,
            "analytics": {
                "categories_count": len(spending_data),
                "has_trend_data": all(isinstance(v, dict) for v in spending_data.values())
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching spending data: {str(e)}")

@app.get("/predict/spending", response_model=List[SpendingPrediction])
async def predict_spending():
    """Predict next month's spending by category - enhanced with DuckDB data"""
    try:
        # Get enhanced spending data with trends
        spending_data = data_loader.get_spending_by_category(90)
        
        # Use ML predictor with trend data
        predictions = ml_predictor.predict_spending(spending_data)
        
        # Add confidence based on data quality
        for prediction in predictions:
            category = prediction.category
            if category in spending_data and isinstance(spending_data[category], dict):
                # Higher confidence if we have trend data
                prediction.confidence = min(0.95, prediction.confidence + 0.1)
        
        return predictions
    except Exception as e:
        # Enhanced fallback with DuckDB data
        print(f"ML prediction error, using enhanced fallback: {e}")
        spending_data = data_loader.get_spending_by_category(30)
        
        predictions = []
        for category, data in spending_data.items():
            if isinstance(data, dict):
                current = data.get("current", 0)
                change = data.get("change", 0)
                # Simple prediction: current + average change
                predicted = current + (change / 2 if change else current * 0.1)
                confidence = 0.7
            else:
                # Basic fallback
                predicted = data
                confidence = 0.6
            
            predictions.append(
                SpendingPrediction(
                    month=datetime.now().strftime("%Y-%m"),
                    predicted_amount=predicted,
                    category=category,
                    confidence=confidence
                )
            )
        
        return predictions

@app.get("/recommend/budget", response_model=List[BudgetRecommendation])
async def get_budget_recommendations():
    """Enhanced budget recommendations with DuckDB analytics"""
    try:
        # Get enhanced spending data
        spending_data = data_loader.get_spending_by_category(30)
        
        # Get historical trends
        trends = data_loader.execute_custom_analytics("spending_trends")
        
        recommendations = []
        
        for category, data in spending_data.items():
            if isinstance(data, dict):
                current = data.get("current", 0)
                change = data.get("change", 0)
                pct_change = data.get("pct_change", 0)
                
                # Enhanced recommendation logic
                if current > 500 and pct_change > 20:
                    # High and increasing spending
                    recommended = current * 0.7  # 30% reduction
                    reason = "High spending with significant increase"
                    priority = "high"
                elif current > 300 and pct_change > 10:
                    # Moderately high and increasing
                    recommended = current * 0.85  # 15% reduction
                    reason = "Moderate spending increasing"
                    priority = "medium"
                elif current < 50 and pct_change < -20:
                    # Low and decreasing
                    recommended = current * 1.3  # 30% increase
                    reason = "Low spending decreasing further"
                    priority = "low"
                elif current < 100:
                    # Low spending
                    recommended = current * 1.1  # 10% increase
                    reason = "Low spending, room for increase"
                    priority = "low"
                else:
                    # Normal range
                    recommended = current
                    reason = "Spending within normal range"
                    priority = "normal"
                
                percentage_change = ((recommended - current) / current * 100) if current > 0 else 0
                
                recommendations.append(
                    BudgetRecommendation(
                        category=category,
                        current_spending=current,
                        recommended_limit=recommended,
                        percentage_change=percentage_change,
                        reason=reason,
                        priority=priority,
                        trend_pct_change=pct_change
                    )
                )
        
        return recommendations
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating recommendations: {str(e)}")

@app.get("/historical")
async def get_historical_data(months: int = Query(6, ge=1, le=60)):
    """Enhanced historical data with DuckDB analytics"""
    try:
        historical = data_loader.get_historical_data(months)
        return {
            "period_months": months,
            "data": historical,
            "analytics": {
                "has_trends": all("savings_rate" in item for item in historical),
                "months_returned": len(historical)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching historical data: {str(e)}")

@app.get("/database/stats")
async def get_database_statistics():
    """Get DuckDB database statistics"""
    try:
        stats = data_loader.get_database_stats()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching database stats: {str(e)}")

@app.on_event("shutdown")
def shutdown_event():
    """Cleanup on shutdown"""
    data_loader.close()

if __name__ == "__main__":
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", 8000))
    debug = os.getenv("DEBUG", "true").lower() == "true"
    
    uvicorn.run(
        "main_enhanced:app",
        host=host,
        port=port,
        reload=debug,
        log_level="info" if debug else "warning"
    )
