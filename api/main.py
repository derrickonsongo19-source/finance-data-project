# api/main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import os
from dotenv import load_dotenv
from datetime import datetime
import uvicorn

# Import models and data loader
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
    title="Personal Finance Analytics API",
    description="Real-time personal finance analytics with ML insights",
    version="1.0.0"
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
    """Root endpoint"""
    return {
        "message": "Personal Finance Analytics API",
        "version": "1.0.0",
        "endpoints": {
            "summary": "/summary",
            "transactions": "/transactions",
            "spending_prediction": "/predict/spending",
            "budget_recommendations": "/recommend/budget",
            "health": "/health"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "finance-api"
    }

@app.get("/summary", response_model=FinancialSummary)
async def get_financial_summary():
    """Get current financial summary"""
    try:
        summary_data = data_loader.get_financial_summary()
        return FinancialSummary(**summary_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching summary: {str(e)}")

@app.get("/transactions", response_model=List[Transaction])
async def get_recent_transactions(limit: int = 10):
    """Get recent transactions"""
    try:
        transactions = data_loader.get_recent_transactions(limit)
        return transactions
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching transactions: {str(e)}")

@app.get("/spending/category")
async def get_spending_by_category(days: int = 30):
    """Get spending by category"""
    try:
        spending_data = data_loader.get_spending_by_category(days)
        return {
            "period_days": days,
            "spending_by_category": spending_data,
            "total_spent": sum(spending_data.values())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching spending data: {str(e)}")

@app.get("/predict/spending", response_model=List[SpendingPrediction])
async def predict_spending():
    """Predict next month's spending by category"""
    try:
        # Get historical spending data
        spending_data = data_loader.get_spending_by_category(90)  # Last 90 days
        
        # Use ML predictor to make predictions
        predictions = ml_predictor.predict_spending(spending_data)
        
        return predictions
    except Exception as e:
        # Fallback to simple predictions if ML fails
        print(f"ML prediction error, using fallback: {e}")
        spending_data = data_loader.get_spending_by_category(30)
        
        # Simple prediction: average of last month
        predictions = []
        for category, amount in spending_data.items():
            predictions.append(
                SpendingPrediction(
                    month=datetime.now().strftime("%Y-%m"),
                    predicted_amount=amount,
                    category=category,
                    confidence=0.7  # Lower confidence for fallback
                )
            )
        
        return predictions

@app.get("/recommend/budget", response_model=List[BudgetRecommendation])
async def get_budget_recommendations():
    """Get budget recommendations based on spending patterns"""
    try:
        # Get current month spending
        current_spending = data_loader.get_spending_by_category(30)
        
        # Get historical data for comparison
        historical_data = data_loader.get_historical_data(3)
        
        # Generate recommendations
        recommendations = []
        
        for category, amount in current_spending.items():
            if amount > 0:  # Only for categories with spending
                # Simple rule-based recommendations
                if amount > 500:  # If spending > $500
                    recommended = amount * 0.8  # Suggest 20% reduction
                    reason = "High spending detected"
                elif amount < 50:  # If spending < $50
                    recommended = amount * 1.2  # Allow 20% increase
                    reason = "Low spending, can increase budget"
                else:
                    recommended = amount  # Keep same
                    reason = "Spending within normal range"
                
                percentage_change = ((recommended - amount) / amount * 100) if amount > 0 else 0
                
                recommendations.append(
                    BudgetRecommendation(
                        category=category,
                        current_spending=amount,
                        recommended_limit=recommended,
                        percentage_change=percentage_change,
                        reason=reason
                    )
                )
        
        return recommendations
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating recommendations: {str(e)}")

@app.get("/historical")
async def get_historical_data(months: int = 6):
    """Get historical financial data"""
    try:
        historical = data_loader.get_historical_data(months)
        return {
            "period_months": months,
            "data": historical
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching historical data: {str(e)}")

@app.on_event("shutdown")
def shutdown_event():
    """Cleanup on shutdown"""
    data_loader.close()

if __name__ == "__main__":
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", 8000))
    debug = os.getenv("DEBUG", "true").lower() == "true"
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=debug,
        log_level="info" if debug else "warning"
    )
