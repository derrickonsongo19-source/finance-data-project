# api/tests/test_api.py
"""
API Tests Module
Unit tests for the Finance Analytics API endpoints.
"""
import pytest
from fastapi.testclient import TestClient
from datetime import datetime, date
import json
import sys
import os

# Add parent directory to path to import api modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.main import app

# Create test client
client = TestClient(app)


def test_root_endpoint():
    """Test the root endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert data["message"] == "Personal Finance Analytics API"
    assert "endpoints" in data


def test_health_check():
    """Test the health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert data["service"] == "finance-api"


def test_financial_summary():
    """Test the financial summary endpoint"""
    response = client.get("/summary")
    assert response.status_code == 200
    
    data = response.json()
    
    # Check required fields
    assert "total_balance" in data
    assert "monthly_income" in data
    assert "monthly_expenses" in data
    assert "top_categories" in data
    assert "last_updated" in data
    
    # Check data types
    assert isinstance(data["total_balance"], (int, float))
    assert isinstance(data["monthly_income"], (int, float))
    assert isinstance(data["monthly_expenses"], (int, float))
    assert isinstance(data["top_categories"], list)


def test_transactions_endpoint():
    """Test the transactions endpoint"""
    # Test with default limit
    response = client.get("/transactions")
    assert response.status_code == 200
    
    transactions = response.json()
    assert isinstance(transactions, list)
    
    # Test with custom limit
    response = client.get("/transactions?limit=5")
    assert response.status_code == 200
    
    limited_transactions = response.json()
    assert isinstance(limited_transactions, list)
    
    # If we have transactions, check their structure
    if transactions:
        transaction = transactions[0]
        assert "id" in transaction
        assert "date" in transaction
        assert "amount" in transaction
        assert "description" in transaction
        assert "category" in transaction
        assert "account" in transaction


def test_spending_by_category():
    """Test the spending by category endpoint"""
    # Test with default days
    response = client.get("/spending/category")
    assert response.status_code == 200
    
    data = response.json()
    assert "period_days" in data
    assert "spending_by_category" in data
    assert "total_spent" in data
    
    # Test with custom days
    response = client.get("/spending/category?days=60")
    assert response.status_code == 200
    
    data = response.json()
    assert data["period_days"] == 60


def test_spending_prediction():
    """Test the spending prediction endpoint"""
    response = client.get("/predict/spending")
    assert response.status_code == 200
    
    predictions = response.json()
    assert isinstance(predictions, list)
    
    # Check prediction structure if predictions exist
    if predictions:
        prediction = predictions[0]
        assert "month" in prediction
        assert "predicted_amount" in prediction
        assert "category" in prediction
        assert "confidence" in prediction
        
        assert isinstance(prediction["predicted_amount"], (int, float))
        assert isinstance(prediction["confidence"], (int, float))
        assert 0 <= prediction["confidence"] <= 1


def test_budget_recommendations():
    """Test the budget recommendations endpoint"""
    response = client.get("/recommend/budget")
    assert response.status_code == 200
    
    recommendations = response.json()
    assert isinstance(recommendations, list)
    
    # Check recommendation structure if recommendations exist
    if recommendations:
        recommendation = recommendations[0]
        assert "category" in recommendation
        assert "current_spending" in recommendation
        assert "recommended_limit" in recommendation
        assert "percentage_change" in recommendation
        assert "reason" in recommendation


def test_historical_data():
    """Test the historical data endpoint"""
    # Test with default months
    response = client.get("/historical")
    assert response.status_code == 200
    
    data = response.json()
    assert "period_months" in data
    assert "data" in data
    
    # Test with custom months
    response = client.get("/historical?months=3")
    assert response.status_code == 200
    
    data = response.json()
    assert data["period_months"] == 3


def test_invalid_endpoint():
    """Test non-existent endpoint returns 404"""
    response = client.get("/nonexistent")
    assert response.status_code == 404


def test_transaction_limit_validation():
    """Test transaction limit parameter validation"""
    # Test with valid limit
    response = client.get("/transactions?limit=20")
    assert response.status_code == 200
    
    # Test with negative limit (should still work, but might return empty)
    response = client.get("/transactions?limit=-5")
    # API might handle this differently, but shouldn't crash
    assert response.status_code in [200, 422]


def test_spending_category_days_validation():
    """Test days parameter validation for spending by category"""
    # Test with valid days
    response = client.get("/spending/category?days=90")
    assert response.status_code == 200
    
    # Test with zero days (might return empty data)
    response = client.get("/spending/category?days=0")
    assert response.status_code == 200


# Run tests from command line
if __name__ == "__main__":
    print("Running API tests...")
    
    # Run all test functions
    test_functions = [
        test_root_endpoint,
        test_health_check,
        test_financial_summary,
        test_transactions_endpoint,
        test_spending_by_category,
        test_spending_prediction,
        test_budget_recommendations,
        test_historical_data,
        test_invalid_endpoint,
    ]
    
    passed = 0
    failed = 0
    
    for test_func in test_functions:
        try:
            test_func()
            print(f"✓ {test_func.__name__}: PASSED")
            passed += 1
        except Exception as e:
            print(f"✗ {test_func.__name__}: FAILED - {str(e)}")
            failed += 1
    
    print(f"\nTest Results: {passed} passed, {failed} failed")
    
    if failed > 0:
        sys.exit(1)
