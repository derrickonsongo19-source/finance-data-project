from fastapi import FastAPI
from datetime import date
import random

app = FastAPI(title="Mock Banking API")

# Sample transaction data
transactions = [
    {"id": 1, "date": "2025-01-20", "amount": -65.50, "description": "Grocery Store", "account": "Checking"},
    {"id": 2, "date": "2025-01-20", "amount": -15.99, "description": "Netflix", "account": "Credit Card"},
    {"id": 3, "date": "2025-01-19", "amount": 2500.00, "description": "Salary Deposit", "account": "Savings"},
]

@app.get("/")
def read_root():
    return {"message": "Mock Banking API is running"}

@app.get("/transactions")
def get_transactions():
    return {"transactions": transactions}

@app.get("/balance/{account}")
def get_balance(account: str):
    balance = sum(t["amount"] for t in transactions if t["account"] == account)
    return {"account": account, "balance": balance}

@app.get("/random-transaction")
def random_transaction():
    new_transaction = {
        "id": len(transactions) + 1,
        "date": str(date.today()),
        "amount": round(random.uniform(-100, 2000), 2),
        "description": random.choice(["Restaurant", "Fuel", "Online Shopping", "Transfer"]),
        "account": random.choice(["Checking", "Savings", "Credit Card"])
    }
    transactions.append(new_transaction)
    return {"new_transaction": new_transaction, "total_transactions": len(transactions)}
