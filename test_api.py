# test_api.py
from api.data_loader import DataLoader
import os

print('Testing database connection...')
loader = DataLoader()

# Test getting financial summary
summary = loader.get_financial_summary()
print(f'Financial Summary:')
print(f'  Total Balance: ${summary["total_balance"]:.2f}')
print(f'  Monthly Income: ${summary["monthly_income"]:.2f}')
print(f'  Monthly Expenses: ${summary["monthly_expenses"]:.2f}')
print(f'  Top Categories: {summary["top_categories"]}')

# Test getting recent transactions
transactions = loader.get_recent_transactions(5)
print(f'\nRecent Transactions ({len(transactions)} found):')
for t in transactions:
    print(f'  - {t["date"]}: ${t["amount"]:.2f} ({t["category"]})')

print('\n✓ Database connection test successful!')
loader.close()
