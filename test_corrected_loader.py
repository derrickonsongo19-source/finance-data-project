#!/usr/bin/env python3
"""
Test Corrected DuckDB Data Loader
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=== Testing Corrected DuckDB Data Loader ===")

try:
    # Import the corrected loader
    import importlib.util
    spec = importlib.util.spec_from_file_location("data_loader_corrected", "api/data_loader_corrected.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    loader = module.data_loader
    
    print("1. Testing database statistics...")
    stats = loader.get_database_stats()
    print(f"   Database: {stats.get('database', 'N/A')}")
    print(f"   Tables: {stats.get('tables_count', 0)}")
    
    print("\n2. Testing financial summary...")
    summary = loader.get_financial_summary()
    print(f"   Total balance: ${summary.get('total_balance', 0):.2f}")
    print(f"   Monthly income: ${summary.get('monthly_income', 0):.2f}")
    print(f"   Monthly expenses: ${summary.get('monthly_expenses', 0):.2f}")
    print(f"   Categories analyzed: {len(summary.get('category_breakdown', []))}")
    
    print("\n3. Testing analytics queries...")
    queries = ["current_month_detailed", "category_breakdown"]
    for query in queries:
        result = loader.execute_custom_analytics(query)
        if "error" in result:
            print(f"   {query}: Error - {result['error']}")
        else:
            print(f"   {query}: {result.get('row_count', 0)} rows")
    
    print("\n4. Testing recent transactions...")
    transactions = loader.get_recent_transactions(5)
    print(f"   Retrieved {len(transactions)} transactions")
    if transactions:
        print(f"   Sample: {transactions[0].get('category', 'N/A')} - ${transactions[0].get('amount', 0):.2f}")
    
    loader.close()
    print("\n✅ Corrected data loader test completed!")
    
except Exception as e:
    print(f"\n❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
