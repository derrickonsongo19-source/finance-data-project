#!/usr/bin/env python3
"""
Test Enhanced DuckDB Analytics
"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=== Testing Enhanced DuckDB Analytics ===")

try:
    from api.data_loader import EnhancedDataLoader
    
    print("1. Initializing EnhancedDataLoader...")
    loader = EnhancedDataLoader()
    
    print("\n2. Testing database statistics...")
    stats = loader.get_database_stats()
    print(f"   Database: {stats.get('database', 'N/A')}")
    print(f"   Tables: {stats.get('tables_count', 0)}")
    print(f"   Views: {stats.get('views_count', 0)}")
    
    print("\n3. Testing enhanced financial summary...")
    summary = loader.get_financial_summary()
    print(f"   Total balance: ${summary.get('total_balance', 0):.2f}")
    print(f"   Monthly income: ${summary.get('monthly_income', 0):.2f}")
    print(f"   Monthly expenses: ${summary.get('monthly_expenses', 0):.2f}")
    print(f"   Categories analyzed: {len(summary.get('category_breakdown', []))}")
    print(f"   Spending trends: {len(summary.get('spending_trends', []))} months")
    
    print("\n4. Testing analytics queries...")
    queries = ["current_month_detailed", "category_breakdown"]
    for query in queries:
        try:
            result = loader.execute_custom_analytics(query)
            print(f"   {query}: {result.get('row_count', 0)} rows")
        except Exception as e:
            print(f"   {query}: Error - {e}")
    
    print("\n5. Testing enhanced spending analysis...")
    spending = loader.get_spending_by_category(30)
    print(f"   Categories with trend data: {len(spending)}")
    if spending:
        sample_cat = next(iter(spending))
        sample_data = spending[sample_cat]
        if isinstance(sample_data, dict):
            print(f"   Sample category '{sample_cat}':")
            print(f"     Current: ${sample_data.get('current', 0):.2f}")
            print(f"     Previous: ${sample_data.get('previous', 0):.2f}")
            print(f"     Change: {sample_data.get('pct_change', 0):.1f}%")
    
    loader.close()
    print("\n✅ Enhanced DuckDB analytics test completed!")
    
except Exception as e:
    print(f"\n❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
    print("\n⚠️  Note: This test requires the database to be populated with data.")
    print("   Run your bronze/silver/gold layers first to populate data.")
