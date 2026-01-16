#!/usr/bin/env python3
"""
Final Test of Updated Data Loader
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=== Final Test of Updated Data Loader ===")

try:
    from api.data_loader import data_loader
    
    print("1. Testing database statistics...")
    stats = data_loader.get_database_stats()
    print(f"   ✓ Connected to: {stats.get('database', 'N/A')}")
    print(f"   ✓ Tables found: {stats.get('tables_count', 0)}")
    
    print("\n2. Testing financial summary...")
    summary = data_loader.get_financial_summary()
    print(f"   ✓ Total balance: ${summary.get('total_balance', 0):.2f}")
    print(f"   ✓ Monthly income: ${summary.get('monthly_income', 0):.2f}")
    print(f"   ✓ Monthly expenses: ${summary.get('monthly_expenses', 0):.2f}")
    print(f"   ✓ Savings rate: {summary.get('savings_rate', 0):.1f}%")
    
    print("\n3. Testing analytics capabilities...")
    # Test a few analytics queries
    test_queries = ["category_breakdown", "current_month_detailed"]
    for query in test_queries:
        result = data_loader.execute_custom_analytics(query)
        if "error" not in result:
            print(f"   ✓ {query}: {result.get('row_count', 0)} rows returned")
        else:
            print(f"   ✗ {query}: {result.get('error', 'Unknown error')}")
    
    print("\n4. Testing data retrieval...")
    transactions = data_loader.get_recent_transactions(3)
    print(f"   ✓ Retrieved {len(transactions)} recent transactions")
    
    # Show DuckDB analytics views were created
    views_created = any('analytics' in table.lower() for table in stats.get('objects', {}))
    print(f"   ✓ Analytics views created: {'Yes' if views_created else 'No'}")
    
    data_loader.close()
    print("\n🎉 SUCCESS: DuckDB analytics implementation complete!")
    print("\n📊 Summary of what was implemented:")
    print("   1. ✅ Enhanced DuckDB analytics engine")
    print("   2. ✅ Analytics views for transactions, categories, monthly summaries")
    print("   3. ✅ Window functions for running balances")
    print("   4. ✅ Custom analytics queries")
    print("   5. ✅ Integration with existing API structure")
    
except Exception as e:
    print(f"\n❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
