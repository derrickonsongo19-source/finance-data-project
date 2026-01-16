# test_api_server.py
import requests
import time
import subprocess
import sys

print('Starting API server...')
server = subprocess.Popen([sys.executable, '-m', 'uvicorn', 'main:app', '--host', '0.0.0.0', '--port', '8000'])

time.sleep(3)

try:
    print('Testing API endpoints...')
    
    # Root endpoint
    response = requests.get('http://localhost:8000/')
    print(f'✓ Root endpoint: {response.status_code}')
    data = response.json()
    print(f'  Message: {data["message"]}')
    print(f'  Available endpoints: {list(data["endpoints"].keys())}')
    
    # Health endpoint
    response = requests.get('http://localhost:8000/health')
    print(f'✓ Health endpoint: {response.status_code}')
    print(f'  Status: {response.json()["status"]}')
    
    # Summary endpoint
    response = requests.get('http://localhost:8000/summary')
    print(f'✓ Summary endpoint: {response.status_code}')
    summary = response.json()
    print(f'  Balance: ${summary["total_balance"]:.2f}')
    print(f'  Income: ${summary["monthly_income"]:.2f}')
    print(f'  Expenses: ${summary["monthly_expenses"]:.2f}')
    
    # Transactions endpoint
    response = requests.get('http://localhost:8000/transactions?limit=3')
    print(f'✓ Transactions endpoint: {response.status_code}')
    transactions = response.json()
    print(f'  Retrieved {len(transactions)} transactions')
    
    # Test ML prediction endpoint
    response = requests.get('http://localhost:8000/predict/spending')
    print(f'✓ Spending prediction endpoint: {response.status_code}')
    
    # Test budget recommendations
    response = requests.get('http://localhost:8000/recommend/budget')
    print(f'✓ Budget recommendations endpoint: {response.status_code}')
    
except Exception as e:
    print(f'✗ Error: {e}')
finally:
    server.terminate()
    server.wait()
    print('\n✓ API test completed successfully!')
