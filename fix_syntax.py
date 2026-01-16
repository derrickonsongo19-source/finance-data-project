with open('api/data_loader.py', 'r') as f:
    lines = f.readlines()

# Look for the problematic line
for i, line in enumerate(lines):
    if 'print("✅ Analytics views created successfully")def __init__' in line:
        # Fix this line
        lines[i] = '        print("✅ Analytics views created successfully")\n'
        break

with open('api/data_loader.py', 'w') as f:
    f.writelines(lines)

print("Syntax error fixed")
