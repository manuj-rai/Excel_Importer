import pandas as pd
import pyodbc

# === 1. Load CSV ===
csv_path = r'C:\Users\it9\Downloads\intertextile_vietnam_cleaned_merged.csv'  # ⬅️ Update path if needed

df = pd.read_csv(csv_path)
df = df.where(pd.notnull(df), None)  # Replace NaN with None
df = df.astype(str)
df = df.replace({'nan': None, 'NaN': None, 'None': None})
df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

# === 2. Sanitize column names for SQL compatibility
original_columns = df.columns.tolist()
safe_columns = [
    col.strip()
    .replace(" ", "_")
    .replace(".", "_")
    .replace(";", "_")
    .replace("-", "_")
    .replace(":", "")
    .lower()
    for col in original_columns
]

# Rename the DataFrame with safe column names
df.columns = safe_columns

# === 3. Connect to SQL Server ===
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=192.168.0.53;'
    'DATABASE=portal;'
    'UID=sanblueuat;'
    'PWD=Admin^portal'
)
cursor = conn.cursor()

# === 4. Create SQL Table ===
table_name = 'intertextile_vietnam'
column_defs = ",\n    ".join([f"[{col}] TEXT" for col in safe_columns])
create_table_sql = f"""
CREATE TABLE [{table_name}] (
    {column_defs}
);
"""
# Only create table if it doesn't already exist
cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL BEGIN {create_table_sql} END")
conn.commit()

# === 5. Insert Data ===
column_names = ", ".join([f"[{col}]" for col in safe_columns])
placeholders = ", ".join(["?"] * len(safe_columns))
insert_sql = f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})"

cursor.executemany(insert_sql, df.values.tolist())
conn.commit()

# === 6. Cleanup ===
cursor.close()
conn.close()

print("✅ intertextile_vietnam combined data imported successfully into SQL Server.")

