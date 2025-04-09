import pandas as pd
import pyodbc
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os

# === CONFIGURATION ===
SQL_CONN_STR = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=192.168.0.53;'
    'DATABASE=portal;'
    'UID=sanblueuat;'
    'PWD=Admin^portal'
)

# === TOGGLE: Use custom schema or dynamic NVARCHAR(MAX)
USE_CUSTOM_COLUMNS = False  # Set to False for dynamic schema

# === Custom SQL column definitions (used only if USE_CUSTOM_COLUMNS is True)
CUSTOM_COLUMNS = {
    "contact_person": "NVARCHAR(255)",
    "company": "NVARCHAR(255)",
    "email": "NVARCHAR(255)",
    "phone": "NVARCHAR(50)",
    "city": "NVARCHAR(100)",
    "zip": "NVARCHAR(20)"
}

# === Optional: Mapping from Excel column names to custom schema
EXCEL_TO_CUSTOM_MAP = {
    "person": "contact_person",
    "companyname": "company",
    "emailid": "email",
    "tel": "phone",
    "location": "city",
    "pincode": "zip"
}

# === UTILITIES ===
def sanitize_column_name(col):
    return col.strip().replace(" ", "").replace(".", "_").replace(";", "_").replace("-", "_").replace(":", "").lower()

def map_dtype_to_sql(col_name, col_data=None):
    return "NVARCHAR(MAX)"  # Always fallback to NVARCHAR(MAX)

def clean_dataframe(df):
    df = df.where(pd.notnull(df), None)
    for col in df.columns:
        if "tel" in col.lower() or "phone" in col.lower():
            df[col] = df[col].astype(str).str.replace(r'(?i)^ph:\s*', '', regex=True).str.strip()
    df = df.applymap(lambda x: str(x).strip() if pd.notnull(x) else None)
    return df

def map_custom_columns(df):
    df = df.rename(columns=EXCEL_TO_CUSTOM_MAP)
    df = df[[col for col in CUSTOM_COLUMNS if col in df.columns]]
    df = df.where(pd.notnull(df), None)
    df = df.applymap(lambda x: str(x).strip() if pd.notnull(x) else None)
    return df

def read_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(file_path)
    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(file_path, engine='openpyxl')
    else:
        raise ValueError("Unsupported file format. Use .csv or .xlsx")

def create_table_force(cursor, conn, table_name, df):
    cursor.execute(f"IF OBJECT_ID(N'{table_name}', N'U') IS NOT NULL DROP TABLE [{table_name}]")
    conn.commit()

    if USE_CUSTOM_COLUMNS:
        print("\n📊 Using custom schema:")
        for col, sql_type in CUSTOM_COLUMNS.items():
            print(f" - {col}: {sql_type}")
        column_defs = ",\n    ".join([f"[{col}] {sql_type}" for col, sql_type in CUSTOM_COLUMNS.items()])
    else:
        print("\n📊 Using dynamic NVARCHAR(MAX) columns:")
        for col in df.columns:
            print(f" - {col}: NVARCHAR(MAX)")
        column_defs = ",\n    ".join([f"[{col}] {map_dtype_to_sql(col)}" for col in df.columns])

    create_sql = f"CREATE TABLE [{table_name}] (\n    {column_defs}\n);"
    print("\n📝 CREATE TABLE SQL:\n", create_sql)
    cursor.execute(create_sql)
    conn.commit()

def insert_data(cursor, table_name, df):
    columns = ", ".join(f"[{col}]" for col in df.columns)
    placeholders = ", ".join("?" for _ in df.columns)
    insert_sql = f"INSERT INTO [{table_name}] ({columns}) VALUES ({placeholders})"

    try:
        print(f"\n🚀 Inserting {len(df)} rows using batch...")
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, df.values.tolist())
    except Exception as e:
        print("❌ Batch insert failed. Falling back to row-by-row insert...")
        for i, row in df.iterrows():
            try:
                cursor.execute(insert_sql, tuple(row))
            except Exception as row_error:
                print(f"❌ Row {i} failed: {row.to_dict()} | Error: {row_error}")
        raise e

# === GUI FUNCTIONS ===
def browse_file():
    file_path = filedialog.askopenfilename(filetypes=[("CSV & Excel files", "*.csv *.xlsx *.xls")])
    file_entry.delete(0, tk.END)
    file_entry.insert(0, file_path)

def import_data():
    file_path = file_entry.get().strip()
    table_name = table_entry.get().strip()

    if not file_path or not table_name:
        messagebox.showwarning("Missing info", "Please select a file and enter a table name.")
        return

    try:
        df = read_file(file_path)
        df.columns = [sanitize_column_name(col) for col in df.columns]
        df = clean_dataframe(df)

        if USE_CUSTOM_COLUMNS:
            df = map_custom_columns(df)

        global conn
        conn = pyodbc.connect(SQL_CONN_STR)
        cursor = conn.cursor()

        create_table_force(cursor, conn, table_name, df)
        insert_data(cursor, table_name, df)
        conn.commit()

        messagebox.showinfo("Success", f"✅ Imported {len(df)} rows into '{table_name}'")
        update_preview(df)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        messagebox.showerror("Import failed", f"❌ Error: {e}")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

def update_preview(df):
    for widget in preview_frame.winfo_children():
        widget.destroy()

    preview_table = ttk.Treeview(preview_frame)
    preview_table.pack(expand=True, fill='both')

    preview_table["columns"] = list(df.columns)
    preview_table["show"] = "headings"

    for col in df.columns:
        preview_table.heading(col, text=col)
        preview_table.column(col, width=150)

    for _, row in df.head(10).iterrows():
        preview_table.insert("", "end", values=list(row))

# === GUI SETUP ===
app = tk.Tk()
app.title("🧠 Excel/CSV to SQL Importer - Custom + Fallback")
app.geometry("1000x600")

tk.Label(app, text="📂 Select File").pack(anchor='w', padx=10, pady=(10, 0))
file_frame = tk.Frame(app)
file_frame.pack(fill='x', padx=10)
file_entry = tk.Entry(file_frame)
file_entry.pack(side='left', expand=True, fill='x')
tk.Button(file_frame, text="Browse", command=browse_file).pack(side='left', padx=5)

tk.Label(app, text="📝 SQL Table Name").pack(anchor='w', padx=10, pady=(10, 0))
table_entry = tk.Entry(app)
table_entry.pack(fill='x', padx=10)

tk.Button(app, text="🚀 Import to SQL Server", bg="#4CAF50", fg="white", command=import_data).pack(pady=10)

tk.Label(app, text="🔍 Preview First 10 Rows").pack(anchor='w', padx=10)
preview_frame = tk.Frame(app)
preview_frame.pack(expand=True, fill='both', padx=10, pady=5)

app.mainloop()
