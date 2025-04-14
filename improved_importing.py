import pandas as pd
import pyodbc
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import re
import logging
import traceback
import json
from PIL import Image, ImageTk

# === LOAD CONFIG ===
with open("config.json", "r") as f:
    config = json.load(f)
SQL_CONN_STR = config["SQL_CONN_STR"]

USE_CUSTOM_COLUMNS = False

CUSTOM_COLUMNS = {
    "contact_person": "NVARCHAR(255)",
    "company": "NVARCHAR(255)",
    "email": "NVARCHAR(255)",
    "phone": "NVARCHAR(50)",
    "city": "NVARCHAR(100)",
    "zip": "NVARCHAR(20)"
}

EXCEL_TO_CUSTOM_MAP = {
    "person": "contact_person",
    "companyname": "company",
    "emailid": "email",
    "tel": "phone",
    "location": "city",
    "pincode": "zip"
}

logging.basicConfig(filename='importer.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


class SQLImporter:
    def __init__(self, conn_str):
        self.conn_str = conn_str
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = pyodbc.connect(self.conn_str)
        self.cursor = self.conn.cursor()

    def close(self):
        if self.cursor: self.cursor.close()
        if self.conn: self.conn.close()

    def table_exists(self, table_name):
        self.cursor.execute(f"SELECT OBJECT_ID(N'{table_name}', N'U')")
        return self.cursor.fetchone()[0] is not None

    def get_existing_columns(self, table_name):
        self.cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", table_name)
        return [row[0] for row in self.cursor.fetchall()]

    def create_table(self, table_name, df):
        if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
            raise ValueError("Invalid table name. Use only alphanumeric characters and underscores.")
        column_defs = ",\n    ".join([f"[{col}] {map_dtype_to_sql(col, df[col])}" for col in df.columns])
        create_sql = f"CREATE TABLE [{table_name}] (\n    {column_defs}\n);"
        logging.info(f"Creating table:\n{create_sql}")
        self.cursor.execute(create_sql)
        self.conn.commit()

    def drop_table(self, table_name):
        self.cursor.execute(f"DROP TABLE [{table_name}]")
        self.conn.commit()

    def insert_data(self, table_name, df):
        columns = ", ".join(f"[{col}]" for col in df.columns)
        placeholders = ", ".join("?" for _ in df.columns)
        insert_sql = f"INSERT INTO [{table_name}] ({columns}) VALUES ({placeholders})"
        try:
            self.cursor.fast_executemany = True
            self.cursor.executemany(insert_sql, df.values.tolist())
        except Exception as e:
            logging.error(f"Batch insert failed: {e}. Trying row-by-row...")
            for i, row in df.iterrows():
                try:
                    self.cursor.execute(insert_sql, tuple(row))
                except Exception as row_error:
                    logging.error(f"Row {i} failed: {row.to_dict()} | Error: {row_error}")
            raise e


def sanitize_column_name(col):
    return re.sub(r'[^a-zA-Z0-9_]', '', col.strip().replace(" ", "_").lower())


def map_dtype_to_sql(col_name, col_data=None):
    if col_data is not None:
        if pd.api.types.is_integer_dtype(col_data):
            return "BIGINT"
        elif pd.api.types.is_float_dtype(col_data):
            return "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            return "DATETIME"
    return "NVARCHAR(MAX)"


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


def browse_file():
    file_path = filedialog.askopenfilename(filetypes=[("CSV & Excel files", "*.csv *.xlsx *.xls")])
    file_entry.delete(0, tk.END)
    file_entry.insert(0, file_path)


def import_data():
    file_path = file_entry.get().strip()
    table_name = table_entry.get().strip()
    preview_count = int(preview_dropdown.get())

    if not file_path or not table_name:
        messagebox.showwarning("Missing info", "Please select a file and enter a table name.")
        return

    try:
        df = read_file(file_path)
        df.columns = [sanitize_column_name(col) for col in df.columns]
        df = clean_dataframe(df)
        if USE_CUSTOM_COLUMNS:
            df = map_custom_columns(df)

        importer = SQLImporter(SQL_CONN_STR)
        importer.connect()

        if importer.table_exists(table_name):
            choice = messagebox.askyesnocancel("Table Exists", f"Table '{table_name}' already exists.\nYes = Drop and recreate\nNo = Append\nCancel = Abort")
            if choice is None:
                return
            elif choice:
                importer.drop_table(table_name)
                importer.create_table(table_name, df)
            else:
                existing_cols = importer.get_existing_columns(table_name)
                df = df[[col for col in df.columns if col in existing_cols]]
        else:
            importer.create_table(table_name, df)

        importer.insert_data(table_name, df)
        importer.conn.commit()
        messagebox.showinfo("Success", f"✅ Imported {len(df)} rows into '{table_name}'")
        update_preview(df.head(preview_count))

    except Exception as e:
        logging.error(f"Import failed: {traceback.format_exc()}")
        messagebox.showerror("Import failed", f"❌ Error: {e}")
    finally:
        importer.close()


def update_preview(df):
    for widget in preview_frame.winfo_children():
        widget.destroy()

    vsb = tk.Scrollbar(preview_frame, orient="vertical")
    hsb = tk.Scrollbar(preview_frame, orient="horizontal")

    preview_table = ttk.Treeview(preview_frame, yscrollcommand=vsb.set, xscrollcommand=hsb.set)
    vsb.config(command=preview_table.yview)
    hsb.config(command=preview_table.xview)
    vsb.pack(side='right', fill='y')
    hsb.pack(side='bottom', fill='x')
    preview_table.pack(expand=True, fill='both')

    preview_table["columns"] = list(df.columns)
    preview_table["show"] = "headings"

    for col in df.columns:
        preview_table.heading(col, text=col)
        preview_table.column(col, width=150, stretch=True)

    for _, row in df.iterrows():
        preview_table.insert("", "end", values=list(row))


# === GUI ===
app = tk.Tk()
app.title("Fibre2Fashion Excel/CSV to SQL Importer")
app.geometry("1100x700")

# Logo and title header
header_frame = tk.Frame(app, bg="white")
header_frame.pack(fill='x', padx=10, pady=(10, 5))

try:
    logo_img = Image.open("f2f-logo.png")
    logo_img = logo_img.resize((100, 50))
    logo = ImageTk.PhotoImage(logo_img)
    tk.Label(header_frame, image=logo, bg="white").pack(side='left', padx=5)
except Exception as e:
    logging.warning(f"Logo load failed: {e}")

tk.Label(header_frame, text="Fibre2Fashion Excel/CSV to SQL Importer", bg="white",
         font=("Segoe UI", 16, "bold")).pack(side='left', padx=10)

status_label = tk.Label(app, text="")
status_label.pack(anchor='w', padx=10, pady=(5, 0))

tk.Label(app, text="📂 Select File").pack(anchor='w', padx=10, pady=(10, 0))
file_frame = tk.Frame(app)
file_frame.pack(fill='x', padx=10)
file_entry = tk.Entry(file_frame)
file_entry.pack(side='left', expand=True, fill='x')
tk.Button(file_frame, text="Browse", command=browse_file).pack(side='left', padx=5)

tk.Label(app, text="📝 SQL Table Name").pack(anchor='w', padx=10, pady=(10, 0))
table_entry = tk.Entry(app)
table_entry.pack(fill='x', padx=10)

tk.Label(app, text="🔍 Preview Rows").pack(anchor='w', padx=10, pady=(10, 0))
preview_dropdown = ttk.Combobox(app, values=[10, 25, 50, 100], state='readonly')
preview_dropdown.set(10)
preview_dropdown.pack(fill='x', padx=10)

tk.Button(app, text="🚀 Import to SQL Server", width=20, bg="#4CAF50", fg="white", command=import_data).pack(pady=10)

preview_frame = tk.Frame(app)
preview_frame.pack(expand=True, fill='both', padx=10, pady=5)

app.mainloop()
