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
import xlrd

# === LOAD CONFIG ===
with open("config.json", "r") as f:
    config = json.load(f)
SQL_CONN_STR = config["SQL_CONN_STR"]


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

    def full_table_name(self, full_name):
        if '.' in full_name:
            schema, table = full_name.split('.', 1)
        else:
            schema, table = 'dbo', full_name
        return f"[{schema}].[{table}]"

    def table_exists(self, table_name):
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema, table = 'dbo', table_name
        self.cursor.execute("""
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """, schema, table)
        return self.cursor.fetchone() is not None

    def get_existing_columns(self, table_name):
        self.cursor.execute(f"""
            SELECT c.name AS column_name, 
                   t.name AS data_type,
                   c.max_length,
                   c.precision,
                   c.scale
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE c.object_id = OBJECT_ID(?)
        """, self.full_table_name(table_name))
        columns = []
        for row in self.cursor.fetchall():
            data_type = row.data_type
            if data_type in {'varchar', 'nvarchar', 'char', 'nchar', 'binary', 'varbinary'}:
                if row.max_length == -1:
                    data_type = f"{data_type}(MAX)"
                else:
                    data_type = f"{data_type}({row.max_length})"
            elif data_type in {'decimal', 'numeric'}:
                data_type = f"{data_type}({row.precision},{row.scale})"
            columns.append((row.column_name, data_type))
        return columns

    def create_table(self, table_name, df):
        if not re.match(r'^[a-zA-Z0-9_.]+$', table_name):
            raise ValueError("Invalid table name. Use only alphanumeric characters, underscores, or dot.")
        column_defs = ",\n    ".join([f"[{col}] {map_dtype_to_sql(col, df[col])}" for col in df.columns])
        create_sql = f"CREATE TABLE {self.full_table_name(table_name)} (\n    {column_defs}\n);"
        logging.info(f"Creating table:\n{create_sql}")
        self.cursor.execute(create_sql)
        self.conn.commit()

    def drop_table(self, table_name):
        self.cursor.execute(f"DROP TABLE {self.full_table_name(table_name)}")
        self.conn.commit()

    def insert_data(self, table_name, df):
        columns = ", ".join(f"[{col}]" for col in df.columns)
        placeholders = ", ".join("?" for _ in df.columns)
        insert_sql = f"INSERT INTO {self.full_table_name(table_name)} ({columns}) VALUES ({placeholders})"

        # Sanitize every value so pyodbc receives only safe Python-native types.
        # Root cause: pandas Timestamp, datetime64[us], NaT, and float('nan') all
        # cause "Numeric value out of range" or silent corruption in pyodbc.
        def sanitize(val):
            # pandas Timestamp -> Python datetime
            if isinstance(val, pd.Timestamp):
                return val.to_pydatetime()
            # NaT -> None
            if val is pd.NaT:
                return None
            # float NaN -> None  (covers str-dtype NaN from values.tolist())
            if isinstance(val, float) and val != val:
                return None
            # numpy bool -> Python bool
            try:
                import numpy as np
                if isinstance(val, (np.integer,)):
                    return int(val)
                if isinstance(val, (np.floating,)):
                    return None if (val != val) else float(val)
                if isinstance(val, np.bool_):
                    return bool(val)
            except ImportError:
                pass
            return val

        rows = [
            [sanitize(v) for v in row]
            for row in df.itertuples(index=False, name=None)
        ]

        try:
            self.cursor.fast_executemany = True
            self.cursor.executemany(insert_sql, rows)
            self.conn.commit()
        except Exception as e:
            logging.error(f"Batch insert failed: {e}. Trying row-by-row...")
            self.conn.rollback()
            for i, row in enumerate(rows):
                try:
                    self.cursor.execute(insert_sql, row)
                    self.conn.commit()
                except Exception as row_error:
                    self.conn.rollback()
                    logging.error(f"Row {i} failed: {dict(zip(df.columns, row))} | Error: {row_error}")
            raise e


def sanitize_column_name(col):
    return re.sub(r'[^a-zA-Z0-9_]', '', col.strip().replace(" ", "_").lower())


def map_dtype_to_sql(col_name, col_data=None):
    return "NVARCHAR(MAX)"


def clean_dataframe(df):
    # Step 1: Force all columns to object dtype FIRST so that where() replaces
    # NaN/NaT with Python None correctly across all pandas versions (including 3+
    # where str-dtype columns don't respond to the old where() approach).
    df = df.astype(object).where(pd.notnull(df), None)

    # Step 2: Convert any remaining Timestamps / datetime64 values to Python datetime.
    # Even after astype(object), datetime columns may hold pd.Timestamp objects.
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: x.to_pydatetime() if isinstance(x, pd.Timestamp) else x
        )

    # Step 3: Phone column cleanup
    phone_cols = [col for col in df.columns if "tel" in col.lower() or "phone" in col.lower()]
    for col in phone_cols:
        df[col] = df[col].apply(
            lambda x: re.sub(r'(?i)^ph:\s*', '', str(x)).strip() if isinstance(x, str) else x
        )

    # Step 4: Strip whitespace from all string values
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    return df


def read_file(file_path, preview_rows=None):
    ext = os.path.splitext(file_path)[1].lower()
    read_args = {}
    if preview_rows:
        read_args['nrows'] = preview_rows
    if ext == ".csv":
        return pd.read_csv(file_path, **read_args)
    elif ext == ".xlsx":
        return pd.read_excel(file_path, engine='openpyxl', **read_args)
    elif ext == ".xls":
        return pd.read_excel(file_path, engine='xlrd', **read_args)
    else:
        raise ValueError("Unsupported file format. Use .csv, .xlsx, or .xls")


def browse_file():
    file_path = filedialog.askopenfilename(filetypes=[("CSV & Excel files", "*.csv *.xlsx *.xls")])
    file_entry.delete(0, tk.END)
    file_entry.insert(0, file_path)
    if file_path:
        status_label.config(text=f"✅ Selected file: {os.path.basename(file_path)}", foreground="green")


def map_columns(file_columns, existing_columns):
    mapping = {}
    confirmed = False
    table_columns_with_types = [f"{col[0]} ({col[1]})" for col in existing_columns]

    mapping_window = tk.Toplevel(app)
    mapping_window.title("Map Columns")

    mapping_window.update_idletasks()
    win_width = 800
    win_height = 600
    screen_width = mapping_window.winfo_screenwidth()
    screen_height = mapping_window.winfo_screenheight()
    x = (screen_width // 2) - (win_width // 2)
    y = (screen_height // 2) - (win_height // 2)
    mapping_window.geometry(f"{win_width}x{win_height}+{x}+{y}")
    mapping_window.transient(app)
    mapping_window.grab_set()

    container = ttk.Frame(mapping_window)
    container.pack(fill='both', expand=True, padx=10, pady=10)

    canvas = tk.Canvas(container)
    scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
    scrollable_frame = ttk.Frame(canvas)

    scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set)
    canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")

    default_value = "Skip Column"
    ttk.Label(scrollable_frame, text="File Column", font=("Arial", 10, "bold")).grid(row=0, column=0, sticky="ew")
    ttk.Label(scrollable_frame, text="Table Column (Data Type)", font=("Arial", 10, "bold")).grid(row=0, column=1, sticky="ew")

    comboboxes = []
    for row, file_col in enumerate(file_columns):
        ttk.Label(scrollable_frame, text=file_col, font=("Arial", 10)).grid(row=row + 1, column=0, sticky="w")
        cb = ttk.Combobox(
            scrollable_frame,
            values=[default_value] + table_columns_with_types,
            state="readonly",
            width=40,
            font=("Arial", 10)
        )
        cb.set(default_value)
        cb.grid(row=row + 1, column=1, padx=5, pady=2, sticky="ew")
        comboboxes.append((file_col, cb))

    def on_done():
        nonlocal confirmed
        confirmed = True
        for file_col, cb in comboboxes:
            selected = cb.get()
            if selected != default_value:
                selected_col = selected.rsplit(" (", 1)[0]
                mapping[file_col] = selected_col
            else:
                mapping[file_col] = None
        mapping_window.destroy()

    ttk.Button(container, text="Apply Mapping", command=on_done, width=20).pack(pady=10)
    mapping_window.wait_window()
    return mapping if confirmed else None


def import_data():
    file_path = file_entry.get().strip()
    table_name = table_entry.get().strip()
    preview_count = int(preview_dropdown.get())

    if not file_path or not table_name:
        messagebox.showwarning("Missing info", "Please select a file and enter a table name.")
        return

    importer = SQLImporter(SQL_CONN_STR)

    try:
        # Step 1: Read preview rows for display
        status_label.config(text="⏳ Reading file...", foreground="blue")
        app.update()
        df = read_file(file_path, preview_rows=preview_count)
        df.columns = [sanitize_column_name(col) for col in df.columns]
        df = clean_dataframe(df)
        update_preview(df)

        # Step 2: Connect
        importer.connect()

        # Step 3: Check table existence
        existing_columns = []
        if importer.table_exists(table_name):
            status_label.config(text="🔍 Fetching table columns...", foreground="blue")
            app.update()
            existing_columns = importer.get_existing_columns(table_name)

        # Step 4: Load full dataset
        status_label.config(text="⏳ Loading full dataset...", foreground="blue")
        app.update()
        full_df = read_file(file_path)
        full_df.columns = [sanitize_column_name(col) for col in full_df.columns]
        full_df = clean_dataframe(full_df)

        # Step 5: Column mapping (existing table only)
        if existing_columns:
            status_label.config(text="📐 Mapping columns...", foreground="blue")
            app.update()
            mapping = map_columns(full_df.columns.tolist(), existing_columns)
            if mapping is None:
                status_label.config(text="❌ Import canceled by user", foreground="red")
                messagebox.showinfo("Canceled", "Column mapping canceled. Import aborted.")
                return

            logging.info(f"Columns mapped: {mapping}")
            full_df = full_df.rename(columns={k: v for k, v in mapping.items() if v is not None})
            full_df = full_df.drop(columns=[k for k, v in mapping.items() if v is None], errors="ignore")

        # Step 6: Create table if needed
        if not importer.table_exists(table_name):
            status_label.config(text="🛠 Creating table...", foreground="blue")
            app.update()
            importer.create_table(table_name, full_df)

        # Step 7: Insert
        status_label.config(text="💾 Inserting data...", foreground="blue")
        app.update()
        importer.insert_data(table_name, full_df)

        status_label.config(text=f"✅ Imported {len(full_df)} rows into '{table_name}'", foreground="green")
        messagebox.showinfo("Success", f"✅ Imported {len(full_df)} rows into '{table_name}'")
        update_preview(full_df.head(preview_count))

    except Exception as e:
        logging.error(f"Import failed: {traceback.format_exc()}")
        status_label.config(text=f"❌ Import failed: {e}", foreground="red")
        messagebox.showerror("Import failed", f"❌ Error: {e}")
    finally:
        importer.close()


def update_preview(df):
    for widget in preview_frame.winfo_children():
        widget.destroy()

    if df.empty:
        tk.Label(preview_frame, text="No data to preview", fg="gray", font=("Arial", 10)).pack(expand=True)
        return

    table_container = ttk.Frame(preview_frame)
    table_container.pack(expand=True, fill='both', padx=5, pady=5)

    vsb = ttk.Scrollbar(table_container, orient="vertical")
    hsb = ttk.Scrollbar(table_container, orient="horizontal")

    preview_table = ttk.Treeview(
        table_container,
        yscrollcommand=vsb.set,
        xscrollcommand=hsb.set,
        height=min(20, len(df)),
        show='headings'
    )
    vsb.config(command=preview_table.yview)
    hsb.config(command=preview_table.xview)

    preview_table.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")

    table_container.grid_rowconfigure(0, weight=1)
    table_container.grid_columnconfigure(0, weight=1)

    preview_table["columns"] = list(df.columns)
    for col in df.columns:
        preview_table.heading(col, text=col)
        preview_table.column(col, width=150, stretch=True, anchor='w')

    for _, row in df.iterrows():
        preview_table.insert("", "end", values=list(row))

    ttk.Label(preview_frame, text=f"Showing {len(df)} records",
              font=("Arial", 9), anchor="e").pack(fill='x', padx=5, pady=(0, 5))


# === GUI ===
app = tk.Tk()
app.title("Fibre2Fashion Excel/CSV to SQL Importer")
app.configure(bg="#f5f5f5")

window_width = 1200
window_height = 800
screen_width = app.winfo_screenwidth()
screen_height = app.winfo_screenheight()
position_top = int(screen_height / 2 - window_height / 2)
position_right = int(screen_width / 2 - window_width / 2)
app.geometry(f'{window_width}x{window_height}+{position_right}+{position_top}')

style = ttk.Style()
style.theme_use("clam")
style.configure(".", background="#f5f5f5", font=("Arial", 10))
style.configure("Header.TLabel", font=("Arial", 12, "bold"))
style.configure("Accent.TButton", font=("Arial", 11, "bold"), foreground="white", background="#4CAF50")
style.map("Accent.TButton", background=[('active', '#45a049')])
style.configure("Treeview", font=("Arial", 9), rowheight=25)

main_frame = ttk.Frame(app, padding="20")
main_frame.pack(fill='both', expand=True)

header_frame = ttk.Frame(main_frame)
header_frame.pack(fill='x', pady=(0, 20))

try:
    logo_img = Image.open("f2f-logo.png")
    logo_img = logo_img.resize((120, 60), Image.LANCZOS)
    logo = ImageTk.PhotoImage(logo_img)
    logo_label = ttk.Label(header_frame, image=logo)
    logo_label.image = logo
    logo_label.pack(side='left', padx=(0, 15))
except Exception as e:
    logging.warning(f"Logo load failed: {e}")
    ttk.Label(header_frame, text="Fibre2Fashion", font=("Arial", 16, "bold")).pack(side='left')

ttk.Label(header_frame, text="Excel/CSV to SQL Importer",
          font=("Arial", 16), style="Header.TLabel").pack(side='left')

status_frame = ttk.Frame(main_frame)
status_frame.pack(fill='x', pady=(0, 10))
status_label = ttk.Label(status_frame, text="Ready", font=("Arial", 10), anchor='w')
status_label.pack(fill='x')

input_frame = ttk.LabelFrame(main_frame, text="Import Settings", padding=10)
input_frame.pack(fill='x', pady=10)

file_frame = ttk.Frame(input_frame)
file_frame.pack(fill='x', pady=5)
ttk.Label(file_frame, text="File:").pack(side='left', padx=(0, 10))
file_entry = ttk.Entry(file_frame, width=60)
file_entry.pack(side='left', fill='x', expand=True, padx=(0, 10))
ttk.Button(file_frame, text="Browse", command=browse_file).pack(side='left', padx=5)

table_frame = ttk.Frame(input_frame)
table_frame.pack(fill='x', pady=5)
ttk.Label(table_frame, text="Table Name:").pack(side='left', padx=(0, 10))
table_entry = ttk.Entry(table_frame, width=30)
table_entry.pack(side='left')

preview_settings_frame = ttk.Frame(input_frame)
preview_settings_frame.pack(fill='x', pady=5)
ttk.Label(preview_settings_frame, text="Preview Rows:").pack(side='left', padx=(0, 10))
preview_dropdown = ttk.Combobox(preview_settings_frame, values=[10, 25, 50, 100], width=8, state='readonly')
preview_dropdown.set(10)
preview_dropdown.pack(side='left')

button_frame = ttk.Frame(main_frame)
button_frame.pack(fill='x', pady=10)
import_btn = ttk.Button(button_frame, text="Import to SQL Server", command=import_data, style="Accent.TButton")
import_btn.pack(pady=10)

preview_frame = ttk.LabelFrame(main_frame, text="Data Preview", padding=10)
preview_frame.pack(fill='both', expand=True, pady=10)

update_preview(pd.DataFrame())

app.mainloop()