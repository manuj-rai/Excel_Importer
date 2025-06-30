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
        """Returns list of tuples (column_name, data_type)"""
        self.cursor.execute(f"""
            SELECT c.name AS column_name, 
                   t.name AS data_type,
                   c.max_length,
                   c.precision,
                   c.scale
            FROM sys.columns c
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE c.object_id = OBJECT_ID(?)
        """, table_name)
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
    # Optimized cleaning using vectorized operations
    df = df.where(pd.notnull(df), None)
    
    # Only process phone columns if they exist
    phone_cols = [col for col in df.columns if "tel" in col.lower() or "phone" in col.lower()]
    for col in phone_cols:
        df[col] = df[col].astype(str).str.replace(r'(?i)^ph:\s*', '', regex=True).str.strip()
    
    # Apply stripping only to string columns
    str_cols = df.select_dtypes(include=['object']).columns
    for col in str_cols:
        df[col] = df[col].str.strip()
    
    return df


def map_custom_columns(df):
    # Only rename columns that exist in both
    rename_map = {k: v for k, v in EXCEL_TO_CUSTOM_MAP.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    
    # Only keep columns that exist in both
    keep_cols = [col for col in CUSTOM_COLUMNS if col in df.columns]
    return df[keep_cols].where(pd.notnull(df), None)


def read_file(file_path, preview_rows=None):
    ext = os.path.splitext(file_path)[1].lower()
    read_args = {}
    
    # Only read preview rows if specified
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
    confirmed = False  # Track if user clicked "Apply Mapping"
    table_columns = [col[0] for col in existing_columns]
    table_columns_with_types = [f"{col[0]} ({col[1]})" for col in existing_columns]

    mapping_window = tk.Toplevel(app)
    mapping_window.title("Map Columns")

    # Center the window
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
            mapping[file_col] = selected.split(" ")[0] if selected != default_value else None
        mapping_window.destroy()

    ttk.Button(container, text="Apply Mapping", command=on_done, width=20).pack(pady=10)

    mapping_window.wait_window()
    return mapping if confirmed else None  # <-- Only return mapping if user confirmed



def import_data():
    file_path = file_entry.get().strip()
    table_name = table_entry.get().strip()
    preview_count = int(preview_dropdown.get())

    if not file_path or not table_name:
        messagebox.showwarning("Missing info", "Please select a file and enter a table name.")
        return

    try:
        # Step 1: Read file with preview rows only
        status_label.config(text="⏳ Reading file...", foreground="blue")
        app.update()
        df = read_file(file_path, preview_rows=preview_count)
        
        # Step 2: Clean and sanitize
        df.columns = [sanitize_column_name(col) for col in df.columns]
        df = clean_dataframe(df)
        
        # Step 3: Update preview with just the preview rows
        update_preview(df)
        
        # Step 4: Connect to database
        importer = SQLImporter(SQL_CONN_STR)
        importer.connect()
        
        # Step 5: Check if table exists and get columns
        existing_columns = []
        if importer.table_exists(table_name):
            status_label.config(text="🔍 Fetching table columns...", foreground="blue")
            app.update()
            existing_columns = importer.get_existing_columns(table_name)
        
        # Step 6: Read the full file only if we need to
        if existing_columns or not importer.table_exists(table_name):
            status_label.config(text="⏳ Loading full dataset...", foreground="blue")
            app.update()
            full_df = read_file(file_path)
            full_df.columns = [sanitize_column_name(col) for col in full_df.columns]
            full_df = clean_dataframe(full_df)
            
            if existing_columns:
                status_label.config(text="📐 Mapping columns...", foreground="blue")
                app.update()
                mapping = map_columns(full_df.columns.tolist(), existing_columns)
                if mapping is None:
                    status_label.config(text="❌ Import canceled by user", foreground="red")
                    messagebox.showinfo("Canceled", "Column mapping canceled. Import aborted.")
                    return  # Stop further execution

                logging.info(f"Columns mapped: {mapping}")
                full_df = full_df.rename(columns={k: v for k, v in mapping.items() if v is not None})
                # Remove columns mapped to None
                full_df = full_df.drop(columns=[k for k, v in mapping.items() if v is None], errors="ignore")

            if USE_CUSTOM_COLUMNS:
                full_df = map_custom_columns(full_df)

            if not importer.table_exists(table_name):
                status_label.config(text="🛠 Creating table...", foreground="blue")
                app.update()
                importer.create_table(table_name, full_df)
            
            status_label.config(text="💾 Inserting data...", foreground="blue")
            app.update()
            importer.insert_data(table_name, full_df)

        importer.conn.commit()
        status_label.config(text=f"✅ Imported {len(full_df)} rows into '{table_name}'", foreground="green")
        messagebox.showinfo("Success", f"✅ Imported {len(full_df)} rows into '{table_name}'")
        
        # Update preview with the full dataset's head
        update_preview(full_df.head(preview_count))

    except Exception as e:
        logging.error(f"Import failed: {traceback.format_exc()}")
        status_label.config(text=f"❌ Import failed: {e}", foreground="red")
        messagebox.showerror("Import failed", f"❌ Error: {e}")
    finally:
        if 'importer' in locals():
            importer.close()


def update_preview(df):
    for widget in preview_frame.winfo_children():
        widget.destroy()

    if df.empty:
        tk.Label(preview_frame, text="No data to preview", fg="gray", font=("Arial", 10)).pack(expand=True)
        return

    # Create frame for table and scrollbars
    table_container = ttk.Frame(preview_frame)
    table_container.pack(expand=True, fill='both', padx=5, pady=5)
    
    # Add scrollbars
    vsb = ttk.Scrollbar(table_container, orient="vertical")
    hsb = ttk.Scrollbar(table_container, orient="horizontal")

    # Create treeview
    preview_table = ttk.Treeview(
        table_container, 
        yscrollcommand=vsb.set, 
        xscrollcommand=hsb.set,
        height=min(20, len(df)),
        show='headings'
    )
    vsb.config(command=preview_table.yview)
    hsb.config(command=preview_table.xview)

    # Grid layout
    preview_table.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")

    # Configure grid weights
    table_container.grid_rowconfigure(0, weight=1)
    table_container.grid_columnconfigure(0, weight=1)

    # Set columns
    preview_table["columns"] = list(df.columns)
    
    # Add headers
    for col in df.columns:
        preview_table.heading(col, text=col)
        preview_table.column(col, width=150, stretch=True, anchor='w')

    # Add data rows
    for _, row in df.iterrows():
        preview_table.insert("", "end", values=list(row))

    # Add record count
    count_label = ttk.Label(preview_frame, text=f"Showing {len(df)} records", 
                           font=("Arial", 9), anchor="e")
    count_label.pack(fill='x', padx=5, pady=(0, 5))


# === GUI ===
app = tk.Tk()
app.title("Fibre2Fashion Excel/CSV to SQL Importer")
app.configure(bg="#f5f5f5")

# Center the window on the screen
window_width = 1200
window_height = 800
screen_width = app.winfo_screenwidth()
screen_height = app.winfo_screenheight()
position_top = int(screen_height / 2 - window_height / 2)
position_right = int(screen_width / 2 - window_width / 2)
app.geometry(f'{window_width}x{window_height}+{position_right}+{position_top}')

# Configure style
style = ttk.Style()
style.theme_use("clam")  # More modern theme
style.configure(".", background="#f5f5f5", font=("Arial", 10))
style.configure("Header.TLabel", font=("Arial", 12, "bold"))
style.configure("Accent.TButton", font=("Arial", 11, "bold"), foreground="white", background="#4CAF50")
style.map("Accent.TButton", background=[('active', '#45a049')])
style.configure("Treeview", font=("Arial", 9), rowheight=25)

# Create main container
main_frame = ttk.Frame(app, padding="20")
main_frame.pack(fill='both', expand=True)

# Header with logo
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
    # Fallback text
    ttk.Label(header_frame, text="Fibre2Fashion", font=("Arial", 16, "bold")).pack(side='left')

ttk.Label(header_frame, text="Excel/CSV to SQL Importer", 
         font=("Arial", 16), style="Header.TLabel").pack(side='left')

# Status bar
status_frame = ttk.Frame(main_frame)
status_frame.pack(fill='x', pady=(0, 10))
status_label = ttk.Label(
    status_frame, 
    text="Ready", 
    font=("Arial", 10),
    anchor='w'
)
status_label.pack(fill='x')

# Input section
input_frame = ttk.LabelFrame(main_frame, text="Import Settings", padding=10)
input_frame.pack(fill='x', pady=10)

# File selection
file_frame = ttk.Frame(input_frame)
file_frame.pack(fill='x', pady=5)
ttk.Label(file_frame, text="File:").pack(side='left', padx=(0, 10))
file_entry = ttk.Entry(file_frame, width=60)
file_entry.pack(side='left', fill='x', expand=True, padx=(0, 10))
ttk.Button(file_frame, text="Browse", command=browse_file).pack(side='left', padx=5)

# Table name
table_frame = ttk.Frame(input_frame)
table_frame.pack(fill='x', pady=5)
ttk.Label(table_frame, text="Table Name:").pack(side='left', padx=(0, 10))
table_entry = ttk.Entry(table_frame, width=30)
table_entry.pack(side='left')

# Preview settings
preview_frame = ttk.Frame(input_frame)
preview_frame.pack(fill='x', pady=5)
ttk.Label(preview_frame, text="Preview Rows:").pack(side='left', padx=(0, 10))
preview_dropdown = ttk.Combobox(preview_frame, values=[10, 25, 50, 100], width=8, state='readonly')
preview_dropdown.set(10)
preview_dropdown.pack(side='left')

# Import button
button_frame = ttk.Frame(main_frame)
button_frame.pack(fill='x', pady=10)
import_btn = ttk.Button(button_frame, text="Import to SQL Server", command=import_data, style="Accent.TButton")
import_btn.pack(pady=10)

# Preview section
preview_frame = ttk.LabelFrame(main_frame, text="Data Preview", padding=10)
preview_frame.pack(fill='both', expand=True, pady=10)

# Initialize with empty preview
update_preview(pd.DataFrame())

app.mainloop()