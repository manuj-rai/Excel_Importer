# 📊 Excel/CSV to SQL Importer

A lightweight desktop tool to import `.csv`, `.xlsx`, and `.xls` files directly into Microsoft SQL Server. Designed for ease of use by both technical and non-technical users, with features like data preview, table overwrite or append options, and automatic type mapping.

---

## ✨ Features

- ✅ Import `.csv`, `.xlsx`, `.xls` files
- ✅ Connects to SQL Server using ODBC
- ✅ Automatically detects data types (INT, FLOAT, DATETIME, TEXT)
- ✅ Option to **append** or **overwrite** existing tables
- ✅ Live preview of imported data (first N rows)
- ✅ Clean GUI with branding
- ✅ Logging enabled for troubleshooting

---

## 📦 Supported Formats

| Format | Extension | Parser Used  |
|--------|-----------|--------------|
| CSV    | `.csv`    | `pandas`     |
| Excel  | `.xlsx`   | `openpyxl`   |
| Excel  | `.xls`    | `xlrd`       |

---

## 🔧 Requirements

- Python 3.8+
- ODBC Driver 17 or higher for SQL Server  
  ➤ [Download ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

---

## ⚙️ Setup & Installation

### 1. Clone the Repository

```bash
git clone https://github.com/manuj-rai/Imp_Excel_to_SQL.git
cd Imp_Excel_to_SQL

### 2. Set Up a Virtual Environment

python -m venv env
env\Scripts\activate         # On Windows
source env/bin/activate      # On macOS/Linux

### 3. Install Python Dependencies

pip install -r requirements.txt

### 🔐 Configuration

Create a config.json
{
  "SQL_CONN_STR": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server;DATABASE=your_db;UID=your_user;PWD=your_password"
}

### 🤝 Maintainers

Manuj Rai
📧 [imanujrai7@gmail.com] (update if applicable)
