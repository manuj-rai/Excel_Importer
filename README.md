# 📊 Excel/CSV to SQL Importer

A lightweight desktop tool to import `.csv`, `.xlsx`, and `.xls` files directly into **Microsoft SQL Server**. Built with Python and Tkinter, this GUI tool is designed for ease of use by both technical and non-technical users, providing **live preview**, **type detection**, **column mapping**, and **overwrite or append** options.
---

## ✨ Features

- ✅ Import `.csv`, `.xlsx`, `.xls` files
- ✅ Connects to SQL Server using ODBC
- ✅ Automatically maps data types (`INT`, `FLOAT`, `DATETIME`, `NVARCHAR(MAX)`)
- ✅ Choose to **append** to or **overwrite** existing tables
- ✅ Column mapping UI for existing SQL tables
- ✅ Live data preview (first N rows)
- ✅ Automatic column sanitization (removes spaces, special characters)
- ✅ Optimized for batch insert (`fast_executemany`)
- ✅ Modern GUI with logo branding (Tkinter + Pillow)
- ✅ Error logging and fallback insert on failure

---

## 📦 Supported Formats

| Format | Extension | Parser Used  |
|--------|-----------|--------------|
| CSV    | `.csv`    | `pandas`     |
| Excel  | `.xlsx`   | `openpyxl`   |
| Excel  | `.xls`    | `xlrd`       |

---

## 🔧 Requirements

- Python **3.8+**
- **ODBC Driver 17** or higher for SQL Server  
  ➤ [Download here](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- See `requirements.txt` for Python libraries


---

## ⚙️ Setup & Installation

### 1. Clone the Repository

```bash
git clone https://github.com/manuj-rai/Imp_Excel_to_SQL.git
cd Imp_Excel_to_SQL
```

### 2. Set Up a Virtual Environment

```bash
python -m venv env
env\Scripts\activate         # On Windows
source env/bin/activate      # On macOS/Linux
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 4. 🔐 Configuration

```bash
Create a config.json
{
  "SQL_CONN_STR": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server;DATABASE=your_db;UID=your_user;PWD=your_password"
}
```

### 5. 🤝 Maintainer

**Manuj Rai**  
📧 [imanujrai7@gmail.com](mailto:imanujrai7@gmail.com)
