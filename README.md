# CSV/Excel to SQL Importer

## Description
This project as for the the csv/excel file and table name to insert data into SQL table.
If table already exist, its ask whether you want to delete table and recreate or append the table.
Its sanitizes data and column also offers custum table schema.

## Setup Instructions

```bash
python -m venv env
source env/bin/activate  # or .\env\Scripts\activate on Windows
pip install -r requirements.txt
