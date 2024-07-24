import sqlite3
import json
import sys

def export_tables_to_jsonl(sqlite_file):
    conn = sqlite3.connect(sqlite_file)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns_info = cursor.fetchall()
        primary_key = None
        for column_info in columns_info:
            if column_info[5] == 1:  # PK column
                primary_key = column_info[1]
                break
        
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        
        for row in rows:
            row_dict = dict(zip(column_names, row))
            row_dict = {f"chembl:{key}": value for key, value in row_dict.items()}
            if primary_key:
                row_dict["id"] = row_dict["chembl:"+primary_key]
            row_dict["grebi:type"] = f"chembl:{table_name}"
            print(json.dumps(row_dict))
    
    conn.close()

export_tables_to_jsonl('chembl_34/chembl_34_sqlite/chembl_34.db')


