#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <datasources_files.jsonl> <equivalences_db_path>"
    exit 1
fi