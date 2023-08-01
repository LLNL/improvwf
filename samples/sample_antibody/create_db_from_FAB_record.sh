#!/bin/bash
echo "Creating sqlite_db.sqlite from sample_FAB_record.yaml records"
python ../../improvwf/db_interface/db_and_yaml.py load -b ./sqlite_db.sqlite -H ./sample_FAB_record.yaml