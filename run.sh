#!/bin/bash

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Running data extraction..."
python3 extract.py

echo "Running data transformation..."
spark-submit transform.py

echo "Running data loading..."
spark-submit load.py

echo "Script execution completed."