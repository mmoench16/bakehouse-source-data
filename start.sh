#!/bin/bash
set -e

# Install dependencies
pip install -r scripts/requirements.txt

# Run the data generation script for the current month
python scripts/generate_monthly_data.py --month $(date '+%Y-%m')