#!/bin/sh

# Run the Python script to populate availability
echo "\n"
echo "************** Running populate_availability.py... \n"
uv run ./scripts/database_setup/populate_availability.py

# Run the Python script to backfill titles
echo "\n"
echo "************** Running backfill_titles.py... \n"
uv run ./scripts/database_setup/backfill_titles.py

# Run the Postgres SQL script for database post-processing
echo "\n"
echo "************** Running db_postprocessing.sql... \n"
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -a -f ./scripts/database_setup/db_postprocessing.sql

# Run the Python script to populate ratings
echo "\n"
echo "************** Running populate_ratings.py... \n"
uv run ./scripts/database_setup/populate_ratings.py

echo "\n"
echo "All commands executed successfully!"
