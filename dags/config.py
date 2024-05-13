import os
UNIVERSITY_API = os.getenv("universities", "http://universities.hipolabs.com/search?country=United%20States")
CSV_FILE_DIR = os.getenv("CSV_FILE_DIR", "datasets")

PSQL_DB = os.getenv("PSQL_DB")
PSQL_USER = os.getenv("PSQL_USER")
PSQL_PASSWORD = os.getenv("PSQL_PASSWORD")
PSQL_PORT = os.getenv("PSQL_PORT")
PSQL_HOST = os.getenv("PSQL_HOST")