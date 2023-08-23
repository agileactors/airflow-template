from dotenv import dotenv_values
from sqlalchemy import create_engine

config = dotenv_values(".env.integrationtesting")

engine = create_engine(url=config["EXTERNAL_PG_DB_URL"])


sql_script_path = "tests/resources/sql_scripts/setup_pg_db.sql"

with open(f"{sql_script_path}") as f:
    setup_database_query: str = "".join(f.readlines())
    engine.execute(setup_database_query)

print("PG database created successfully")