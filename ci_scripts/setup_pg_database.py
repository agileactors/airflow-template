from sqlalchemy import create_engine


engine = create_engine(url="postgresql+psycopg2://postgres:postgres@localhost:5433/postgres")


sql_script_path = "tests/resources/sql_scripts/setup_pg_db.sql"

with open(f"{sql_script_path}") as f:
    setup_database_query: str = "".join(f.readlines())
    engine.execute(setup_database_query)

print("PG database created successfully")