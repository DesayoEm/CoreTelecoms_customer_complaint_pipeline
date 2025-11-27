from sqlalchemy import create_engine, text, inspect
from dags.include.config import config


def list_and_delete_all_tables(conn_string: str, dry_run: bool = True):
    engine = create_engine(conn_string)

    inspector = inspect(engine)
    tables = inspector.get_table_names(schema="public")

    print(f"Found {len(tables)} tables:")
    for table in tables:
        print(f"  - {table}")

    if dry_run:
        print("\nDry run mode. Set dry_run=False to actually delete.")
        return

    confirm = input("\nAre you sure you want to delete ALL tables? (yes/no): ")
    if confirm.lower() != "yes":
        print("Aborted.")
        return

    with engine.begin() as conn:
        for table in tables:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
            print(f"Dropped {table}")

    print(f"Deleted {len(tables)} tables")


engine = create_engine(config.SILVER_DB_CONN_STRING)
#
# with engine.begin() as conn:
#     result = conn.execute(text("SELECT * FROM conformed_customers LIMIT 5"))
#     rows = result.fetchall()
#
# for row in rows:
#     print(row)


# with engine.begin() as conn:
#     result = conn.execute(text("SELECT COUNT(*) FROM conformed_agents"))
#     rows = result.fetchall()
#
# for row in rows:
#     print(row)


if __name__ == "__main__":
    list_and_delete_all_tables(config.SILVER_DB_CONN_STRING, True)
