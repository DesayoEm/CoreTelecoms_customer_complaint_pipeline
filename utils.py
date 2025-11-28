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
        engine.dispose()
        return

    confirm = input("\ndelete ALL tables?: ")
    if confirm.lower() != "yes":
        print("Aborted.")
        engine.dispose()
        return

    for table in tables:
        try:
            with engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
                print(f"Dropped {table}")
        except Exception as e:
            print(f"Error dropping {table}: {e}")

    print(f"Finished processing {len(tables)} tables")
    engine.dispose()


if __name__ == "__main__":
    list_and_delete_all_tables(config.SILVER_DB_CONN_STRING, False)
#
# with engine.begin() as conn:
#     result = conn.execute(text("SELECT COUNT(*) FROM staging_conformed_customers"))
#     rows = result.fetchall()
#
# for row in rows:
#     print(row)
