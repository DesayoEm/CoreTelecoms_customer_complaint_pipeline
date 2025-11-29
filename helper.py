from sqlalchemy import create_engine, text, inspect
from dags.include.config import config

conn_string = config.SILVER_DB_CONN_STRING
engine = create_engine(config.SILVER_DB_CONN_STRING, isolation_level="AUTOCOMMIT")


def list_and_delete_all_tables(dry_run: bool = True):
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


def truncate_staging_tables():
    tables = [
        "staging_conformed_sm_complaints",
        "staging_conformed_web_complaints",
        "staging_conformed_call_logs",
        "staging_conformed_agents",
        "staging_conformed_customers",
    ]

    table_list = ", ".join(tables)

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_list} CASCADE"))
    engine.dispose()


if __name__ == "__main__":
    list_and_delete_all_tables(False)


# if __name__ == "__main__":
#     truncate_staging_tables()


#
# with engine.begin() as conn:
#     result = conn.execute(text("SELECT COUNT(*) FROM conformed_customers"))
#     rows = result.fetchall()
# engine.dispose()
# # #
# for row in rows:
#     print(row)


#
# engine = create_engine(conn_string, isolation_level="AUTOCOMMIT")
