from sqlalchemy import text, create_engine
from include.config import config

conn_string = config.SILVER_DB_CONN_STRING


def truncate_staging_tables():
    engine = create_engine(conn_string)

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
