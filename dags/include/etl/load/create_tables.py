from sqlalchemy import text, create_engine
from include.config import config

conn_string = config.SILVER_DB_CONN_STRING


def create_all_tables():
    engine = create_engine(conn_string)

    with engine.begin() as conn:
        # STAGED CUSTOMERS
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS staging_conformed_customers (
                customer_key VARCHAR(100) PRIMARY KEY,
                customer_id VARCHAR(100) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                gender CHAR(1) NOT NULL,
                date_of_birth DATE,
                signup_date DATE,
                email VARCHAR(255),
                address VARCHAR(500),
                zip_code VARCHAR(5),
                state_code VARCHAR(2),
                state VARCHAR(50),
                last_updated_at TIMESTAMP,
                etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
            )
        )

        # STAGED AGENTS
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS staging_conformed_agents (
                agent_key VARCHAR(100) PRIMARY KEY,
                id VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(50) NOT NULL,
                experience VARCHAR(50),
                state VARCHAR(50),
                last_updated_at TIMESTAMP,
                etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
            )
        )

        # STAGED SM COMPLAINTS
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS staging_conformed_sm_complaints (
                sm_complaint_key VARCHAR(100) PRIMARY KEY,
                complaint_id VARCHAR(100) UNIQUE NOT NULL,
                customer_id VARCHAR(100),
                agent_id VARCHAR(100),
                complaint_category VARCHAR(50),
                media_channel VARCHAR(50),
                request_date DATE,
                resolution_date DATE,
                resolution_status VARCHAR(50),
                media_complaint_generation_date DATE,
                last_updated_at TIMESTAMP,
                etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
            )
        )

        # STAGED WEB COMPLAINTS
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS staging_conformed_web_complaints (
                web_complaint_key VARCHAR(100) PRIMARY KEY,
                request_id VARCHAR(100) UNIQUE NOT NULL,
                customer_id VARCHAR(100),
                agent_id VARCHAR(100),
                complaint_category VARCHAR(100),
                request_date DATE,
                resolution_date DATE,
                resolution_status VARCHAR(50),
                web_form_generation_date DATE,
                last_updated_at TIMESTAMP,
                etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
            )
        )

        # STAGED CALL LOGS
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS staging_conformed_call_logs (
                call_log_key VARCHAR(100) PRIMARY KEY,
                call_id VARCHAR(100) UNIQUE NOT NULL,
                customer_id VARCHAR(100),
                agent_id VARCHAR(100),
                complaint_category VARCHAR(100),
                call_start_time TIMESTAMP,
                call_end_time TIMESTAMP,
                request_date DATE,
                resolution_status VARCHAR(50),
                call_logs_generation_date DATE,
                last_updated_at TIMESTAMP,
                etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
            )
        )
        # ---------------------------------------------------------------------------
        # CUSTOMERS
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS conformed_customers (
                customer_key VARCHAR(100) PRIMARY KEY,
                customer_id VARCHAR(100) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                gender CHAR(1) NOT NULL,
                date_of_birth DATE,
                signup_date DATE,
                email VARCHAR(255),
                address VARCHAR(500),
                zip_code VARCHAR(5),
                state_code VARCHAR(2),
                state VARCHAR(50),
                last_updated_at TIMESTAMP,
                etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
            )
        )

        # AGENTS
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS conformed_agents (
                agent_key VARCHAR(100) PRIMARY KEY,
                id VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(50) NOT NULL,
                experience VARCHAR(50),
                state VARCHAR(50),
                last_updated_at TIMESTAMP,
                etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
            )
        )

        # SM COMPLAINTS
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS conformed_sm_complaints (
                sm_complaint_key VARCHAR(100) PRIMARY KEY,
                complaint_id VARCHAR(100) UNIQUE NOT NULL,
                customer_id VARCHAR(100) NOT NULL,
                agent_id VARCHAR(100) NOT NULL,
                complaint_category VARCHAR(50),
                media_channel VARCHAR(50),
                request_date DATE,
                resolution_date DATE,
                resolution_status VARCHAR(50),
                media_complaint_generation_date DATE,
                last_updated_at TIMESTAMP,
                etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES conformed_customers(customer_id),
                FOREIGN KEY (agent_id) REFERENCES conformed_agents(id)
            );
        """
            )
        )

        # WEB COMPLAINTS
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS conformed_web_complaints (
                web_complaint_key VARCHAR(100) PRIMARY KEY,
                request_id VARCHAR(100) UNIQUE NOT NULL,
                customer_id VARCHAR(100) NOT NULL,
                agent_id VARCHAR(100) NOT NULL,
                complaint_category VARCHAR(100),
                request_date DATE,
                resolution_date DATE,
                resolution_status VARCHAR(50),
                web_form_generation_date DATE,
                last_updated_at TIMESTAMP,
                etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES conformed_customers(customer_id),
                FOREIGN KEY (agent_id) REFERENCES conformed_agents(id)
            );
        """
            )
        )

        # CALL LOGS
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS conformed_call_logs (
                call_log_key VARCHAR(100) PRIMARY KEY,
                call_id VARCHAR(100) UNIQUE NOT NULL,
                customer_id VARCHAR(100) NOT NULL,
                agent_id VARCHAR(100) NOT NULL,
                complaint_category VARCHAR(100),
                call_start_time TIMESTAMP,
                call_end_time TIMESTAMP,
                request_date DATE,
                resolution_status VARCHAR(50),
                call_logs_generation_date DATE,
                last_updated_at TIMESTAMP,
                etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES conformed_customers(customer_id),
                FOREIGN KEY (agent_id) REFERENCES conformed_agents(id)
            );
        """
            )
        )

        # --------------QUARANTINE---------------------------
        conn.execute(
            text(
                """
                        CREATE TABLE IF NOT EXISTS data_quality_quarantine (
                            id SERIAL PRIMARY KEY,
                            table_name VARCHAR(100),
                            issue_type VARCHAR(50),
                            record_data JSONB,
                            error_message TEXT,
                            quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """
            )
        )
