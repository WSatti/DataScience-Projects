import json
import os
from logic_layer.postgres_database.database_utils import create_db_connection

async def initialize_db():
    DB_NAME = os.getenv('DB_NAME')

    conn = await create_db_connection()
    async with conn.transaction():
        exists = await conn.fetchval(f"SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}')")
        if not exists:
            await conn.execute(f'CREATE DATABASE {DB_NAME}')
            print(f"Database '{DB_NAME}' created successfully.")
        else:
            print(f"Database '{DB_NAME}' already exists.")

    async with conn.transaction():
        await conn.execute('''
                    CREATE TABLE IF NOT EXISTS api_inferences (
                        id SERIAL PRIMARY KEY,
                        request_data JSONB NOT NULL,
                        prediction VARCHAR(255) NOT NULL,
                        prediction_source VARCHAR(255) NOT NULL,
                        timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
                    );
                ''')
        print("Table 'api_inferences' created/verified successfully.")
    await conn.close()
