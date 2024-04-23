import asyncpg
import os

async def create_db_connection():
    return await asyncpg.connect(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
        host=os.getenv('DB_HOST')
    )

async def close_db_connection(conn):
    await conn.close()