from psycopg2 import pool
import os


class JobDBConnectionPool:
    def __init__(self, minconn, maxconn):
        self.connection_pool = pool.SimpleConnectionPool(
            minconn,
            maxconn,
            user=os.getenv("DW_USER"),
            password=os.getenv("DW_PASS"),
            host="postgres",
            port="5432",
            database=os.getenv("DW_DB"),
        )

    def get_connection(self):
        return self.connection_pool.getconn()

    def release_connection(self, conn):
        self.connection_pool.putconn(conn)

    def close_all_connections(self):
        self.connection_pool.closeall()
