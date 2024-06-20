import os

import duckdb

from framework.utils.custlogging import LoggerProvider
from framework.utils.singleton import SingletonMeta

logger = LoggerProvider().get_logger(os.path.basename(__file__))


class DuckDB(metaclass=SingletonMeta):
    def __init__(self, db_path=None):
        self.db_path = db_path
        self.duckDBClient = None
        self.connect_to_duckdb()
        self.register_custom_function()

    def connect_to_duckdb(self):
        try:
            logger.info("connecting to duckdb")
            if self.db_path and self.db_path.strip():
                self.duckDBClient = duckdb.connect(self.db_path)
            else:
                self.duckDBClient = duckdb.connect()
            self.duckDBClient.execute("SET GLOBAL pandas_analyze_sample=10000000")
            self.duckDBClient.execute("""
                        INSTALL 'httpfs';
                        LOAD 'httpfs';
                        SET s3_region='ap-south-1';
            """)

        except Exception as e:
            logger.error(f'Failed to connect to DuckDB: {e}')
            raise

    def get_connection(self):
        return self.duckDBClient

    def drop_all_objects(self, object_type, drop_command):
        objects_query = self.duckDBClient.execute(f"SHOW {object_type}")
        objects = objects_query.fetch_df()['name'].tolist()

        if objects:
            for obj in objects:
                self.duckDBClient.execute(drop_command.format(obj=obj))

    def flush_all(self):
        try:
            # Drop all tables
            self.drop_all_objects("TABLES", "DROP TABLE {table}")

            # Drop all views
            self.drop_all_objects("VIEWS", "DROP VIEW {view}")

            logger.info("All tables and views have been dropped from DuckDB.")

        except Exception as e:
            logger.error(f"Failed to flush DuckDB: {e}")
            raise

    # TODO: figure out how to decouple this
    def register_custom_function(self):
        pass
