from db_logger.db_types import AbstractDBType, VariableMetaData

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Metric(AbstractDBType):
    _experiment_id: str
    metric_name: str
    value: float

    @staticmethod
    def create_table(conn):
        try:
            c = conn.cursor()
            c.execute(""" CREATE TABLE IF NOT EXISTS metrics (
                _metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                _experiment_id text NOT NULL,
                metric_name text NOT NULL,
                value real NOT NULL,
                FOREIGN KEY(_experiment_id) REFERENCES experiments(_experiment_id)
            ); """)
            conn.commit()
        except Exception as e:
            print(e)

    def save_to_table(self, conn):
        try:
            c = conn.cursor()
            query = """INSERT INTO variables
                           (
                                _experiment_id,
                                metric_name,
                                value real
                           ) VALUES
                           (?, ?, ? );"""
            data = self.get()

            c.execute(query, data)
            conn.commit()
        except Exception as e:
            print(e)

    @staticmethod
    def save_many_to_table(conn, entries):
        try:
            c = conn.cursor()
            query = """INSERT INTO variables
                           (
                                _experiment_id,
                                metric_name,
                                value real
                           ) VALUES
                           (?, ?, ? );"""

            data = []
            for entry in entries:
                data.append(entry.get())

            c.executemany(query, data)
            conn.commit()
        except Exception as e:
            print(e)

    def get(self):
        return (
            self._experiment_id,
            self.metric_name,
            self.value
        )
