from db_logger.db_types import AbstractDBType

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Experiment(AbstractDBType):

    _experiment_id: str
    project_name: str
    experiment_name: str
    experiment_count: int
    date_created: datetime
    description: str

    @staticmethod
    def create_table(conn):
        try:
            c = conn.cursor()
            c.execute(""" CREATE TABLE IF NOT EXISTS experiments (
                _experiment_id text PRIMARY KEY,
                project_name text NOT NULL,
                experiment_name text NOT NULL,
                experiment_count integer NOT NULL,
                date_created timestamp,
                description text
            ); """)
            conn.commit()
        except Exception as e:
            print(e)

    def save_to_table(self, conn):
        try:
            c = conn.cursor()
            query = """INSERT INTO experiments
                (
                    _experiment_id, 
                    project_name, 
                    experiment_name, 
                    experiment_count, 
                    date_created, 
                    description
                ) VALUES
                (?, ?, ?, ?, ?, ?);"""
            data = (
                self._experiment_id,
                self.project_name,
                self.experiment_name,
                self.experiment_count,
                self.date_created,
                self.description
            )

            c.execute(query, data)
            conn.commit()
        except Exception as e:
            print(e)

    @staticmethod
    def save_many_to_table(conn, entries):
        try:
            c = conn.cursor()
            query = """INSERT INTO experiments
                (
                    _experiment_id, 
                    project_name, 
                    experiment_name, 
                    experiment_count, 
                    date_created, 
                    description
                ) VALUES
                (?, ?, ?, ?, ?, ?);"""
            data = [entry.get() for entry in entries]

            c.executemany(query, data)
            conn.commit()
        except Exception as e:
            print(e)

    def get(self):
        return (
            self._experiment_id,
            self.project_name,
            self.experiment_name,
            self.experiment_count,
            self.date_created,
            self.description
        )