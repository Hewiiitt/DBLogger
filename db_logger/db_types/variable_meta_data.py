from db_logger.db_types import AbstractDBType

from dataclasses import dataclass
from datetime import datetime


@dataclass
class VariableMetaData(AbstractDBType):

    VAR_META_DATA = {}

    variable_name: str

    @staticmethod
    def pre_run_execution(conn):
        cur = conn.cursor()
        cur.execute("SELECT * FROM variable_meta_data")
        rows = cur.fetchall()
        for row in rows:
            VariableMetaData.VAR_META_DATA[row[1]] = row[0]

    @staticmethod
    def create_table(conn):
        try:
            c = conn.cursor()
            c.execute(""" CREATE TABLE IF NOT EXISTS variable_meta_data (
                _variable_name_id INTEGER PRIMARY KEY AUTOINCREMENT,
                variable_name text NOT NULL,
                UNIQUE(variable_name)
            ); """)
            conn.commit()
        except Exception as e:
            print(e)

    def save_to_table(self, conn):
        try:
            c = conn.cursor()
            query = """INSERT OR IGNORE INTO variable_meta_data
                           (
                               variable_name
                           ) VALUES
                           (?);"""
            data = self.get()

            c.execute(query, data)
            conn.commit()

            query = """SELECT * FROM variable_meta_data WHERE variable_name=?;"""
            c.execute(query, data)
            rows = c.fetchall()

            VariableMetaData.VAR_META_DATA[rows[0][1]] = rows[0][0]

        except Exception as e:
            print(e)

    @staticmethod
    def save_many_to_table(conn, entries):
        try:
            c = conn.cursor()
            query = """INSERT OR IGNORE INTO variable_meta_data
                                       (
                                           variable_name
                                       ) VALUES
                                       (?);"""
            data = [entry.get() for entry in entries]

            c.execute(query, data)
            conn.commit()

            query = """SELECT * FROM variable_meta_data WHERE variable_name=?;"""
            c.execute(query, data)
            rows = c.fetchall()

            for row in rows:
                VariableMetaData.VAR_META_DATA[row[1]] = row[0]

        except Exception as e:
            print(e)

    def get(self):
        return (
            self.variable_name,
        )

    @staticmethod
    def has_variable(var_name):
        return var_name in VariableMetaData.VAR_META_DATA
