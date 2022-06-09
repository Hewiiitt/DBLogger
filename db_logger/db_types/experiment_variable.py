from db_logger.db_types import AbstractDBType, VariableMetaData

from dataclasses import dataclass
from datetime import datetime


@dataclass
class Variable(AbstractDBType):
    _experiment_id: str
    variable_name: str
    x_value: float
    y_value: float

    @staticmethod
    def create_table(conn):
        try:
            c = conn.cursor()
            c.execute(""" CREATE TABLE IF NOT EXISTS variables (
                _variable_id INTEGER PRIMARY KEY AUTOINCREMENT,
                _experiment_id text NOT NULL,
                _variable_name_id integer NOT NULL,
                x_value real NOT NULL,
                y_value real NOT NULL,
                FOREIGN KEY(_experiment_id) REFERENCES experiments(_experiment_id),
                FOREIGN KEY(_variable_name_id) REFERENCES variable_meta_data(_variable_name_id)
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
                               _variable_name_id,
                               x_value,
                               y_value
                           ) VALUES
                           (?, ?, ?, ? );"""
            if not VariableMetaData.has_variable(self.variable_name):
                var_meta_1 = VariableMetaData(
                    variable_name=self.variable_name
                )
                var_meta_1.save_to_table(conn)
            data = self.get()

            c.execute(query, data)
            conn.commit()
        except Exception as e:
            print(e)

    def get(self):
        var_meta_id = VariableMetaData.VAR_META_DATA[self.variable_name]
        return (
            self._experiment_id,
            var_meta_id,
            self.x_value,
            self.y_value,
        )
