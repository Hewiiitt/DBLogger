import numpy as np
import sqlite3 as sql


class DBAnalyser:

    def __init__(self, path):
        self.path = path
        self.conn = sql.connect(path, check_same_thread=False, detect_types=sql.PARSE_DECLTYPES | sql.PARSE_COLNAMES)

    def get_experiments(self):
        c = self.conn.cursor()

        query = """SELECT * FROM experiments;"""

        c.execute(query)
        rows = c.fetchall()

        result = {}

        for row in rows:

            _result = {}
            _result['project_name'] = row[1]
            _result['experiment_name'] = row[2]
            _result['experiment_count'] = row[3]
            _result['date_created'] = row[4]
            _result['description'] = row[5]

            result[row[0]] = _result

        return result

    def get_variables(self):
        c = self.conn.cursor()

        query = """SELECT * FROM variable_meta_data;"""

        c.execute(query)
        rows = c.fetchall()

        result = {}

        for row in rows:

            result[str(row[0])] = row[1]

        return result

    def get_variable_data(self, experiment, variable):
        if isinstance(variable, str):
            var_id = self.get_variable_id(variable)

            if var_id is None:
                return [], []
            variable = var_id

        c = self.conn.cursor()

        query = """SELECT x_value, y_value FROM variables WHERE _experiment_id=? AND _variable_name_id=?;"""

        c.execute(query, (experiment, variable))
        rows = c.fetchall()

        x = []
        y = []

        for row in rows:
            x.append(row[0])
            y.append(row[1])

        return np.array(x), np.array(y)

    def get_variable_id(self, variable_name):
        c = self.conn.cursor()
        query = """SELECT _variable_name_id FROM variable_meta_data WHERE variable_name=?;"""

        c.execute(query, (variable_name, ))
        rows = c.fetchall()

        if len(rows) < 1:
            return None

        return int(rows[0][0])

    def get_bin_variable_data(self, experiment, variable):
        if isinstance(variable, str):
            var_id = self.get_variable_id(variable)

            if var_id is None:
                return [], []
            variable = var_id

        c = self.conn.cursor()

        query = """SELECT x_value, y_value FROM bin_variables WHERE _experiment_id=? AND _variable_name_id=?;"""

        c.execute(query, (experiment, variable))
        rows = c.fetchall()

        x = []
        y = []

        for row in rows:
            x.append(row[0])
            y.append(row[1])

        return np.array(x), np.array(y)

    def get_all_experiment_bin_variable_data(self, experiment,):
        c = self.conn.cursor()

        query = """SELECT x_value, y_value FROM bin_variables WHERE _experiment_id=?;"""

        c.execute(query, (experiment,))
        rows = c.fetchall()

        x = []
        y = []

        for row in rows:
            x.append(row[0])
            y.append(row[1])

        return np.array(x), np.array(y)

    def run_query(self, query, params):
        pass
