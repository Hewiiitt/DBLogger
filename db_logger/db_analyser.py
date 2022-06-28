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

    def get_metric(self, experiment, metric_name):
        c = self.conn.cursor()

        query = """SELECT value FROM metrics WHERE _experiment_id=? AND metric_name=?;"""

        c.execute(query, (experiment, metric_name))
        rows = c.fetchall()

        x = []

        for row in rows:
            x.append(row[0])

        return float(x)

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

    def get_all_experiment_bin_variable_data(self, experiment):
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

    def get_all_tables(self):
        c = self.conn.cursor()
        query = """SELECT name FROM sqlite_schema WHERE type='table';"""
        c.execute(query)
        rows = c.fetchall()

        return rows[0]

    def delete_experiment(self, experiment_id):
        try:
            tables = self.get_all_tables()

            for table in tables:
                print('[DB Logger] - Deleting experiment {} from {}'.format(experiment_id, table))
                c = self.conn.cursor()
                query = """DELETE FROM {} WHERE _experiment_id=?;""".format(table)
                data = (experiment_id, )
                c.execute(query, data)
                self.conn.commit()

        except Exception as e:
            print(e)

    def run_query(self, query, params):
        pass
