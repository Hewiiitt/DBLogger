import uuid

from db_logger.db_types import AbstractDBType, Experiment, Variable, VariableMetaData
from multiprocessing import Manager, Process
from datetime import datetime

import sqlite3 as sql


class DBLogger:

    db_defaults = [
        Experiment,
        VariableMetaData,
        Variable
    ]

    def __init__(self, path='results.db', data_tables=[], multi_process=True):

        data_tables = data_tables + DBLogger.db_defaults
        data_tables = set(data_tables)

        self.data_tables = data_tables
        self.conn = sql.connect(path, check_same_thread=False, detect_types=sql.PARSE_DECLTYPES | sql.PARSE_COLNAMES)

        self._init_db()

        self.conn.close()

        if multi_process:
            self.manager = Manager()
            self.queue = self.manager.Queue()

            self.process = Process(target=DBLogger.run, args=(self.queue, path, data_tables))
            self.process.start()

    @staticmethod
    def run(queue, path, data_tables):
        conn = sql.connect(path, check_same_thread=False, detect_types=sql.PARSE_DECLTYPES | sql.PARSE_COLNAMES)

        for table in data_tables:
            table.pre_run_execution(conn)

        while True:
            task = queue.get()

            task.save_to_table(conn)

    def can_close(self):
        return self.queue.qsize() < 1

    def get_length(self):
        return self.queue.qsize()

    def _init_db(self):
        print('[DBLogger] - Initialising database...')
        for table in self.data_tables:
            table.create_table(self.conn)

        print('[DBLogger] - Database finished initialising!')

    def close(self, force=False):
        if force: self.process.join()

        while not self.can_close():
            print('\r[DBLogger] - Clearing remaining tasks: {}\t\t'.format(self.get_length()), end='')

        self.process.join()
        print('[DBLogger] - Logger process terminated.')

    def register_experiment(self, project_name, experiment_name, experiment_count, description='Sample Description.',date_created=None):
        if date_created is None:
            date_created = datetime.now()

        experiment_id = str(uuid.uuid4())

        exp_1 = Experiment(
            _experiment_id=experiment_id,
            project_name=project_name,
            experiment_name=experiment_name,
            experiment_count=experiment_count,
            date_created=date_created,
            description=description
        )

        self.queue.put(exp_1)

        return experiment_id
