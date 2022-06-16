import uuid
import threading

from queue import Queue
from pathlib import Path
from db_logger import LogClient
from db_logger.db_types import AbstractDBType, Experiment, Variable, VariableMetaData, Metric, BinVariable
from multiprocessing import Manager, Process
from datetime import datetime

import sqlite3 as sql


class DBLogger:

    db_defaults = [
        Experiment,
        VariableMetaData,
        Variable,
        Metric,
        BinVariable
    ]

    def __init__(self, path='results.db', data_tables=[], multi_process=True, block_size=10000):
        Path(path).parents[0].mkdir(parents=True, exist_ok=True)

        data_tables = data_tables + DBLogger.db_defaults
        data_tables = set(data_tables)

        self.data_tables = data_tables
        self.block_size = block_size
        self.conn = sql.connect(path, check_same_thread=False, detect_types=sql.PARSE_DECLTYPES | sql.PARSE_COLNAMES)

        self._init_db()

        self.conn.close()
        self.multi_process = multi_process

        if multi_process:
            self.manager = Manager()
            self.queue = self.manager.Queue()

            self.process = Process(target=DBLogger.run, args=(self.queue, path, data_tables, block_size))
            self.process.start()
        else:
            self.queue = Queue()
            self.process = threading.Thread(target=DBLogger.run, args=(self.queue, path, data_tables, block_size))
            self.process.start()

    @staticmethod
    def run(queue, path, data_tables, block_size=10000):
        conn = sql.connect(path, check_same_thread=False, detect_types=sql.PARSE_DECLTYPES | sql.PARSE_COLNAMES)

        for table in data_tables:
            table.pre_run_execution(conn)

        next_task = None
        force_stop = False

        while not force_stop:
            tasks = []
            if next_task is not None:
                task = next_task
            else:
                if queue.qsize() < 1:
                    continue
                task = queue.get()
                if task == 'Stop':
                    force_stop = True
                    break

            tasks.append(task)
            next_task = queue.get()

            while isinstance(next_task, type(task)):
                tasks.append(next_task)
                if queue.qsize() < 1:
                    next_task = None
                    break
                next_task = queue.get()
                if next_task == 'Stop':
                    force_stop = True
                    break
                if len(tasks) >= block_size:
                    next_task = None
                    break

            if force_stop:
                break

            if len(tasks) > 1:
                type(tasks[0]).save_many_to_table(conn, tasks)
            else:
                tasks[0].save_to_table(conn)

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
        self.queue.put('Stop')
        if force:
            if self.multi_process:
                self.process.terminate()
            else:
                self.process.join()
            return

        while not self.can_close():
            print('\r[DBLogger] - Clearing remaining tasks: {}\t\t'.format(self.get_length()), end='')

        if self.multi_process:
            self.process.terminate()
        else:
            self.process.join()
        print('\r[DBLogger] - Logger process terminated.')

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

        return LogClient(self.queue, experiment_id)
