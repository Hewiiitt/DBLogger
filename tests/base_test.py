import os
import unittest
import uuid

from datetime import datetime
from pathlib import Path
from db_logger import DBLogger

from db_logger.db_types import Experiment, Variable, VariableMetaData


class MyTestCase(unittest.TestCase):

    def test_init_logger(self):
        if Path('./test.db').exists():
            os.remove('./test.db')

        db_types = [
            Experiment,
            VariableMetaData,
            Variable
        ]

        logger = DBLogger('test.db', db_types)

        self.assertEqual(Path('./test.db').exists(), True)

    def test_register_experiment(self):
        db_types = [
            Experiment,
            VariableMetaData,
            Variable
        ]

        logger = DBLogger('test.db', db_types)
        exp_1 = Experiment(
            _experiment_id=str(uuid.uuid4()),
            project_name='Project 1',
            experiment_name='Experiment 1',
            experiment_count=1,
            date_created=datetime.now(),
            description='This is the first experiment test!'
        )
        exp_1.save_to_table(logger.conn)

if __name__ == '__main__':
    unittest.main()
