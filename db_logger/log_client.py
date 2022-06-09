from db_logger.db_types import Variable, VariableMetaData, Experiment


class LogClient:

    def __init__(self, queue, experiment_id):
        self.experiment_id = experiment_id
        self.queue = queue

    def save_scalar_variable(self, variable_name, x, y):
        var = Variable(
            _experiment_id=self.experiment_id,
            variable_name=variable_name,
            x_value=x,
            y_value=y,
        )

        self.queue.put(var)
