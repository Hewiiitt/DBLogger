from db_logger.db_types import Variable, VariableMetaData, Experiment, Metric


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

    def save_metric(self, metric_name, value):
        met = Metric(
            _experiment_id=self.experiment_id,
            metric_name=metric_name,
            value=value,
        )

        self.queue.put(met)
