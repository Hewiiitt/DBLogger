from abc import abstractmethod


class AbstractDBType:

    @staticmethod
    @abstractmethod
    def create_table(conn):
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def pre_run_execution(conn):
        pass

    @abstractmethod
    def save_to_table(self, conn):
        raise NotImplementedError()
