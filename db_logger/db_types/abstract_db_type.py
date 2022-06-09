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

    @staticmethod
    @abstractmethod
    def save_many_to_table(conn, entries):
        raise NotImplementedError()

    @abstractmethod
    def get(self):
        raise NotImplementedError()

    @abstractmethod
    def save_to_table(self, conn):
        raise NotImplementedError()
