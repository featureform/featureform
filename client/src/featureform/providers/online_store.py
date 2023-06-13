from abc import ABC


class OnlineStore(ABC):
    def create_table(self, name, variant, entity_type):
        pass

    def get_table(self, name, variant):
        pass

    def table_exists(self, name, variant):
        pass


class OnlineTable(ABC):
    def set(self, key, value):
        pass

    def get(self, key):
        pass

    def close(self):
        pass


class OnlineVectorTable(ABC):
    def nearest(self, vector):
        pass
