class OnlineStore:
    def create_table(self, name, variant, entity_type):
        raise NotImplementedError()

    def get_table(self, name, variant):
        raise NotImplementedError()

    def table_exists(self, name, variant):
        raise NotImplementedError()

    def is_vector_store(self):
        raise NotImplementedError()


class OnlineTable:
    def set(self, key, value):
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()


class OnlineVectorTable:
    def nearest(self, vector):
        raise NotImplementedError()
