from . import interactive_client


class ResourceList:
    def __init__(self):
	self.__list = []

    def append(self, item):
        self.__list.append(item)
        if interactive_client is not None:
            interactive_client.apply()

    def list(self):
        return self.__list

    def __iter__(self):
        return iter(self.__list)

    def __next__(self):
	return next(self.__list)
