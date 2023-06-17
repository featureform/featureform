class ResourceList:
    interactive_client = None

    def __init__(self):
        self.__list = []
        self.idx = 0

    def append(self, item):
        self.__list.append(item)
        if ResourceList.interactive_client is not None:
            ResourceList.interactive_client.apply()

    def list(self):
        return self.__list

    def __iter__(self):
        return iter(self.__list)
