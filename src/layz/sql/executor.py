# turn the parsed SQL into function calls.
from sql.parser import SCHEMA, CREATE


class Executor(object):
    def __init__(self):
        pass

    def generate(self, query):
        print(query)
        return self.__generate(query)

    def __generate(self, query, is_create=False):
        for index, item in enumerate(query):
            if type(item) == CREATE:
                create: CREATE = item
                return self.__generate([create.obj], is_create=True)

            if type(item) == SCHEMA:
                pass
