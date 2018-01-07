class Row(object):

    def __init__(self, data={}):
        self.data = data

    def get_as_dict(self):
        return self.data

    def get_as_list(self):
        return list(self.data.values())

    def get(self, column):
        if column in self.data.keys():
            return self.data[column]
        else:
            return None

    def get_as_str(self, column):
        return str(self.get(column))

    def get_as_int(self, column):
        return int(self.get(column))

    @property
    def columns(self):
        return self.data.keys()