class Group(object):
    def __init__(self, group, rows):
        self.group = group
        self.rows = rows

    def get_rows(self):
        return self.rows

    def get_group(self):
        return self.group