import copy

from layz.row import Row

def is_iterator(obj):
    if (
            hasattr(obj, '__iter__') and
            (hasattr(obj, 'next') or hasattr(obj, "__next__")) and      # or __next__ in Python 3
            callable(obj.__iter__) and
            obj.__iter__() is obj
        ):
        return True
    else:
        return False

class RowManager(object):
    _internal_rows: [Row]
    _external_rows: [Row]
    index = 0
    func = None

    def __init__(self):
        self._internal_rows = []
        self._external_rows = []
        self.index = 0
        self.func = lambda x: x

    def add_row(self, data: {str, any}):
        row = Row(data)
        self._internal_rows.append(row)

    def set_internal_rows(self, rows):
        self._internal_rows = rows

    def get_internal_rows(self):
        return copy.deepcopy(self._internal_rows)

    def __getitem__(self, index):
        self.index = index
        return self.__next__()

    def __next__(self):
        # pagination...

        # TODO:// handle cases when a function operates on a single level...
        # TODO:// They may still be computing!

        # get next page!
        if self.index >= len(self._external_rows):
            gen = self.func(self._internal_rows)
            self._internal_rows = []
            if is_iterator(gen) or type(gen) == list:
                for row in gen:
                    self._external_rows.append(row)
            else:
                # not an iterator, must be a row.
                self._external_rows.append(gen)

        to_ret = self._external_rows[self.index]
        return to_ret