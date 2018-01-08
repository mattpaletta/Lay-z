import copy
import logging

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
    _internal_buffer: [Row]
    index = 0
    func = None
    current_page = 0
    page_size = 100

    def __init__(self):
        self._internal_rows = []  # rows to be processed
        self._external_rows = []  # rows that have been processed, ready to be requested.
        self._internal_buffer = []  # next page of internal rows to be processed as a page...
        self.index = 0
        self.func = lambda x: x
        self.current_page = 0
        self.page_size = 100

    def add_row(self, data: {str, any}):
        row = Row(data)
        self._internal_rows.append(row)

    def get_by_page(self):
        pass

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

        # we've run out of input, so create the next page for this DF to be processed.
        if len(self._internal_buffer) == 0:
            for index, item in enumerate(self._internal_rows):
                self._internal_buffer.append(item)
                if index > self.page_size:
                    logging.debug("Reached page size!")
                    break

        # TODO:// How to handle generator functions (0 to many functions)

        # get next page!
        if self.index >= len(self._external_rows) and len(self._internal_buffer) > 0:
            # process that gets internal rows from previous dataframe...
            # process that takes the internal rows, and turns them into pages
            # for this dataframe to consume from.

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
