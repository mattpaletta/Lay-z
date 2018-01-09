import copy
import logging
import threading
from time import sleep

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
    _gen = None  # stored so we can reuse the generator.  Should continue from last point.
    current_page = 0
    page_size = 100

    def __init__(self):
        self._internal_rows = []  # rows to be processed
        self._external_rows = []  # rows that have been processed, ready to be requested.
        self._internal_buffer = []  # next page of internal rows to be processed as a page...
        self.index = 0
        self.func = lambda x: x
        self._gen = None
        self.current_page = 0
        self.page_size = 100

        threading.Thread(target=self.__get_next_page).start()

    def add_row(self, data: {str, any}):
        row = Row(data)
        logging.debug("Adding more rows! " + str(data.keys()))
        self._internal_rows.append(row)

    def getting_more_rows(self):
        return self.current_page > 10

    def has_rows(self):
        return self.index > self.page_size

    def __get_next_page(self):
        # we've run out of input, so create the next page for this DF to be processed.
        should_get_more_rows = True
        while True:
            sleep(1)
            # Incrementally add rows from the internal rows (copied from last DF) to buffer (buffer to be processed by this DF)
            if len(self._internal_buffer) == 0 and should_get_more_rows:
                logging.debug("Pulling more rows!" + str(len(self._internal_rows)) + " " + str(len(self._internal_buffer)))
                for index, item in enumerate(self._internal_rows):
                    self._internal_buffer.append(copy.deepcopy(item))
                    if index > self.page_size:
                        logging.debug("Reached page size!")
                        break

            # get next page! When halfway through current page.
            if self.index >= len(self._external_rows) * 0.5 and (len(self._internal_buffer) > 0 or self._gen is None):
                # process that gets internal rows from previous dataframe...
                # process that takes the internal rows, and turns them into pages
                # for this dataframe to consume from.

                logging.debug("Generating more rows")
                self._gen = self.func(self._internal_rows)
                self._internal_rows = []
                if is_iterator(self._gen) or type(self._gen) == list:
                    for row in self._gen:
                        self._external_rows.append(row)
                        if len(self._external_rows) - self.index > self.page_size:
                            break

                else:
                    # not an iterator, must be a row.
                    self._external_rows.append(self._gen)

                should_get_more_rows = True

            break

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

        # TODO:// How to handle generator functions (0 to many functions)

        to_ret = self._external_rows[self.index]
        return to_ret
