import itertools
from time import sleep

import parquet
import csv
import logging
import threading

from layz.row import Row
from layz.row_manager import RowManager


def isEmpty(iterable):
    my_iter = itertools.islice(iterable, 0)
    try:
        my_iter.next()
    except StopIteration:
        return True
    return False


def INTERSECT(a: list, b: list):
    filtered = filter(lambda x: (x[0] in b and x[1] in a), zip(a, b))
    return map(lambda x: x[0], filtered)


class Dataframe(object):
    row_manager: RowManager
    prev_row_manager: RowManager = None

    def __init__(self, func=None):
        self.row_manager = RowManager()
        if func is None:
            # default to the identity function
            self.row_manager.func = lambda x: x
        else:
            self.row_manager.func = func

        # create the thread to get new rows...
        threading.Thread(target=self.__get_new_rows).start()

    def __repr__(self):
        self.row_manager.index = 0  # reset pointer...
        rows = list(self.get_rows())  # evaluate this!

        keys = []
        for row in rows:
            keys.extend(row.columns)
        keys = list(sorted(set(keys)))

        longest_rows = {}

        for col in keys:
            # find the longest data point in that row...
            this_col_len = []
            for row in rows:
                this_col_len.append(len(str(row.get(col))))

            longest_rows.update({col: max(this_col_len)})

        print_str_header: str = ""

        for col in keys:
            print_str_header += '{:<' + str(longest_rows[col]) + '} | '

        print(print_str_header.format(*keys))
        print('-' * len(print_str_header.format(*keys)))

        for row in rows:
            print_str: str = ""

            items = []

            for col in keys:
                print_str += "{:<" + str(longest_rows[col]) + "} | "
                items.append(str(row.get(col)))

            print(print_str.format(*items))

        return ""

    def __get_new_rows(self):
        # background thread to pull rows from the previous dataframe...
        # add those new rows into the current row_manager.

        while True:
            sleep(5)
            if self.row_manager.has_rows():
                sleep(5)
                continue
            logging.debug("Getting input rows from last DF!")

            if self.prev_row_manager is None:
                return

            for row in self.prev_row_manager:
                self.row_manager.add_row(row.data)

            if not self.row_manager.getting_more_rows():
                break

    def add_row(self, data: {str, any}):
        self.row_manager.add_row(data)

    def get_rows(self):
        return self.row_manager

    def to_list(self):
        rows = []
        for row in self.row_manager:
            rows.append(row.get_as_dict())
        return rows

    def read_data_multiple(self, files: [str]):
        for file in files:
            yield self.read_data(file)

    def read_data(self, file: str):
        if file.endswith("parquet"):
            # turn into dataframe _internal_rows.
            for row in self.__read_parquet(file):
                # make the conversion from parquet to Row()
                print(row)

        elif file.endswith("csv"):
            for row in self.__read_csv(file):
                print(row)

    def __read_parquet_multiple(self, files):
        for file in files:
            for row in self.__read_parquet(file):
                yield row

    def __read_parquet(self, file):
        with open(file) as fo:
            for row in parquet.reader(fo):
                yield row

    def __read_csv(self, file):
        with open(file, 'rb') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for row in csvreader:
                yield row

    def read_txt(self, file_name, col_name):
        def f(iterator):
            logging.debug("Reading lines!")
            with open(file_name, "r") as txtfile:
                for line in txtfile:
                    yield Row({col_name: line})

        return self.map_using(f)

    def map_row(self, func):
        def f(rows):
            for row in rows:
                for item in func(row):
                    yield item

        return self.map_using(f)


    def map_using(self, func):
        # Return a new dataframe where the input rows of the dataframe
        # are the output of this dataframe. (after self.func is applied to each row.)
        df = Dataframe(func)
        df.prev_row_manager = self.row_manager

        return df

    def with_column_renamed(self, col_name, new_col_name):
        def f(rows):
            for row in rows:
                # Replace the key in the dictionary, create new row with that new key.
                data: dict = row.get_as_dict()
                assert col_name in data.keys(), "Original Column does not exist!"
                assert new_col_name not in data.keys(), "Duplicate column!"

                data[new_col_name] = data.pop(col_name)
                yield Row(data)

        return self.map_using(f)

    def with_column(self, col_name, func):
        def f(rows):
            for row in rows:
                data: dict = row.get_as_dict()
                data.update({col_name: func(row)})
                yield Row(data)

        return self.map_using(f)

    def filter(self, func):
        def f(rows):
            for row in rows:
                should_include = func(row)
                assert type(should_include) == bool, "Filter function must return a boolean"
                if should_include:
                    yield row

        return self.map_using(f)

    def limit(self, lim):
        def f(rows):
            index = 0
            logging.debug("Getting Rows")
            for item in rows:
                logging.debug("Reached Limit!")
                index += 1
                yield item

                if index >= lim:
                    break

        return self.map_using(f)
