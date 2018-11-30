import itertools
import os
import pyarrow.parquet
import csv
import logging

from layz.group import Group
from layz.row import Row


def isEmpty(iterable):
    my_iter = itertools.islice(iterable, 0)
    try:
        next(my_iter)
    except StopIteration:
        return True
    return False


def INTERSECT(a: list, b: list):
    filtered = filter(lambda x: (x[0] in b and x[1] in a), zip(a, b))
    return map(lambda x: x[0], filtered)


class Dataframe(object):

    def __init__(self, func=None):
        self.row_manager: [Row] = []
        if func is None:
            # default to the identity function
            self.func = lambda x: x
        else:
            self.func = func

    def __repr__(self):
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

    @staticmethod
    def from_folder(folder):
        def f():
            for file in os.listdir(folder):
                file_ext = file.split(".")[-1]
                file_path = os.path.join(folder, file)

                logging.debug("Reading file: {0}".format(file_path))
                if file_ext == "csv":
                    yield from Dataframe.from_csv(file_path).get_rows()
                elif file_ext == "parquet":
                    yield from Dataframe.from_parquet(file_path).get_rows()
                elif file_ext == "txt":
                    yield from Dataframe.read_txt(file_path, col_name = "0").get_rows()
                else:
                    logging.warning("Unknown file type: ({0})".format(file_ext))
        return Dataframe(func = f)

    @staticmethod
    def from_csv(file):
        def f():
            with open(file) as csvfile:
                reader = csv.DictReader(csvfile)
                yield from reader
        return Dataframe(func = f)

    @staticmethod
    def from_parquet(file, columns = None):
        def f():
            if columns is None or len(columns) == 0:
                yield from pyarrow.parquet.read_table(file)
            else:
                yield from pyarrow.parquet.read_table(file, columns = columns)

        return Dataframe(func = f)

    @staticmethod
    def from_generator(f):
        return Dataframe(func = f)

    @staticmethod
    def with_data(data):
        def f():
            yield from data
        return Dataframe(func = f)

    @staticmethod
    def read_txt(file_name, col_name):
        def f():
            logging.debug("Reading lines!")
            with open(file_name, "r") as txtfile:
                for line in txtfile:
                    yield Row({col_name: line})

        return Dataframe(func = f)

    def get_rows(self):
        return self.row_manager

    def to_list(self):
        rows = []
        for row in self.row_manager:
            rows.append(row.get_as_dict())
        return rows

    def map_row(self, func):
        def f(rows):
            for row in rows:
                yield from func(row)

        return self.map_using(f)

    def map_using(self, func):
        # Return a new dataframe where the input rows of the dataframe
        # are the output of this dataframe. (after self.func is applied to each row.)
        self.func = map(lambda x: func(x), self.func)
        return self

    def add_row(self, row: Row):
        def f():
            yield from self.func
            yield row
        self.func = f
        return self

    def add_rows(self, rows: [Row]):
        def f():
            yield from self.func
            yield from rows
        self.func = f
        return self

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
                yield item
                index += 1

                if index >= lim:
                    raise StopIteration
        return self.map_using(f)

    def group_by_key(self, key):
        def f():
            groups = {}
            for item in self.func:
                assert key in item.keys(), "Column not found in row"
                if key in groups.keys():
                    groups[key].append(item)
                else:
                    groups.update({key, [item]})

            for group, values in groups.items():
                yield Group(group = group,
                            rows = values)