import itertools
import parquet
import csv
import logging

from Row import Row
from RowManager import RowManager


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

def isEmpty (iterable):
    my_iter = itertools.islice (iterable, 0)
    try:
        my_iter.next()
    except StopIteration:
         return True
    return False

def INTERSECT(a: list, b: list):
    filtered = filter(lambda x: (x[0] in b and x[1] in a), zip(a, b))
    return map(lambda x: x[0], filtered)  # flatmap it...

class Dataframe(object):
    row_manager: RowManager

    def __init__(self, func):
        self.row_manager = RowManager()
        self.row_manager.func = func

    def __repr__(self):
        self.row_manager.index = 0 # reset pointer...
        rows = list(self.get_rows()) # evaluate this!

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
            for row in self.read_parquet(file):
                # make the conversion from parquet to Row()
                pass
                print(row)

        if file.endswith("csv"):
            for row in self.read_csv(file):
                print(row)

    def read_parquet_multiple(self, files):
        for file in files:
            for row in self.read_parquet(file):
                yield row

    def read_parquet(self, file):
        with open(file) as fo:
            for row in parquet.reader(fo):
                yield row

    def read_csv(self, file):
        with open(file, 'rb') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
            for row in csvreader:
                yield row

    def explode_dict(self, me_col, friends_col):

        def f(rows):
            logging.debug("Explode Looping over rows!")
            for row in rows:
                me = row.get(me_col)
                my_friends = row.get(friends_col)
                for my_friend in my_friends:
                    logging.debug("Exploding!" + str(my_friends))
                    yield Row({me_col: sorted([me, my_friend]), friends_col: sorted(my_friends)})

        df = Dataframe(f)
        df.row_manager._internal_rows = self.row_manager

        return df  # pointer to current dataframe.

    def group_by_key(self, me_col, friends_col):
        def f(rows):
            # here we have to loop over all the items :(
            seen_friends = {}
            for row in rows:
                key = tuple(row.get(me_col))
                value = row.get(friends_col)

                logging.debug("Grouping by: " + str(key))
                if key in seen_friends.keys():
                    current_friends: list = seen_friends[key]
                    current_friends.append(value)
                else:
                    seen_friends.update({key: [value]})

            for key, value in seen_friends.items():
                yield Row({me_col: key, friends_col: value})

        df = Dataframe(f)
        df.row_manager._internal_rows = self.row_manager

        return df

    def find_common_friends(self, me_col, friends_col):
        def f(rows):
            for row in rows:
                key = row.get(me_col)
                value = row.get(friends_col)

                # compute intersection...
                if len(value) == 1:
                    intersection = value
                elif len(value) == 2:
                    intersection = INTERSECT(value[0], value[1])
                else:
                    intersection = INTERSECT(value[0], value[1])
                    for i in value[1:]:
                        intersection = INTERSECT(list(intersection), i)

                logging.debug("Finding Common friends for: " + str(key))
                yield Row({me_col: key, friends_col: list(intersection)})

        df = Dataframe(f)
        df.row_manager._internal_rows = self.row_manager
        return df

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

        df = Dataframe(f)
        df.row_manager._internal_rows = self.row_manager
        return df