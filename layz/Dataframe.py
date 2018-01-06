import itertools
import parquet
import csv
import copy

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

class Row(object):

    def __init__(self, data={}):
        self.data = data

    def get(self, column):
        if column in self.data.keys():
            return self.data[column]
        else:
            return None

    @property
    def columns(self):
        return self.data.keys()


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

        # if we ran out of external rows, compute more and add them.

        # get next page!
        if self.index >= len(self._external_rows):
            gen = self.func(self._internal_rows)
            self._internal_rows = []
            if is_iterator(gen) or type(gen) == list:
                for row in gen:
                    self._external_rows.append(row)
                    yield self._external_rows[self.index]
            else:
                # not an iterator, must be a row.
                self._external_rows.append(gen)

        to_ret = self._external_rows[self.index]
        return to_ret


class Dataframe(object):
    row_manager: RowManager

    def __init__(self, func):
        self.row_manager = RowManager()
        self.row_manager.func = func

    def add_row(self, data: {str, any}):
        self.row_manager.add_row(data)

    def get_rows(self):
        return self.row_manager

    def read_data_multiple(self, files: [str]):
        for file in files:
            yield self.read_data(file)

    def read_data(self, file: str):
        if file.endswith("parquet"):
            # turn into dataframe _internal_rows.
            for row in self.read_parquet(file):
                print(row)

                self.rows.append(Row())

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
        """
        for row in self.get_rows():
            me = row.get(me_col)
            my_friends = row.get(friends_col)
            for my_friend in my_friends:
                print("explode")

                yield Row({me_col: sorted([me, my_friend]), friends_col: sorted(my_friends)})
        """

        def f(rows):
            print("ex")
            for row in rows:
                me = row.get(me_col)
                my_friends = row.get(friends_col)
                for my_friend in my_friends:
                    print("explode")
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

                print("group by")
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

                print("finding common")
                yield Row({me_col: key, friends_col: list(intersection)})

        df = Dataframe(f)
        df.row_manager._internal_rows = self.row_manager
        return df

    def limit(self, lim):

        def f(rows):
            index = 0
            print("lim")
            for item in rows:
                print("limit")
                index += 1
                yield item

                if index >= lim:
                    break

        df = Dataframe(f)
        df.row_manager._internal_rows = self.row_manager
        return df