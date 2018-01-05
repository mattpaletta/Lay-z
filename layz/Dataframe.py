import parquet
import csv

class Row(object):
    def __init(self):
        self.data = {}

    def __init__(self, data):
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
    rows: [Row]

    def __init__(self):
        self.rows = []

    def add_row(self, data: {str, any}):
        row = Row(data)
        self.rows.append(row)

    def __next__(self):
        # pagination...





class Dataframe(object):
    row_manager: RowManager

    def __init__(self):
        self.row_manager = RowManager()

    def add_row(self, data: {str, any}):
        self.row_manager.add_row(data)

    def get_rows(self):
        for row in self.rows:
            yield row

    def read_data_multiple(self, files: [str]):
        for file in files:
            yield self.read_data(file)

    def read_data(self, file: str):
        if file.endswith("parquet"):
            # turn into dataframe rows.
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

        def f(iterator):
            return

        return self  # pointer to current dataframe.
