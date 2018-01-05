import parquet
import csv

class Row(object):
    def __init(self):
        self.data = {}

    def get(self, column):
        if column in self.data.keys():
            return self.data[column]
        else:
            return None

    @property
    def columns(self):
        return self.data.keys()


class Dataframe(object):
    rows: [Row]

    def __init__(self):
        self.rows = []

    def add_row(self, data: {str, any}):
        self.rows.append(data)

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