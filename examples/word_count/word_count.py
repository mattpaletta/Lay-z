# Initialize our logger
import logging
import sys

from layz.dataframe import Dataframe
from row import Row

root = logging.getLogger()
root.setLevel(logging.NOTSET)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.NOTSET)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)s]')
ch.setFormatter(formatter)
root.addHandler(ch)

def split_by_word(row):
    line: str = row.get_as_str("data")
    logging.debug("Splitting Line!")
    for word in line.split(" "):
        yield Row({"word": word, "num": 1})


def count_words(rows):
    curr_count = {}
    logging.debug("Counting words!")
    for row in rows:
        word = row.get("word")
        count = row.get("num")
        if word in curr_count.keys():
            curr_count[word] += count
        else:
            curr_count[word] = count

    for key, value in curr_count.items():
        yield Row({"word": key, "count": value})


pride = "pride_and_prejudice.txt"
small_test = "small_test.txt"

df = Dataframe()
df2 = df.read_txt(file_name=small_test, col_name="data")
df3 = df2.map_row(split_by_word)
df4 = df3.map_using(count_words)

print("DF2")
print(df2)
print("DF3")
print(df3)
print("DF4")
print(df4)
