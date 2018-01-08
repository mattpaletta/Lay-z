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
    line:str = row.get_as_str("data")
    for word in line.split(" "):
        logging.debug("Splitting line!")
        yield Row({"word": word, "num": 1})


def count_words(rows):
    curr_count = {}
    for row in rows:
        word = row.get("word")
        count = row.get("num")
        if word in curr_count.keys():
            curr_count[word] = count
        else:
            curr_count[word] += count

    for key, value in curr_count.items():
        yield Row({key: value})


pride = "pride_and_prejudice.txt"
small_test = "small_test.txt"

df = Dataframe()
df2 = df.read_txt(file_name=small_test, col_name="data")\
    #.map_row(split_by_word).map_using(count_words)

print(df)