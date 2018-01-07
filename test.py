import logging
import sys

from src.layz import Dataframe


def common_friends_example():
    myfriends = {
        "A": ["B", "C", "D"],
        "B": ["A", "C", "D", "E"],
        "C": ["A", "B", "D", "E"],
        "D": ["A", "B", "C", "E"],
        "E": ["B", "C", "D"]
    }

    df = Dataframe(lambda x: x)
    for key, value in myfriends.items():
        row = {"me": key, "friends": value}
        df.add_row(row)

    df2 = df.explode_dict(me_col="me", friends_col="friends")\
        .group_by_key(me_col="me", friends_col="friends")\
        .find_common_friends(me_col="me", friends_col="friends")\
        .limit(100)

    print(df2)

    print("Done!")

if __name__ == "__main__":
    root = logging.getLogger()
    root.setLevel(logging.NOTSET)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.NOTSET)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)s]')
    ch.setFormatter(formatter)
    root.addHandler(ch)

    common_friends_example()