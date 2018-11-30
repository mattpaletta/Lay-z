from unittest import TestCase
from layz.dataframe import Dataframe

# from examples.mutual_friends.mutual_friends import explode_dict


class TestExamples(TestCase):
    def test_common_friends(self):
        myfriends = {
            "A": ["B", "C", "D"],
            "B": ["A", "C", "D", "E"],
            "C": ["A", "B", "D", "E"],
            "D": ["A", "B", "C", "E"],
            "E": ["B", "C", "D"]
        }

        df = Dataframe(lambda x: x)
        # for key, value in myfriends.items():
        #     row = {"me": key, "friends": value}
        #     df.add_row(row)
        #
        # df2 = df.explode_dict(me_col="me", friends_col="friends")\
        #     .group_by_key(me_col="me", friends_col="friends")\
        #     .find_common_friends(me_col="me", friends_col="friends")\
        #     .limit(100)
