from layz.Dataframe import Dataframe


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

    df2 = df.explode_dict(me_col="me", friends_col="friends").limit(10)
    #df3 = df2.group_by_key(me_col="me", friends_col="friends")
    #df4 = df3.find_common_friends(me_col="me", friends_col="friends")\
    #.limit(10)

    print(list(df.row_manager))
    print(list(df2.row_manager))

    print("Done!")

if __name__ == "__main__":
    common_friends_example()