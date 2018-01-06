from layz.Dataframe import Dataframe


def common_friends_example():
    myfriends = {
        "A": ["B", "C", "D"],
        "B": ["A", "C", "D", "E"],
        "C": ["A", "B", "D", "E"],
        "D": ["A", "B", "C", "E"],
        "E": ["B", "C", "D"]
    }

    df = Dataframe()
    for key, value in myfriends.items():
        row = {"me": key, "friends": value}
        df.add_row(row)

    df2 = df.explode_dict(me_col="me", friends_col="friends")
    print(list(df.row_manager))


if __name__ == "__main__":
    common_friends_example()