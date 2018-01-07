# Import system libraries (for logging) and layz dataframe object.
import json
import logging
import sys

from layz.dataframe import Dataframe, INTERSECT
from layz.row import Row

# Initialize our logger
root = logging.getLogger()
root.setLevel(logging.NOTSET)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.NOTSET)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)s]')
ch.setFormatter(formatter)
root.addHandler(ch)

# define constants for our data
ME = "me"
FRIENDS = "friends"

# define our custom functions for this task:

def explode_dict(rows):
    logging.debug("Explode Looping over rows!")
    for row in rows:
        me = row.get(ME)
        my_friends = row.get(FRIENDS)
        for my_friend in my_friends:
            logging.debug("Exploding!" + str(my_friends))
            yield Row({ME: sorted([me, my_friend]), FRIENDS: sorted(my_friends)})

def group_friends(rows):
    # here we have to loop over all the items :(
    seen_friends = {}
    for row in rows:
        key = tuple(row.get(ME))
        value = row.get(FRIENDS)

        logging.debug("Grouping by: " + str(key))
        if key in seen_friends.keys():
            current_friends: list = seen_friends[key]
            current_friends.append(value)
        else:
            seen_friends.update({key: [value]})

    for key, value in seen_friends.items():
        yield Row({ME: key, FRIENDS: value})

def find_common_friends(rows):
    for row in rows:
        key = row.get(ME)
        value = row.get(FRIENDS)

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
        yield Row({ME: key, FRIENDS: list(intersection)})


# Our sample data.  Each item of the dictionary will be turned into a DataFrame Row.
with open("my_friends.json", "r") as friends_data:
    myfriends = json.load(friends_data)

# Add those items to a new dataframe (df), each as a new row.
df = Dataframe()
for key, value in myfriends.items():
    row = {"me": key, "friends": value}
    df.add_row(row)

# create a new dataframe after passing in our custom functions.
df_output = df.map_using(explode_dict)\
    .map_using(group_friends)\
    .map_using(find_common_friends)\
    .limit(100)

print(df_output)