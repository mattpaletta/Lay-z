"""
WITH showEpisodes AS
(SELECT episodeof, avg(rank) as arank, avg(votes) as avotes,
count(*) as cepisodes, count(distinct seasons) as cseasons
FROM episodes NATURAL JOIN ratings
WHERE arank > 8.5 AND avotes > 1000 GROUP BY episodeof)

SELECT * FROM productions NATURAL JOIN showEpisodes WHERE attr=‘TV-Show’;

"""
from parser import Parser

# Return create objects.
from validator import Validator

if __name__ == "__main__":
    parser = Parser()
    validator = Validator()
    while True:
        query = input(">> ")
        if query == "exit" or query == "quit":
            break

        query = query \
            .replace("\t", " ") \
            .replace("\n", " ") \
            .split(" ")
        parsed = parser.parse(query)
        is_valid = validator.is_valid_query(query=parsed)
        if is_valid:
            full = validator.fill_in_query(query=parsed)
        else:
            print("Invalid query!")

        print(is_valid)
