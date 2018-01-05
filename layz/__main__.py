"""
WITH showEpisodes AS
(SELECT episodeof, avg(rank) as arank, avg(votes) as avotes,
count(*) as cepisodes, count(distinct seasons) as cseasons
FROM episodes NATURAL JOIN ratings
WHERE arank > 8.5 AND avotes > 1000 GROUP BY episodeof)

SELECT * FROM productions NATURAL JOIN showEpisodes WHERE attr=‘TV-Show’;

"""



class DB_OBJECT(object):
    name = ""
    def __init__(self):
        name = ""

    def set_name(self, name):
        self.name = name

    def get_name(self):
        return self.name

class TABLE(DB_OBJECT):
    schema: DB_OBJECT
    table_def: {str: str}

class SCHEMA(DB_OBJECT):
    pass

class CREATE(object):
    obj: DB_OBJECT

    def __init__(self, obj):
        self.obj = obj

    def get_type(self):
        return self.obj.__class__

def parse(query):

    processed = []

    for index, word in enumerate(query):
        item = None
        if word.lower() == "create":
            item = handle_create(query[index:])
        elif word.lower() == "select":
            item = handle_select(query[index:])
        elif word.lower() == "insert":
            item = handle_insert(query[index:])

        # what else?
        elif word.lower() == "(":
            item = parse(query[index:])

        processed.append(item)

    return processed

# Return create objects.
def handle_create(query):
    create: CREATE = None
    for index, word in enumerate(query):
        if word.lower() == "table":
            # return a create table.
            create = handle_create_table(query[index:])
        elif word.lower() == "schema":
            # return a create schema.
            create = handle_create_schema(query[index:])

    return create

def handle_create_table(query):
    tbl: TABLE = None
    for index, word in enumerate(query):
        if "." in word:
            # first item is the schema, second is the table.
            schema_name, table_name = word.split(".")

            schema = SCHEMA()
            schema.set_name(schema_name)

            tbl = TABLE()
            tbl.schema = schema
            tbl.set_name(table_name)

        elif word.lower() == "like":
            print("ERROR: LIKE Not handled yet...")
            pass

    return tbl

def handle_create_schema(query):
    schma = SCHEMA()
    schma.set_name(query[0])
    return schma

def handle_select(query):
    return query

def handle_insert(query):
    return query

def create_db(db):
    # take in create object, create the db as a folder under 'tables' if not exists.
    pass

def create_table(tbl):
    # take in create object, create the table in 'tables'
    # (new folder for the table, under the schema)
    # create the schema first if not exists.
    # store the schema of the table as a CSV, for now.
    pass

if __name__ == "__main__":
    while(True):
        query = input(">> ")
        query = query \
            .replace("\t", " ") \
            .replace("\n", " ") \
            .split(" ")
        parsed = parse(query)
        print(parsed)