import os


class DB_OBJECT(object):
    name = ""

    def __init__(self):
        name = ""

    def set_name(self, name):
        self.name = name

    def get_name(self):
        return self.name


class COLUMN(DB_OBJECT):
    var_type = "str"
    tbl_col = ""
    alias = ""


class SCHEMA(DB_OBJECT):
    path = ""

    def get_path(self):
        return os.path.join("..", "tables", self.get_name())


class TABLE(DB_OBJECT):
    schema: SCHEMA
    table_def: {str: str}
    path = ""

    def get_path(self):
        return os.path.join(self.schema.get_path(), self.get_name())


class INSERT(object):
    tbl: TABLE


class SELECT(object):
    columns: [DB_OBJECT] = []  # could be another select statement.
    tbl: [DB_OBJECT] = []
    conditions = [] # TODO!
    order = None # ASC, DESC, None


class CREATE(object):
    obj: DB_OBJECT

    def __init__(self, obj):
        self.obj = obj

    def get_type(self):
        return self.obj.__class__


class Parser(object):
    def parse(self, query):

        processed = []

        for index, word in enumerate(query):
            item = None
            if word.lower() == "create":
                item = self.handle_create(query[index+1:])
            elif word.lower() == "select":
                item = self.handle_select(query[index+1:])
            elif word.lower() == "insert":
                item = self.handle_insert(query[index+1:])

            # what else?
            elif word.lower() == "(":
                item = self.parse(query[index:])

            if item is not None:
                processed.append(item)

        return processed

    def handle_create(self, query):
        to_create = None
        for index, word in enumerate(query):
            if word.lower() == "table":
                # return a create table.
                to_create = self.handle_create_table(query[index:])
            elif word.lower() == "schema":
                # return a create schema.
                to_create = self.handle_create_schema(query[index:])

        create = CREATE(to_create)

        return create

    def handle_create_table(self, query):
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

    def handle_create_schema(self, query):
        schma = SCHEMA()
        schma.set_name(query[0])

        return schma

    def handle_select(self, query):
        # "COLUMNS [as blah]... scm.tbl [JOIN tbl] [WHERE ...] GROUP BY [blah...] [LIMIT X] [ORDER BY ASC]"
        select = SELECT()
        columns, query = self.handle_columns(query)
        select.columns = columns
        select.order = None

        is_table = False
        last_where_index = -1
        for index, word in enumerate(query):
            if last_where_index is not -1 and index < last_where_index:
                continue # already processed those ones, so just skip over them.
            elif word.lower() == "from":
                is_table = True
                last_where_index = -1
            elif word.lower() == "join":
                print("Join not implemented!")
                last_where_index = -1
            elif word.lower() == "group":
                print("Group by not implemented!")
                last_where_index = -1
            elif word.lower() == "limit":
                print("limit not implemented!")
                last_where_index = -1
            elif word.lower() == "order":
                print("order not implemented")
                last_where_index = -1  # got to the bottom, reset the where clause

            elif word.lower() == "where":
                conditions, last_where_index = handle_where(query[index+1:])
                select.conditions = conditions
            else:
                if is_table:
                    schma_name, tbl_name = word.split(".")
                    # found the table!
                    tbl = TABLE()
                    schma = SCHEMA()

                    tbl.name = tbl_name
                    schma.name = schma_name

                    tbl.schema = schma
                    select.tbl = tbl
                    is_table = False

        return select

    def handle_columns(self, query):
        columns = []
        is_alias = False
        for index, word in enumerate(query):
            # col_name [as] [blah]

            if word.lower() == "as":
                is_alias = True
            elif word.lower() == "from":
                break
            else:
                if is_alias:
                    is_alias = False
                    # update last item
                    last_column = columns.pop()
                    last_column.alias = word
                    columns.append(last_column)
                else:
                    last_column = COLUMN()
                    last_column.name = word
                    last_column.alias = word
                    # push onto list.
                    columns.append(last_column)

        return columns, query

    def handle_insert(self, query):
        return query

    def handle_where(self, query):
        max_index = 0
        #for index, word in enumerate(query):
            #max_index = index
            #break

        return max_index, []

    def create_db(self, db):
        # take in create object, create the db as a folder under 'tables' if not exists.
        pass

    def create_table(self, tbl):
        # take in create object, create the table in 'tables'
        # (new folder for the table, under the schema)
        # create the schema first if not exists.
        # store the schema of the table as a CSV, for now.
        pass