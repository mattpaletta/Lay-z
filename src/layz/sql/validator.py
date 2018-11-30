import os

from sql.parser import SELECT, TABLE, COLUMN, INSERT, CREATE, SCHEMA


class Validator(object):

    def is_valid_query(self, query):
        print(query)

        is_valid = True

        for item in query:
            if type(item) == SELECT:
                """
                Check for recursively:
                - columns
                - table
                - order is valid
                - conditions
                """
                select: SELECT = item
                is_valid = is_valid and len(select.columns) > 0
                is_valid = is_valid and select.tbl is not None and select.tbl is not ""
                tbl: TABLE = select.tbl
                is_valid = is_valid and tbl.schema is not None and tbl.schema is not ""
                is_valid = is_valid and select.order in ["ASC", "DESC", None]

                print("TODO! Check conditions... [where clause]")
                is_valid = is_valid and self.is_valid_query(select.columns)
                is_valid = is_valid and self.is_valid_query([select.tbl])

            elif type(item) == INSERT:
                insert: INSERT = item
                is_valid = is_valid and self.is_valid_query([insert])

            elif type(item) == CREATE:
                create: CREATE = item

                if type(create.obj) == TABLE:
                    tbl: TABLE = create.obj
                    is_valid = is_valid and not self.is_valid_query([tbl])

                elif type(create.obj) == SCHEMA:
                    schma: SCHEMA = create.obj
                    is_valid = is_valid and not self.is_valid_query([schma])

            elif type(item) == COLUMN:
                col: COLUMN = item
                is_valid = is_valid and col.name is not "" and col.name is not None
                is_valid = is_valid and col.var_type in \
                           ["str", "int", "float", "long", "bool"]  # check for supported types.

            elif type(item) == TABLE:
                tbl: TABLE = item
                # check that the table exists.
                is_valid = is_valid and os.path.exists(tbl.get_path())

            elif type(item) == SCHEMA:
                schma: SCHEMA = item
                is_valid = is_valid and os.path.exists(schma.get_path())

            else:
                print("Warning! Invalid type in validation!")

        return is_valid


    # take all * and replace with all the columns for the table, etc...
    def fill_in_query(self, query):
        print("TODO! Expand query")

        """
        for item in query:
            if type(item) == SELECT:
                select: SELECT = item
                columns_to_add = []
                for column in select.columns:
                    if column.get_name() == "*":  # later maybe be regex?
                        pass
        """
        return query