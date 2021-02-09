import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
        conn.close()
    return conn


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        if query == "":
            return "Query Blank"
        else:
            cursor.execute(query)
            connection.commit()
            return "Query executed successfully"
    except Error as e:
        return "Error occurred: " + str(e)


def main():
    conn = create_connection('sec.db')
    query = """
        create table holdings (
        report_pd TEXT,
        filer TEXT,
        company TEXT,
        value REAL
        );
        """
    execute_query(conn, query)
    conn.close()


if __name__ == '__main__':
    main()
