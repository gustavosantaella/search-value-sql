import psycopg2
import argparse


def search_value_in_db(value_to_search, connection):
    cursor = connection.cursor()
    
    # Get all tables and columns in the database
    cursor.execute("""
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog');
    """)

    results = cursor.fetchall()

    for schema, table, column in results:
        query = f"""
            SELECT '{schema}' AS schema, '{table}' AS table, '{column}' AS column
            FROM {schema}.{table}
            WHERE CAST({column} AS TEXT) like '%{value_to_search}%'
            LIMIT 1;
        """
        
        try:
            # print(f"-----------\nsearching:\n schema: {schema}\n table:{table}\ncolumn:{column} \n-----------\n\n")
            cursor.execute(query)
            if cursor.fetchone():
                print(f"Value found in table '{table}', column '{column}' in schema '{schema}'")
            # else:
                # print("Value not found")
        except Exception as e:
            print(f"Error searching in column {column} of table {table}: {e}")

    cursor.close()

# Configure database connection
if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument("-d",  required=True)  # database
    args.add_argument("-H", required=True) #host
    args.add_argument("-p",  required=True) # port
    args.add_argument("-w",  required=True) # password
    args.add_argument("-u",  required=True) # user
    args.add_argument("-v",  required=False) # value
    
    variables = vars(args.parse_args())
    connection = psycopg2.connect(
        dbname= variables['d'],
        user=variables['u'],
        password=variables['w'],
        host=variables['H'],
        port=variables['p']
    )
    # connection.autocommit = True


        
    if variables['v']:
        print("Value to search:", variables['v'])
        value_to_search = variables['v']
        search_value_in_db(value_to_search, connection)

    connection.close()
