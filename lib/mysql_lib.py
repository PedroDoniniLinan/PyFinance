import mysql.connector
import pandas as pd


def execute_query(query_name, query_type='file', mode='select'):
    cnx = mysql.connector.connect(
        host="localhost",
        user="pedro",
        password="tateYuusha!13",
        database="general_schema"
    )

    if query_type == 'file':
        with open(query_name, 'r') as file:
            query = file.read()
    else:
        query = query_name

    cursor = cnx.cursor()
    cursor.execute(query)
    df = 'Failed'
    if mode == 'select':
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=cursor.column_names)
    elif mode == 'update':
        cnx.commit()
        df = 'Success'
    elif mode == 'management':
        df = 'Success'
    cursor.close()
    cnx.close()

    return df


def list_row_values(row):
    result = []
    for val in row.values:
        if str(val) == 'nan':
            result.append('Null')
        else:
            result.append("'" + str(val).replace("'", "") + "'" )        
    return result


def generate_insert_query(df, table, keys, merge=True):
    merge = 'on duplicate key update {keys}' if merge else ''
    query = """
    insert into {table} 
    {columns} values {values} as src
    """ + merge + """; 
    """
    rows = []
    for i, row in df.iterrows():
        row_values = "\n(" + ", ".join(list_row_values(row)) + ")"
        rows.append(row_values)
    values_string = ", ".join(rows)

    query = query.format(
        table=table, 
        columns="\n(" + ", ".join(df.columns) + ")", 
        values=values_string, 
        keys=", ".join(k + '=src.' + k + '\n' for k in keys) if merge else ''
        )
    return query


def insert_df(df, table, keys, merge=True):
    query = generate_insert_query(df, table, keys, merge)
    with open('queries/debug_query.sql', 'w+') as f:
        # write the string to the file
        f.write(query)
    # print(query)
    execute_query(query, 'code', 'update')

