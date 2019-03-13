import cx_Oracle
import csv
import sys

if len(sys.argv) == 1:
    print(f'{sys.argv[1]} <URL database origin> <URL database destination>')
    print('URL = username/password@hostname:port/SID')
    exit

CONNECTION_EXPORT= sys.argv[1]
CONNECTION_IMPORT= sys.argv[2]

def main():
    connection = cx_Oracle.connect(CONNECTION_EXPORT, encoding='UTF-8', nencoding='UTF-8')    
    #connection = cx_Oracle.connect('VVS', 'vvs', cx_Oracle.makedsn('192.168.33.155', 1521, 'doze'), encoding='UTF-8', nencoding='UTF-8')
    cursor = connection.cursor()

    fileIn = open('data.csv', 'r')

    csvreader = csv.reader(fileIn, delimiter=';', lineterminator='\n', quotechar='"')
    
    for line in csvreader:
        exportLine(line, cursor)

    cursor.close()

    connection.close()


def exportLine(line, cursor):
    table_name = line[0]
    where_params = (f' where {line[1]}' if len(line) > 1 else '')

    print(f'Exporting table {table_name}')

    query = f'select * from {table_name} {where_params}'
    cursor.execute(query)

    fileOut = open(f'{table_name}.txt', 'w')

    writer = csv.writer(fileOut, lineterminator='|\n', delimiter=';', escapechar='', quoting=csv.QUOTE_NONNUMERIC)

    columns = [i[0] for i in cursor.description]

    createExternalTable(table_name, columns)

    writer.writerow(columns)

    for row in cursor:
        writer.writerow(row)

    fileOut.close()


def createExternalTable(table_name, columns):
    ddl = f""" 
    CREATE TABLE {table_name}_EXT
        (
    """

    for column in columns:
        ddl += f"""
        {column} varchar2(200),"""
    ddl = ddl[:-1]

    ddl += f"""
        )
            ORGANIZATION EXTERNAL
        (
            TYPE ORACLE_LOADER
            DEFAULT DIRECTORY "EVVS_FILES"
            ACCESS PARAMETERS
                (
                    RECORDS DELIMITED BY '|'
                    SKIP 1
                    FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '"'
                    MISSING FIELD VALUES ARE NULL
                    REJECT ROWS WITH ALL NUL FIELDS
                )
            LOCATION
            (
                '{table_name}.txt'
            )
        )
    """

    connection = cx_Oracle.connect(CONNECTION_IMPORT, encoding='UTF-8', nencoding='UTF-8')    
    
    cursor = connection.cursor()

    cursor.execute(ddl)

    print(ddl)


def trataResultado(cursorInternal):
    for result in cursorInternal:
        # print(type(result))
        print(result)
        for index in result:
            teste += f'{index};'
            print(index)
        teste += '|\n'

    print(teste)


main()