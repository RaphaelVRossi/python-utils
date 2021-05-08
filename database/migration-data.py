import cx_Oracle
import csv
import sys

if len(sys.argv) == 1:
    print(f'{sys.argv[1]} <URL database origin> <URL database destination>')
    print('URL = username/password@hostname:port/SID')
    sys.exit()

CONNECTION_EXPORT= sys.argv[1]

def main():
    with cx_Oracle.connect(CONNECTION_EXPORT, encoding='UTF-8', nencoding='UTF-8') as connection:
    #with cx_Oracle.connect('VVS', 'vvs', cx_Oracle.makedsn('192.168.33.155', 1521, 'doze'), encoding='UTF-8', nencoding='UTF-8') as connection:
        with connection.cursor() as cursor:
            with open('data.csv', 'r') as file_in:
                csvreader = csv.reader(file_in, delimiter=';', lineterminator='\n', quotechar='"')    
                for line in csvreader:
                    exportLine(line, cursor)

def exportLine(line, cursor):
    table_name = line[0]
    where_params = (f' where {line[1]}' if len(line) > 1 else '')

    print(f'Exporting table {table_name}')

    query = f'select * from {table_name} {where_params}'
    cursor.execute(query)

    with open(f'{table_name}.txt', 'w', encoding='utf-8') as file_out:
        writer = csv.writer(file_out, lineterminator='\n', delimiter=';', escapechar='\\', quoting=csv.QUOTE_ALL)
        
        columns = [i[0] for i in cursor.description]

        createExternalTable(table_name, columns)
        createTempTable(table_name)

        writer.writerow(columns)

        for row in cursor:
            #print(str(row))
            cacheRow = []
            for index, col in enumerate(row):
                newCol = col
                #print(isinstance(col, str) and '\t' in str(object=bytes(col, 'utf-8')))
                if isinstance(col, str): #and '\t' in str(object=bytes(col, 'utf-8')):
                    #print(col.replace('\t', 'XXXX'))
                    #print(f'has TAB in {col}')
                    newCol = col.replace('\t', '    ')
                    #print(f'{row[index]}')

                #print(f'{index} | {col}')
                cacheRow.append(newCol)

            writer.writerow(tuple(cacheRow))


def createExternalTable(table_name, columns):
    ddl = f"""
SET SERVEROUTPUT ON SIZE UNLIMITED
DECLARE
    V_SYSDATE DATE;

BEGIN

V_SYSDATE := SYSDATE;

DBMS_OUTPUT.PUT_LINE('INICIO CRIAÇÃO DA TABELA "{table_name}" EXTERNAL '|| V_SYSDATE);

CREATE TABLE {table_name}_EXT
    ("""

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
                RECORDS DELIMITED BY NEWLINE
                SKIP 1
                FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '"'
                MISSING FIELD VALUES ARE NULL
                REJECT ROWS WITH ALL NUL FIELDS
            )
        LOCATION
        (
            '{table_name}.txt'
        )
    );

V_SYSDATE := SYSDATE;

DBMS_OUTPUT.PUT_LINE('TERMINO CRIAÇÃO DA TABELA "{table_name}" EXTERNAL '|| V_SYSDATE);

END;
/
"""

    with open(f'CREATE_{table_name}_EXT.sql', 'w') as ddl_file_ext:
        ddl_file_ext.write(ddl)


def createTempTable(table_name):
    ddl = f"""
SET SERVEROUTPUT ON SIZE UNLIMITED
DECLARE
    V_SYSDATE DATE;

BEGIN

V_SYSDATE := SYSDATE;

DBMS_OUTPUT.PUT_LINE('INICIO CRIAÇÃO DA TABELA "{table_name}" TEMPORARIA '|| V_SYSDATE);

CREATE TABLE {table_name}_TMP
    AS
        SELECT * FROM {table_name}_EXT;

V_SYSDATE := SYSDATE;

DBMS_OUTPUT.PUT_LINE('TERMINO CRIAÇÃO DA TABELA "{table_name}" TEMPORARIA '|| V_SYSDATE);

END;
/
"""

    with open(f'CREATE_{table_name}_TMP.sql', 'w') as ddl_file_ext:
        ddl_file_ext.write(ddl)


main()