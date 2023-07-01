#2020510039 Berkay GÜZEL - 2020510009 Akif Selim ARSLAN
import csv
import psycopg2 #py -m pip install psycopg2
from psycopg2 import errorcodes

# PostgreSQL veritabanı bağlantısı
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="123456",
    port= "5432"
)

# Veritabanı bağlantısı üzerinden bir cursor oluşturulur
conn.autocommit = True
cursor = conn.cursor()

# students adında bir veritabanı oluşturulur
try:
    cursor.execute("CREATE DATABASE students")
except psycopg2.errors.DuplicateDatabase as error:
        if error.pgcode == errorcodes.DUPLICATE_DATABASE:
            print("database already exist.")

# students veritabanına bağlantı yapılır
conn.close()
conn = psycopg2.connect(
    host="localhost",
    database="students",
    user="postgres",
    password="123456",
    port = "5432"
)
conn.autocommit = True
cursor = conn.cursor()

# ogrenci.csv dosyasının sütunlarını kullanarak students tablosu oluşturulur
rows = []
with open('students.csv', 'r') as file:
    csv_data = csv.reader(file,delimiter=";")
    header = next(csv_data)
    rows = list(csv_data)
sortedlist = sorted(rows, key=lambda x: int(x[0]))

# create_table = "CREATE TABLE students ({});".format(
#     ', '.join(['{} TEXT'.format(column) for column in header])
#     )
# cursor.execute(create_table)

createTable = """CREATE TABLE students(
id INTEGER,
name TEXT,
lastname TEXT,
email TEXT,
grade INTEGER)"""

try:
    cursor.execute(createTable)
    existance= False
except psycopg2.errors.DuplicateTable as error:
    if error.pgcode == errorcodes.DUPLICATE_TABLE:
        existance = True
        print("Table already exist.")
    
    # CSV dosyasındaki her satırı students tablosuna ekler
if existance == False:
    for row in sortedlist:
        insert_query = "INSERT INTO students ({}) VALUES ({});".format(
        ', '.join(header),
        ', '.join(["%s"] * len(header))
        )
        datalist = list(row)
        cursor.execute(insert_query, datalist)
    

def checkSelect(query):
    input_list = query.lower().split()

    if len(input_list) < 4:
        return False
    if input_list[0] != "select":
        return False

    if input_list[1] != "all":
        columns = input_list[1].split(',')
        if len(columns) == 0 :
            return False
        else:
            for i in range(len(columns)):
                if columns[i] not in ["id", "name", "lastname", "email", "grade"]:
                    return False

    if input_list[2] != "from":
        return False

    if input_list[3] != "students":
        return False

    index = 4
    while index < len(input_list):
        if input_list[index] == "where":
            index += 1
            if index >= len(input_list):
                return False

            if input_list[index] not in ["id", "name", "lastname", "email", "grade"]:
                return False

            index += 1
            if index >= len(input_list):
                return False
            
            if input_list[index - 1] in ["name", "lastname", "email"]:
                if input_list[index] not in ['=', '!=']:
                    return False
                
            if input_list[index] not in ['=', '!=', '<', '>', '<=', '>=', '!<', '!>']:
                return False

            index += 1
            if index >= len(input_list):
                return False

            if input_list[index].isdigit():
                pass  # Sayı geçerli, devam et

            elif input_list[index].startswith("'") and input_list[index].endswith("'"):
                # String geçerli, ancak sadece = veya != kullanılabilir
                if input_list[index - 1] not in ['=', '!=']:
                    return False
            else:
                return False  # Geçerli sayı veya string değil

        elif input_list[index] == "and" or input_list[index] == "or":
            index += 1
            if index >= len(input_list):
                return False

            if input_list[index] not in ["id", "name", "lastname", "email", "grade"]:
                return False

            index += 1
            if index >= len(input_list):
                return False

            if input_list[index - 1] in ["name", "lastname", "email"]:
                if input_list[index] not in ['=', '!=']:
                    return False
            
            if input_list[index] not in ['=', '!=', '<', '>', '<=', '>=', '!<', '!>']:
                return False

            index += 1
            if index >= len(input_list):
                return False

            if input_list[index].isdigit():
                pass  # Sayı geçerli, devam et

            elif input_list[index].startswith("'") and input_list[index].endswith("'"):
                
                # String geçerli, ancak sadece = veya != kullanılabilir
                if input_list[index - 1] not in ['=', '!=']:
                    return False
            else:
                return False  # Geçerli sayı veya string değil

        elif input_list[index] == "order":
            index += 1
            if index >= len(input_list):
                return False

            if input_list[index] != "by":
                return False

            index += 1
            if index >= len(input_list):
                return False

            if input_list[index] not in ["asc", "dsc"]:
                return False
            break
        else:
            return False
        index += 1
    return True

def checkinsert(query):
    querylist2 = query.lower().split()
    if len(querylist2) == 5:
        if querylist2[0] != "insert" and querylist2[1] != "into" and querylist2[2] != "students" and querylist2[3] != "values":
            return False
        else: 
            if querylist2[4].startswith('(') and querylist2[4].endswith(')'):
                value = querylist2[4].strip(querylist2[4][0]).strip(querylist2[4][-1])
                values = value.split(',')
                if len(values) != 5:
                    return False
                else:
                    if values[0].isdigit() and values[4].isdigit():
                        if not values[1].isdigit() and not values[2].isdigit() and not values[3].isdigit():
                            return True
                        else:
                            return False
                    else:
                        return False
            else:
                return False
    else:
        return False 

def checkdelete(query):
    input_list = query.lower().split()

    if len(input_list) < 7:
        return False
    if input_list[0] != "delete":
        return False

    if input_list[1] != "from":
        return False

    if input_list[2] != "students":
        return False

    index = 3
    while index < len(input_list):
        if input_list[index] == "where":
            index += 1
            if index >= len(input_list):
                return False

            if input_list[index] not in ["id", "name", "lastname", "email", "grade"]:
                return False

            index += 1
            if index >= len(input_list):
                return False
            
            if input_list[index - 1] in ["name", "lastname", "email"]:
                if input_list[index] not in ['=', '!=']:
                    return False
                
            if input_list[index] not in ['=', '!=', '<', '>', '<=', '>=', '!<', '!>']:
                return False

            index += 1
            if index >= len(input_list):
                return False

            if input_list[index].isdigit():
                pass  # Sayı geçerli, devam et

            elif input_list[index].startswith("'") and input_list[index].endswith("'"):
                # String geçerli, ancak sadece = veya != kullanılabilir
                if input_list[index - 1] not in ['=', '!=']:
                    return False
            else:
                return False  # Geçerli sayı veya string değil

        elif input_list[index] == "and" or input_list[index] == "or":
            index += 1
            if index >= len(input_list):
                return False

            if input_list[index] not in ["id", "name", "lastname", "email", "grade"]:
                return False

            index += 1
            if index >= len(input_list):
                return False

            if input_list[index - 1] in ["name", "lastname", "email"]:
                if input_list[index] not in ['=', '!=']:
                    return False
            
            if input_list[index] not in ['=', '!=', '<', '>', '<=', '>=', '!<', '!>']:
                return False

            index += 1
            if index >= len(input_list):
                return False

            if input_list[index].isdigit():
                pass  # Sayı geçerli, devam et

            elif input_list[index].startswith("'") and input_list[index].endswith("'"):
                
                # String geçerli, ancak sadece = veya != kullanılabilir
                if input_list[index - 1] not in ['=', '!=']:
                    return False
            else:
                return False  # Geçerli sayı veya string değil

        else:
            return False
        index += 1
    return True    
    
    
    
def selectfromtable(query):
    queryList = query.lower().split(" ")
    if checkSelect(query):
        if queryList[1] == "all":
            queryList[1] = '*'
        if queryList[len(queryList) - 1] == "dsc":
            queryList[len(queryList) - 1] = "DESC"
        elif queryList[len(queryList) - 1] == "asc":
            queryList[len(queryList) - 1] = "ASC"    
        for i in range(len(queryList)):
            if queryList[i] == '=':
                if queryList[i-1] == "name" or queryList[i-1] == "lastname" or queryList[i-1] == "email":
                    queryList[i] = "ILIKE"
            if queryList[i] == '!=':
                if queryList[i-1] == "name" or queryList[i-1] == "lastname" or queryList[i-1] == "email":
                    queryList[i] = "NOT ILIKE"
            if queryList[i] == '!<':
                queryList[i] = '>='
            if queryList[i] == '!>':
                queryList[i] = '<='
            
            
        if len(queryList) == 15: #SELECT name FROM STUDENTS WHERE grade > 40 AND name = ‘John’ ORDER BY DSC
            query_sel = "SELECT {0} FROM students WHERE {1} {2} {3} {4} {5} {6} {7} ORDER BY id {8}".format(queryList[1],queryList[5],queryList[6],queryList[7],queryList[8],queryList[9],queryList[10],queryList[11],queryList[14])
            return query_sel
        elif len(queryList) == 12: #SELECT name FROM STUDENTS WHERE grade > 40 AND name = ‘John’
            query_sel = "SELECT {0} FROM students WHERE {1} {2} {3} {4} {5} {6} {7}".format(queryList[1],queryList[5],queryList[6],queryList[7],queryList[8],queryList[9],queryList[10],queryList[11])
            return query_sel        
        elif len(queryList) == 11: #SELECT name FROM STUDENTS WHERE grade > 40 ORDER BY DSC
            query_sel = "SELECT {0} FROM students WHERE {1} {2} {3} ORDER BY id {4}".format(queryList[1],queryList[5],queryList[6],queryList[7],queryList[10])
            return query_sel
        elif len(queryList) == 8: #SELECT name FROM STUDENTS WHERE grade > 40 
            query_sel = "SELECT {0} FROM students WHERE {1} {2} {3}".format(queryList[1],queryList[5],queryList[6],queryList[7])
            return query_sel
        elif len(queryList) == 4:
            query_sel = "SELECT {0} FROM students".format(queryList[1])
            return query_sel
    else:
        
        return None
    
def insertTable(query):
    if checkinsert(query):
        queryl = query.split()
        value = queryl[4].strip(queryl[4][0]).strip(queryl[4][-1])
        values = value.split(',')
        for i in range(1,4):
            if not "'" in values[i]:
                values[i] = "'" + values[i] + "'"   
         
        insertquery = "INSERT INTO students (id,name,lastname,email,grade) VALUES ({},{},{},{},{})".format(values[0],values[1],values[2],values[3],values[4])
        cursor.execute(insertquery)
        print("values inserted to students table")
    else:
        print("your query is in incorrect form")
        print("query must be in following form: insert into students values (number,text,text,text,number)")

def deletefromTable(query):
    if checkdelete(query):
        queryy = query.split()
        if len(queryy) == 7:
            deletequery = "DELETE FROM STUDENTS WHERE {} {} {}".format(queryy[4],queryy[5],queryy[6])
            cursor.execute(deletequery) 
            conn.commit()
            print("Values deleted.")
        elif len(queryy) == 11:
            deletequery = "DELETE FROM STUDENTS WHERE {} {} {} {} {} {} {}".format(queryy[4],queryy[5],queryy[6],queryy[7],queryy[8],queryy[9],queryy[10])
            cursor.execute(deletequery) 
            conn.commit()
            print("Values deleted.")
    else:
        print("your query is in incorrect form")
                      


while True:
    query = input("Enter a SQL query: ")
    if query == "exit":
        print("!!!EXIT!!!")
        break
    
    queryList = query.lower().split(" ")
    
    if queryList[0] == "select":
        querysel = selectfromtable(query)
        if querysel != None:
            cursor.execute(querysel)
            qu = cursor.fetchall()
            for q in qu:
                print(q)
        else:
            print("query format is incorrect.")
            
    elif queryList[0] == "insert":
        insertTable(query)
            
    
    elif queryList[0] == "delete":
        deletefromTable(query)

    else:
        print("incorrect form of query")
    
