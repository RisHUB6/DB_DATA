import os
import mysql.connector


'''
Version == 1.0.0
It is a class that can give you whole table in the form of list of dictionary with a explicit key that
you will provide.
A simple approach inorder to implement less time on query while retreving the information from database.
'''

class DBDICT():
    mydb_local = None
    def __init__(self, host:str, user:str, password:str, database:str):
        try:
            self.mydb_local = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
        except Exception as e:
            print("Error while connecting to DB. ", e)
        self.all_db_data = []
    
    def get_db_data(self, tablename, hash_key, get_data=False):
        '''
        function to get all the dat from the table
        tablename: Table name of the database
        hash_key: Column of the table against you have to make a key value pair (it should be same as the table column name)
        get_data: Set it TRUE if you want the data in return to make further manipulations else it won't return anything
        '''
        if self.mydb_local:
            my_cursor = self.mydb_local.cursor(buffered=True, dictionary=True)
            all_query = f"select * from {tablename};"
            count_query = f"select count(*) from {tablename}"
            my_cursor.execute(count_query)
            user_count = my_cursor.fetchone()
            my_cursor.execute(all_query)
            user_data = my_cursor.fetchall()
            num_cpus = os.cpu_count()
            rows_per_section = user_count["count(*)"] // num_cpus
            
            ind = 0
            for _ in range(num_cpus):
                section_dict = {}
                for _ in range(rows_per_section):
                    section_dict[user_data[ind][hash_key]] = user_data[ind]
                    ind += 1
                self.all_db_data.append(section_dict)
            if get_data:
                return self.all_db_data
    
    def search_key(self, search_key):
        if self.mydb_local:
            for dic in self.all_db_data:
                if search_key in dic:
                    return (dic[search_key])
            return {}
        print("Database is not connected")



'''
A simple use case you all can try!
'''

# db = DBDICT(host="localhost", user="root", password="#password", database="test_db")
# val = db.get_db_data(tablename="user", hash_key="username", get_data=True)
# print(db.search_key(search_key="admin1@sharperax.com"))