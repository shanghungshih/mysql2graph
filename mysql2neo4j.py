from utils import *
import sqlalchemy as sql

def DFS(strt, count):
    
    visit[strt] = 1
    
    for i in range(len(adj_list[strt])):
        if not visit[adj_list[strt][i]]:
            #print(adj_list[strt][i])
            
            connector.cursor.execute("""SELECT REFERENCED_TABLE_NAME, COLUMN_NAME, REFERENCED_COLUMN_NAME
                                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                                        WHERE `TABLE_NAME` = "%s" AND `TABLE_SCHEMA` = 'test'""" % all_Tables[adj_list[strt][i]])
            
            refered = connector.cursor.fetchall() #refered table
             
            for num in range(len(refered)): #merge
                if not refered[num][0]: # if none pass
                    pass
                else: #merge
                    #print(refered[num])
                    #print(all_Tables[adj_list[strt][i]], refered[num][0])
                    
                    if count == 0: # first time
                        connector.cursor.execute("""
                                                    CREATE TABLE tp AS
                                                    SELECT * FROM {table1}
                                                    LEFT OUTER JOIN {table2}
                                                    USING ({col1})
                                                    UNION
                                                    SELECT * FROM {table1}
                                                    RIGHT OUTER JOIN {table2}
                                                    USING ({col2})
                                                    WHERE {table1}.{col1} IS NULL
                                                    """.format(table1 = all_Tables[adj_list[strt][i]], 
                                                                                            table2 = refered[num][0], 
                                                                                            col1 = refered[num][1], 
                                                                                            col2 = refered[num][2]))
                        
                    elif not count % 2: 
                        connector.cursor.execute("""
                                                    CREATE TABLE tp AS
                                                    SELECT * FROM {table1}
                                                    LEFT OUTER JOIN {table2} USING ({col1})
                                                    UNION ALL
                                                    SELECT * FROM {table1}
                                                    RIGHT OUTER JOIN {table2} USING ({col2})
                                                    WHERE {table1}.{col1} IS NULL""".format(table1 = 'tp2', 
                                                                                            table2 = refered[num][0], 
                                                                                            col1 = refered[num][1], 
                                                                                            col2 = refered[num][2]))
                        connector.cursor.execute("""DROP TABLE tp2""")
                    else:
                        connector.cursor.execute("""
                                                    CREATE TABLE tp2 AS
                                                    SELECT * FROM {table1}
                                                    LEFT OUTER JOIN {table2} USING ({col1})
                                                    UNION ALL
                                                    SELECT * FROM {table1}
                                                    RIGHT OUTER JOIN {table2} USING ({col2})
                                                    WHERE {table1}.{col1} IS NULL""".format(table1 = 'tp', 
                                                                                            table2 = refered[num][0], 
                                                                                            col1 = refered[num][1], 
                                                                                            col2 = refered[num][2]))                       
                        connector.cursor.execute("""DROP TABLE tp""")
                    count += 1
                    
            DFS(adj_list[strt][i], count)

def create_df():
    engine = sql.create_engine("mysql+mysqlconnector://dirk514121:DIRK2reina4ever@localhost/test") # dataframe support alchemy
    
    for table in ['tp', 'tp2']:
        try:
            connector.cursor.execute("""SHOW FIELDS FROM %s""" % table)
            col_list = connector.cursor.fetchall()
            cols = [col_list[i][0] for i in range(len(col_list))]
        
            SQL_Query = pd.read_sql_query('''SELECT * FROM %s''' % table, engine)
            df = pd.DataFrame(SQL_Query, columns = cols)
    
            df_list.append(df)
    
            connector.cursor.execute("""DROP TABLES %s""" % table)
            connector.cursor.fetchall()
        except:
            pass

class Mysql2neo4j:
    def __init__(self, mysqlInfo, neo4jInfo=None):
        self.mysql = MySQL_processor(mysqlInfo['host'], mysqlInfo['port'], mysqlInfo['user'], mysqlInfo['password'], mysqlInfo['database'])
        self.neo4j = Neo4j_processor(neo4jInfo['host'], neo4jInfo['port'], neo4jInfo['user'], neo4jInfo['password'])
        self.df

        visit = [0 for i in range(len(all_Tables))]
        df_list = []
        fn_cnt = 0

        for i in range(len(all_Tables)):
            cnt = 0
            if not visit[i]:
                DFS(self.mysql, i, cnt)
                create_df()

        def func1(self):
            pass

def test(args):
    return args


mysqlInfo = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'test',
    'database': 'test'
}

neo4jInfo = {
    'host': '127.0.0.1',
    'port': 7687,
    'user': 'neo4j',
    'password': 'neo4j'
}

m = Mysql2neo4j(mysqlInfo, neo4jInfo)