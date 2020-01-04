#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymysql

class MySQL_processor:
    def __init__(self, host, port, user, password, database=None):
        if database is None:
            self.cnx = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password
            )
        else:
            self.cnx = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )
        self.cursor = self.cnx.cursor()


# In[2]:


import pandas as pd

from utils import *

def build(host, port, user, password, database):
    connector = MySQL_processor(host, port, user, password)
    connector.cursor.execute('CREATE DATABASE IF NOT EXISTS %s' %(database))
    connector.cnx.close()

    connector = MySQL_processor(host, port, user, password, database)

    ### songs
    df = pd.read_csv('D:\\Users\\USER\\Desktop\\1.csv')
    table = 'songs'
    connector.cursor.execute("""
    CREATE TABLE IF NOT EXISTS `%s` (
        `song_id` varchar(255) NOT NULL,
        `song_length` int NULL,
        `genre_ids` varchar(255) NULL,
        `artist_name` varchar(255) NULL,
        `composer` varchar(255) NULL,
        `lyricist` varchar(255) NULL,
        `language` int NULL,
        PRIMARY KEY (`song_id`)
    ) ENGINE=InnoDB""" %(table))

    vals = []
    for row in df.itertuples(index=False, name=None):
        val = []
        for i in row:
            if str(i) == 'nan':
                val.append(None)
            else:
                i = i.strip() if type(i) is str else i
                val.append(i)
        vals.append(tuple(val))

    CMD = "INSERT INTO `%s` (`song_id`, `song_length`, `genre_ids`, `artist_name`, `composer`, `lyricist`, `language`) VALUES (%s) ON DUPLICATE KEY UPDATE song_id = `song_id`" %(table, '%s, %s, %s, %s, %s, %s, %s')
    connector.cursor.executemany(CMD, vals)
    connector.cnx.commit()

    ### members
    df = pd.read_csv('D:\\Users\\USER\\Desktop\\2.csv')
    table = 'members'
    connector.cursor.execute("""
    CREATE TABLE IF NOT EXISTS `%s` (
        `msno` varchar(255) NOT NULL,
        `city` int NULL,
        `bd` int NULL,
        `gender` varchar(255) NULL,
        `registered_via` int NULL,
        `registration_init_time` varchar(255) NULL,
        `expiration_date` varchar(255) NULL,
        PRIMARY KEY (`msno`)
    ) ENGINE=InnoDB""" %(table))

    vals = []
    for row in df.itertuples(index=False, name=None):
        val = []
        for i in row:
            if str(i) == 'nan':
                val.append(None)
            else:
                i = i.strip() if type(i) is str else i
                val.append(i)
        vals.append(tuple(val))

    CMD = "INSERT INTO `%s` (`msno`,`city`,`bd`,`gender`,`registered_via`,`registration_init_time`,`expiration_date`) VALUES (%s) ON DUPLICATE KEY UPDATE msno = `msno`" %(table, '%s, %s, %s, %s, %s, %s, %s')
    connector.cursor.executemany(CMD, vals)
    connector.cnx.commit()

    ### song_extra
    df = pd.read_csv('D:\\Users\\USER\\Desktop\\3.csv')
    table = 'song_extra'
    connector.cursor.execute("""
    CREATE TABLE IF NOT EXISTS `%s` (
        `song_id` varchar(255) NOT NULL,
        `name` varchar(255) NULL,
        `isrc` varchar(255) NULL,
        PRIMARY KEY (`song_id`)
    ) ENGINE=InnoDB""" %(table))

    vals = []
    for row in df.itertuples(index=False, name=None):
        val = []
        for i in row:
            if str(i) == 'nan':
                val.append(None)
            else:
                i = i.strip() if type(i) is str else i
                val.append(i)
        vals.append(tuple(val))

    CMD = "INSERT INTO `%s` (`song_id`,`name`,`isrc`) VALUES (%s) ON DUPLICATE KEY UPDATE song_id = `song_id`" %(table, '%s, %s, %s')
    connector.cursor.executemany(CMD, vals)
    connector.cnx.commit()

    ### train
    table = 'train'
    connector.cursor.execute("""
    CREATE TABLE IF NOT EXISTS `%s` (
        `msno` varchar(255) NOT NULL,
        `song_id` varchar(255) NOT NULL,
        `source_system_tab` varchar(255) NULL,
        `source_screen_name` varchar(255) NULL,
        `source_type` varchar(255) NULL,
        `target` int NULL,
        PRIMARY KEY (`msno`, `song_id`),
        FOREIGN KEY (`msno`) REFERENCES members(`msno`),
        FOREIGN KEY (`song_id`) REFERENCES songs(`song_id`),
        FOREIGN KEY (`song_id`) REFERENCES song_extra(`song_id`)    
    ) ENGINE=InnoDB""" %(table))

    df = pd.read_csv('D:\\Users\\USER\\Desktop\\0.csv')
    vals = []
    for row in df.itertuples(index=False, name=None):
        val = []
        for i in row:
            if str(i) == 'nan':
                val.append(None)
            else:
                i = i.strip() if type(i) is str else i
                val.append(i)
        vals.append(tuple(val))

    CMD = "INSERT INTO `%s` (`msno`, `song_id`, `source_system_tab`, `source_screen_name`, `source_type`, `target`) VALUES (%s) ON DUPLICATE KEY UPDATE msno = `msno`" %(table, '%s, %s, %s, %s, %s, %s')
    connector.cursor.executemany(CMD, vals)
    connector.cnx.commit()

    connector.cnx.close()

def drop(host, port, user, password, database):
    connector = MySQL_processor(host, port, user, password)
    connector.cursor.execute('DROP DATABASE %s' %(database))
    connector.cnx.close()

def main():
    # MySQL 連線資訊（改成自己 local 環境）
    host='127.0.0.1'
    port=3306
    user='root'
    password='DIRK2reina4ever'
    database='test'

    build(host, port, user, password, database)
    # drop(host, port, user, password, database)   # drop database

if __name__ == '__main__':
    main()


# In[3]:


host='127.0.0.1'
port=3306
user='root'
password='DIRK2reina4ever'
database='test'

connector = MySQL_processor(host, port, user, password)


# In[4]:


connector.cursor.execute("USE test")
connector.cursor.execute("SHOW TABLES")
all_Tables = connector.cursor.fetchall()


# In[5]:


connector.cursor.execute("""SELECT TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME
                            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                            WHERE REFERENCED_TABLE_SCHEMA = 'test' AND REFERENCED_TABLE_NAME = 'members'""")
connector.cursor.fetchall()


# In[6]:


all_Tables = [all_Tables[i][0] for i in range(len(all_Tables))]
all_Tables


# In[7]:


adj_list = []
for i in range(len(all_Tables)):
    adj_list.append([])

for i in range(len(all_Tables)):
    connector.cursor.execute("""SELECT TABLE_NAME, REFERENCED_TABLE_NAME, COLUMN_NAME, REFERENCED_COLUMN_NAME
                                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                                WHERE `TABLE_NAME` = "%s" AND `TABLE_SCHEMA` = 'test'""" % all_Tables[i])
    ref = connector.cursor.fetchall()
    
    for j in range(len(ref)): #adjacency list
        if not ref[j][1]:
            pass
        else:
            adj_list[i].append(all_Tables.index(ref[j][1]))
            adj_list[all_Tables.index(ref[j][1])].append(i)
    
adj_list


# In[40]:


import pandas as pd
import sqlalchemy as sql


# In[62]:


visit = [0 for i in range(len(all_Tables))]
df_list = []
fn_cnt = 0

def traversal():
    
    for i in range(len(all_Tables)):
        cnt = 0
        if not visit[i]:
            DFS(i, cnt)
            create_df()
        

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
        
traversal()


# In[65]:


import pandas as pd
import csv

def readFromCsv(fileName):
    table=[]
    with open(fileName,newline='',encoding='UTF-8') as csvfile:
        rows=csv.reader(csvfile,delimiter=',');
        for row in rows:
            table_tuple=[]
            for attr in row:
                table_tuple.append(attr.strip())
            table.append(table_tuple)
    return table


def recommandColumn(dfin,maxShowNode=1000):
    lst=[]
    for i in range(dfin.shape[1]):
        refineList=set(list(dfin.iloc[:,i]))
        lst.append(len(refineList))
        
    setNum=[];
    for i in range(len(lst)):
        setNum.append((lst[i],i))
    setNum.sort()
    list1=[]
    nodeNum=0;
    maxColumn=0;
    for i in range(len(setNum)):
        list1.append(setNum[i][1])
        nodeNum=nodeNum+setNum[i][0]
        if nodeNum>maxShowNode:
            break
        maxColumn=i
    return list1

def recommandColumnByName(dfin,maxShowNode=1000):
    lst=recommandColumn(dfin,maxShowNode)
    headList=list(dfin.head())
    recommandName=[]
    for index in lst:
        recommandName.append(headList[index])
    return recommandName
    
    


# ls=readFromCsv('test data/testdata.csv')

# df=pd.read_csv('test data/testdata.csv')
df = df_list[0]
print('推薦:',recommandColumn(df))
print('推薦(列名):',recommandColumnByName(df))



# In[123]:


def createEdge(dfIn,colList):
    lst = dict()
    comb = combinations(colList, 2)    
    for i in comb:
        df2 = df.iloc[:,list(i)]
        col1 = df2.columns[0]
        col2 = df2.columns[1]
        lst[str(col1) + '-' + str(col2)] = [tuple(x) for x in df2.values]
    return lst


# In[131]:


edgeDict = createEdge(df, recommandColumn(df))
edgeDict.keys()
# for key in edgeDict.keys(): # check if tuples are all in
#     if len(edgeDict[key]) != df.shape[0]:
#         print(key)
#     else:
#         print(key, len(edgeDict[key]))


# In[132]:


edgeDict['target-registered_via']


# In[127]:


def createNode(dfIn,colList):
    lst = dict()
    
    for i in colList:
        node = set(dfIn.iloc[:, i])
        lst[dfIn.columns[i]] = node
        
    return lst


# In[128]:


nodeDict = createNode(df, recommandColumn(df))
nodeDict.keys()


# In[ ]:





# In[ ]:




