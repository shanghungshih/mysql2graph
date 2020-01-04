import pymysql
from neo4j import GraphDatabase

### mysql
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

    def get_allTables(self):
        self.cursor.execute("USE test")
        self.cursor.execute("SHOW TABLES")
        all_Tables = self.cursor.fetchall()
        return [all_Tables[i][0] for i in range(len(all_Tables))]

    def get_adj_list(self, all_Tables):
        adj_list = []
        for i in range(len(all_Tables)):
            adj_list.append([])
        
        for i in range(len(all_Tables)):
            self.cursor.execute("""SELECT TABLE_NAME, REFERENCED_TABLE_NAME, COLUMN_NAME, REFERENCED_COLUMN_NAME
                                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                                        WHERE `TABLE_NAME` = "%s" AND `TABLE_SCHEMA` = 'test'""" % all_Tables[i])
            ref = self.cursor.fetchall()
            
            for j in range(len(ref)): #adjacency list
                if not ref[j][1]:
                    pass
                else:
                    adj_list[i].append(all_Tables.index(ref[j][1]))
                    adj_list[all_Tables.index(ref[j][1])].append(i)
        return adj_list

### neo4j
class Neo4j_processor:
    def __init__(self, host, port, user, password):
        self.driver = GraphDatabase.driver("bolt://%s:%s" %(host, port), auth=(user, password))

def add(tx, node1_name, node2_name, relation):
    tx.run("MERGE (a:Node {name: $node1_name}) "
        "MERGE (a)-[:%s]->(neighbor:Node {name: $node2_name})" %(relation),
        node1_name=node1_name, node2_name=node2_name)

def printNode(tx, name):
    for record in tx.run("MATCH (a:Node)-[:cooccurence]->(neighbor) WHERE a.name = $name "
                        "RETURN neighbor.name ORDER BY neighbor.name", name=name):
        print(record["neighbor.name"])