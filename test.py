from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j"))

def addNode(tx, node_label, node_name):
    tx.run("CREATE (a:%s {name: $node_name})" %(node_label), node_name=node_name)

def addRelation(tx, node1_label, node2_label, node1_name, node2_name, relation):
    tx.run("""MATCH (a:%s), (b:%s) WHERE a.name = "%s" AND b.name = "%s" CREATE (a)-[r:%s]->(b) RETURN r""" %(node1_label, node2_label, node1_name, node2_name, relation))

def add(tx, node1_name, node2_name, relation):
    tx.run("MERGE (a:Node {name: $node1_name}) "
           "MERGE (a)-[:%s]->(neighbor:Node {name: $node2_name})" %(relation),
           node1_name=node1_name, node2_name=node2_name)

def printNode(tx, name):
    for record in tx.run("MATCH (a:Node)-[:cooccurence]->(neighbor) WHERE a.name = $name "
                         "RETURN neighbor.name ORDER BY neighbor.name", name=name):
        print(record["neighbor.name"])

uniqs_0 = set()
uniqs_1 = set()
with open('age_artist.csv') as f:
    for row in f:
        sep = row.strip('\n').split(',')
        uniqs_0.add(sep[1])
        uniqs_1.add(sep[2])

with driver.session() as session:
    for i in uniqs_0:
        session.write_transaction(addNode, 'age', i)
    for i in uniqs_1:
        session.write_transaction(addNode, 'singer', i)

    f = open('age_artist.csv')
    for row in f:
        sep = row.strip('\n').split(',')
        session.write_transaction(addRelation, 'age', 'singer', sep[1], sep[2], "cooccurence")
        # session.read_transaction(printNode, "0")
driver.close()