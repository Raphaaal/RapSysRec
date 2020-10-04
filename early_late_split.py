from neo4j_handler import Neo4JHandler

graph = Neo4JHandler(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="root"
)
driver = graph.driver

query = """
MATCH (a: Artist)-[r:FEAT_NB]-(b: Artist)
WHERE r.year <= 2013
MERGE (a)-[:FEAT_EARLY {year: r.year, nb_feats: r.nb_feats}]-(b);
"""

with driver.session() as session:
    session.run(query)

query = """
MATCH (a: Artist)-[r:FEAT_NB]-(b: Artist)
WHERE r.year > 2013
MERGE (a)-[:FEAT_LATE {year: r.year, nb_feats: r.nb_feats}]-(b);
"""

with driver.session() as session:
    session.run(query)
