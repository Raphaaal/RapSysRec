from neo4j_handler import Neo4JHandler

graph = Neo4JHandler(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="root"
)
driver = graph.driver


def split_early_late(date):
    query = """
    MATCH (a: Artist)-[r:FEAT_NB]-(b: Artist)
    MATCH (a)-[r2:FEAT]->(b)
    WHERE r.year <= $date
    WITH collect(distinct r2) as all_feats, a , b
    FOREACH (rel in all_feats | CREATE (a)-[:FEAT_EARLY]->(b))
    """

    with driver.session() as session:
        session.run(query, parameters={'date': date})

    query = """
    MATCH (a: Artist)-[r:FEAT_NB]-(b: Artist)
    MATCH (a)-[r2:FEAT]->(b)
    WHERE r.year > $date
    WITH collect(distinct r2) as all_feats, a , b
    FOREACH (rel in all_feats | CREATE (a)-[:FEAT_LATE]->(b))
    """

    with driver.session() as session:
        session.run(query, parameters={'date': date})
