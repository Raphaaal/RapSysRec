def split_early_late(driver):
    query = """
    MATCH (a: Artist)-[r:FEAT_NB]-(b: Artist)
    MATCH (a)-[r2:FEAT]->(b)
    WHERE (r.year % 2 = 0)
    WITH collect(distinct r2) as all_feats, a , b
    FOREACH (rel in all_feats | CREATE (a)-[:FEAT_EARLY]->(b))
    """

    with driver.session() as session:
        session.run(query)

    query = """
    MATCH (a: Artist)-[r:FEAT_NB]-(b: Artist)
    MATCH (a)-[r2:FEAT]->(b)
    WHERE NOT (r.year % 2 = 0)
    WITH collect(distinct r2) as all_feats, a , b
    FOREACH (rel in all_feats | CREATE (a)-[:FEAT_LATE]->(b))
    """

    with driver.session() as session:
        session.run(query)
