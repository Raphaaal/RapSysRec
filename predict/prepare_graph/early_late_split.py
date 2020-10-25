def split_early_late(driver, min_year):
    query = """
    MATCH (a: Artist)-[r:FEAT_NB]-(b: Artist)
    MATCH (a)-[r2:FEAT]->(b)
    WHERE (r.year % 2 = 0) AND r.year >= $min_year
    WITH collect(distinct r2) as all_feats, a , b
    FOREACH (rel in all_feats | CREATE (a)-[:FEAT_EARLY]->(b))
    """

    with driver.session() as session:
        session.run(
            query,
            {'min_year': min_year}
        )

    query = """
    MATCH (a: Artist)-[r:FEAT_NB]-(b: Artist)
    MATCH (a)-[r2:FEAT]->(b)
    WHERE NOT (r.year % 2 = 0) AND r.year >= $min_year
    WITH collect(distinct r2) as all_feats, a , b
    FOREACH (rel in all_feats | CREATE (a)-[:FEAT_LATE]->(b))
    """

    with driver.session() as session:
        session.run(
            query,
            {'min_year': min_year}
        )

    # Delete relationships older than min_year
    query = """
        MATCH (a: Artist)-[r:FEAT_NB]-(b: Artist)
        MATCH (a)-[r2:FEAT]->(b)
        WHERE r.year < $min_year
        WITH collect(distinct r2) as old_feats, a , b
        FOREACH (rel in old_feats | DELETE rel)
        """

    with driver.session() as session:
        session.run(
            query,
            {'min_year': min_year}
        )
