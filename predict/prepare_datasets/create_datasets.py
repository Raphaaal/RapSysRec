from neo4j_handler import Neo4JHandler
import pandas as pd
from predict.prepare_datasets.feature_engineering import engineer_features, get_artist_specific_features
from predict.prepare_datasets.test_existing_missing_links import get_test_set
from predict.prepare_datasets.train_existing_missing_links import get_train_set


def get_artist_specific_set(artist_urn, driver):
    with driver.session() as session:

        # Get negative links within 2 or 3 hops (missing)
        result = session.run(
            """
            MATCH (a:Artist {urn: $artist_urn})
            WHERE (a)-[:FEAT]-()
            MATCH (a)-[:FEAT*2..3]-(b)
            WHERE NOT ((a)-[:FEAT]-(b)) AND a <> b
            RETURN DISTINCT id(a) AS node1, id(b) AS node2, 0 AS label
            """
            , parameters={
                "artist_urn": artist_urn,
            }
        )

        artist_missing_links = pd.DataFrame([dict(record) for record in result])
        return artist_missing_links.drop_duplicates()


if __name__ == '__main__':
    # Graph DB connection
    graph = Neo4JHandler(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="root"
    )
    driver = graph.driver

    train_set = get_train_set(max_links=5000, driver=driver)
    train_set.to_csv('train_set.csv')
    test_set = get_test_set(max_links=1000, driver=driver)
    test_set.to_csv('test_set.csv')

    for i, chunk in enumerate(pd.read_csv('train_set.csv', chunksize=5)):
        train_set_chunk = engineer_features(driver, dataset=chunk, train=True)
        if i == 0:
            train_set_chunk.to_csv('train_set_features.csv', mode='w', header=True)
        else:
            train_set_chunk.to_csv('train_set_features.csv', mode='a', header=False)

    for i, chunk in enumerate(pd.read_csv('test_set.csv', chunksize=1)):
        test_set_chunk = engineer_features(driver, dataset=chunk, train=False)
        if i == 0:
            test_set_chunk.to_csv('test_set_features.csv', mode='w', header=True)
        else:
            test_set_chunk.to_csv('test_set_features.csv', mode='a', header=False)

    # All pairs in the 2nd or 3rd hop of the considered artist
    # Thus, some pairs may not be sampled and they will not be proposed in the predictions at the end
    artist_set = get_artist_specific_set(artist_urn='5gs4Sm2WQUkcGeikMcVHbh', driver=driver)
    artist_set.to_csv('artist_set.csv')
    for i, chunk in enumerate(pd.read_csv('artist_set.csv', chunksize=1)):
        artist_set_chunk = get_artist_specific_features(artist_df=chunk, min_nb_tn=0.0, driver=driver)
        if i == 0:
            artist_set_chunk.to_csv('artist_set_features.csv', mode='w', header=True)
        else:
            artist_set_chunk.to_csv('artist_set_features.csv', mode='a', header=False)
