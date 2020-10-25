from csv_handler import truncate_file
from neo4j_handler import Neo4JHandler
import pandas as pd
from predict.prepare_datasets.feature_engineering import engineer_features, get_artist_specific_features
from predict.prepare_datasets.test_set import get_test_set
from predict.prepare_datasets.train_set import get_train_set
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('prepare_datasets')
# logger.propagate = False


def write_artist_specific_set(artist_urn, path, driver):
    with driver.session() as session:

        # Get negative links within 2..3 hops (missing)
        result = session.run(
            """
            MATCH (a:Artist {urn: $artist_urn})
            MATCH (a)-[:FEAT*2..3]-(b)
            WHERE NOT ((a)-[:FEAT]-(b)) AND a <> b
            RETURN DISTINCT id(a) AS node1, id(b) AS node2, 0 AS label
            """
            , parameters={
                "artist_urn": artist_urn,
            }
        )
        artist_missing_links = pd.DataFrame([dict(record) for record in result]).drop_duplicates()
        artist_missing_links.to_csv(path, index=False)


if __name__ == '__main__':
    # Graph DB connection
    graph = Neo4JHandler(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="root"
    )
    driver = graph.driver

    logger.info("Started training set creation")
    train_set = get_train_set(max_links=10000, batch_size=2, driver=driver)
    logger.info("Ended training set creation")
    train_set.to_csv('train_set.csv')

    logger.info("Started testing set creation")
    test_set = get_test_set(max_links=10000,  batch_size=4, driver=driver)
    test_set.to_csv('test_set.csv')
    logger.info("Ended testing set creation")

    truncate_file('train_set_features.csv')
    for i, chunk in enumerate(pd.read_csv('train_set.csv', chunksize=1)):
        train_set_chunk = engineer_features(driver, dataset=chunk, train=True)
        if i == 0:
            train_set_chunk.to_csv('train_set_features.csv', mode='w', header=True, index=False)
        else:
            train_set_chunk.to_csv('train_set_features.csv', mode='a', header=False, index=False)
    logger.info('Train set with features computed')

    truncate_file('test_set_features.csv')
    for i, chunk in enumerate(pd.read_csv('test_set.csv', chunksize=1)):
        test_set_chunk = engineer_features(driver, dataset=chunk, train=False)
        if i == 0:
            test_set_chunk.to_csv('test_set_features.csv', mode='w', header=True, index=False)
        else:
            test_set_chunk.to_csv('test_set_features.csv', mode='a', header=False, index=False)
    logger.info('Test set with features computed')

    # Create full set
    truncate_file('full_set_features.csv')
    full_set = pd.read_csv('train_set_features.csv').append(pd.read_csv('test_set_features.csv'), ignore_index=True)
    full_set.to_csv('full_set_features.csv')
    logger.info('Full set computed')

    # All pairs in the 2..4th hop of the considered artist
    # Thus, some pairs may not be sampled and they will not be proposed in the predictions at the end
    truncate_file('artist_set.csv')
    truncate_file('artist_set_features.csv')
    write_artist_specific_set(artist_urn='5gs4Sm2WQUkcGeikMcVHbh', path='artist_set.csv', driver=driver)
    logger.info('Artist set computed')
    for i, chunk in enumerate(pd.read_csv('artist_set.csv', chunksize=1)):
        artist_set_chunk = get_artist_specific_features(artist_df=chunk, min_nb_tn=0.0, driver=driver)
        if i == 0:
            artist_set_chunk.to_csv('artist_set_features.csv', mode='w', header=True, index=False)
        else:
            artist_set_chunk.to_csv('artist_set_features.csv', mode='a', header=False, index=False)
    logger.info('Artist set with features computed')
