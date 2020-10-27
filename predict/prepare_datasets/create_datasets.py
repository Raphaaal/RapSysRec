from handlers.csv_handler import truncate_file
from handlers.neo4j_handler import Neo4JHandler
import pandas as pd
from predict.prepare_datasets.feature_engineering import engineer_features
from predict.prepare_datasets.test_set import get_test_set
from predict.prepare_datasets.train_set import get_train_set
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('prepare_datasets')
# logger.propagate = False


def write_features(path, iteration, driver, dataset):
    train_set_chunk = engineer_features(driver=driver, dataset=dataset)
    if iteration == 0:
        train_set_chunk.to_csv(path, mode='w', header=True, index=False)
    else:
        train_set_chunk.to_csv(path, mode='a', header=False, index=False)


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

    # Build train dataset
    logger.info("Started training set creation")
    train_set = get_train_set(max_links=100, batch_size=2, target_year=2020, driver=driver)
    logger.info("Ended training set creation")
    train_set.to_csv('train_set.csv', index=False)

    # Build test dataset
    logger.info("Started testing set creation")
    train_set = get_test_set(max_links=100, density=0.01, batch_size=2, target_year=2020, driver=driver)
    logger.info("Ended testing set creation")
    train_set.to_csv('test_set.csv', index=False)

    # Build validation dataset
    logger.info("Started validation set creation")
    validation_set = get_test_set(max_links=100, density=0.01, batch_size=2, target_year=2020, driver=driver)
    logger.info("Ended validation set creation")
    validation_set.to_csv('validation_set.csv', index=False)

    truncate_file('train_set_features.csv')
    for i, chunk in enumerate(pd.read_csv('train_set.csv', chunksize=1)):
        write_features(path='train_set_features.csv', iteration=i, driver=chunk, dataset=chunk)
    logger.info('Train set with features computed')

    truncate_file('validation_set_features.csv')
    for i, chunk in enumerate(pd.read_csv('validation_set.csv', chunksize=1)):
        write_features(path='validation_set_features.csv', iteration=i, driver=chunk, dataset=chunk)
    logger.info('Validation set with features computed')

    truncate_file('test_set_features.csv')
    for i, chunk in enumerate(pd.read_csv('test_set.csv', chunksize=1)):
        write_features(path='test_set_features.csv', iteration=i, driver=chunk, dataset=chunk)
    logger.info('Test set with features computed')

    # Create full set
    truncate_file('full_set_features.csv')
    full_set = pd.read_csv('train_set_features.csv').append(
        pd.read_csv('test_set_features.csv'), ignore_index=True
    ).append(
        pd.read_csv('validation_set_features.csv'), ignore_index=True
    )
    full_set.to_csv('full_set_features.csv')
    logger.info('Full set computed')

    # All pairs in the 2..4th hop of the considered artist
    # Thus, some pairs may not be sampled and they will not be proposed in the predictions at the end
    truncate_file('artist_set.csv')
    write_artist_specific_set(artist_urn='1afjj7vSBkpIjkiJdSV6bV', path='artist_set.csv', driver=driver)
    logger.info('Artist set computed')

    truncate_file('artist_set_features.csv')
    for i, chunk in enumerate(pd.read_csv('artist_set.csv', chunksize=1)):
        write_features(path='artist_set_features.csv', iteration=i, driver=chunk, dataset=chunk)
    logger.info('Artist set with features computed')
