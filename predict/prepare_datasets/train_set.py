from handlers.csv_handler import write_list_to_csv, truncate_file
# from neo4j_handler import Neo4JHandler
import pandas as pd
import logging
# import numpy as np
# import itertools
# from random import shuffle
from tqdm import tqdm
from neo4j import unit_of_work
import neo4j


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('feature_engineering')
# logger.propagate = False

pd.set_option('display.float_format', lambda x: '%.3f' % x)


@unit_of_work(timeout=5)
def _get_missing_links(tx, artist_id, batch_size):
    result = tx.run(
        """
        MATCH (a:Artist)-[r:FEAT]-(:Artist)-[:FEAT*0..2]-(b:Artist) WHERE id(a) = $artist_id
        AND NOT ((a)-[:FEAT_2020]-(b)) AND a <> b AND NOT (r.track_name ENDS WITH "t")
        RETURN DISTINCT id(a) AS node1, id(b) AS node2, 0 AS label,  a.urn as node1_urn, b.urn as node2_urn
        LIMIT $batch_size
        """,
        {
            "artist_id": artist_id,
            "batch_size": batch_size
        }
    )
    return [row for row in result]


def get_train_set(max_links, batch_size, target_year, driver):

    # Reset datasets
    truncate_file('training_existing_links.csv')
    truncate_file('training_all_ids.csv')
    truncate_file('training_missing_links.csv')
    write_list_to_csv([['node1', 'node2', 'label']], 'training_missing_links.csv')

    # Get existing links
    with driver.session() as session:
        result = session.run(
            """
            MATCH (a:Artist)-[r:FEAT_2020]-(b:Artist)
            WHERE a <> b
            // We use the track name to exclude links that will be in the test set
            AND NOT (r.track_name ENDS WITH "t")
            RETURN DISTINCT id(a) AS node1, id(b) AS node2, 1 AS label, rand() as r, a.urn as node1_urn, b.urn as node2_urn
            ORDER BY r ASC
            LIMIT $max_links
            """,
            {'target_year': target_year, 'max_links': max_links}
        )
        train_existing_links = pd.DataFrame([dict(record) for record in result])
        train_existing_links.to_csv('training_existing_links.csv', index=False)
    driver.close()
    logger.info("Training existing links computed.")

    # Get missing links within 3 hops
    with driver.session() as session:
        result = session.run(
            """
            MATCH (a:Artist)
            RETURN DISTINCT id(a) AS node
            """
        )
        all_ids = pd.DataFrame([dict(record)['node'] for record in result]).drop_duplicates()
        all_ids.to_csv('training_all_ids.csv', index=False)
    driver.close()

    # Get pairs of Artist nodes id by checking that link is indeed missing
    artists_ids = [x[0] for x in pd.read_csv('training_all_ids.csv').values.tolist()]
    for artist_id in tqdm(artists_ids):
        try:
            with driver.session() as session:
                result = session.read_transaction(_get_missing_links, artist_id, batch_size)
                train_missing_links = pd.DataFrame([dict(record) for record in result])
                train_missing_links.to_csv('training_missing_links.csv', mode='a', header=False, index=False)
                session.close()
        except neo4j.exceptions.ClientError:
            pass
    logger.info("Training missing links computed.")

    # Append existing and missing links
    train_existing_links = pd.read_csv('training_existing_links.csv')
    train_missing_links = pd.read_csv('training_missing_links.csv').drop_duplicates().sample(max_links)
    training_df = train_missing_links.append(train_existing_links, ignore_index=True).drop_duplicates()
    training_df['label'] = training_df['label'].astype('category')

    count_class_0, count_class_1 = training_df.label.value_counts()
    print(f"Negative examples: {count_class_0}")
    print(f"Positive examples: {count_class_1}")

    return training_df
