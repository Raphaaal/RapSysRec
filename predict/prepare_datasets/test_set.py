from handlers.csv_handler import write_list_to_csv, truncate_file
# from neo4j_handler import Neo4JHandler
import pandas as pd
import logging
# import numpy as np
# import itertools
# from random import shuffle

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('feature_engineering')
# logger.propagate = False

pd.set_option('display.float_format', lambda x: '%.3f' % x)


def get_test_set(max_links, density, batch_size, target_year, driver):
    with driver.session() as session:

        # Reset datasets
        truncate_file('testing_existing_links.csv')
        truncate_file('testing_missing_links.csv')
        write_list_to_csv([['node1', 'node2', 'label']], 'testing_missing_links.csv')

        # Get existing links
        nb_samples = 10
        if int(max_links*density) > 0:
            nb_samples = int(max_links*density)

        result = session.run(
            """
            MATCH (a:Artist)-[r:FEAT_2020]-(b:Artist)
            WHERE a <> b 
            // We use the track name to isolate links that will be in the test set
            AND r.track_name ENDS WITH "t"
            RETURN DISTINCT id(a) AS node1, id(b) AS node2, 1 AS label, rand() as r
            ORDER BY r ASC
            LIMIT $max_links
            """,
            {'target_year': target_year, 'max_links': nb_samples}
        )
        test_existing_links = pd.DataFrame([dict(record) for record in result])
        test_existing_links.to_csv('testing_existing_links.csv', index=False)
        logger.info("Training existing links computed.")

        # Get missing links within 4 hops
        artists_ids = [x[0] for x in pd.read_csv('training_all_ids.csv').values.tolist()]

        # Get pairs of Artist nodes id by checking that link is indeed missing
        for artist_id in artists_ids:
            result = session.run(
                """
                MATCH (a:Artist)-[r:FEAT]-()-[:FEAT*0..2]-(b:Artist) WHERE id(a) = $artist_urn
                AND NOT ((a)-[r:FEAT_2020]-(b)) AND a <> b AND (r.track_name ENDS WITH "t")
                RETURN DISTINCT id(a) AS node1, id(b) AS node2, 0 AS label
                LIMIT $batch_size
                """,
                {
                    "artist_urn": artist_id,
                    "batch_size": batch_size
                }
            )

            test_missing_links = pd.DataFrame([dict(record) for record in result])
            test_missing_links.to_csv('testing_missing_links.csv', mode='a', header=False, index=False)

        logger.info("Testing missing links computed.")

    # Append existing and missing links
    test_missing_links = pd.read_csv('testing_missing_links.csv').drop_duplicates().sample(max_links)
    testing_df = test_missing_links.append(test_existing_links, ignore_index=True).drop_duplicates()
    testing_df['label'] = testing_df['label'].astype('category')

    count_class_0, count_class_1 = testing_df.label.value_counts()
    print(f"Negative examples: {count_class_0}")
    print(f"Positive examples: {count_class_1}")

    return testing_df
