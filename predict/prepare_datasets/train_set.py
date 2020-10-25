import random

from csv_handler import write_list_to_csv, truncate_file
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


def get_train_set(max_links, batch_size, driver):
    with driver.session() as session:

        # Reset datasets
        truncate_file('training_existing_links.csv')
        truncate_file('training_all_ids.csv')
        truncate_file('random_pairs_early.csv')
        write_list_to_csv([['node1', 'node2']], 'random_pairs_early.csv')
        truncate_file('random_pairs_early_tested.csv')
        write_list_to_csv([['node1', 'node2']], 'random_pairs_early_tested.csv')
        truncate_file('training_missing_links.csv')
        write_list_to_csv([['node1', 'node2', 'label']], 'training_missing_links.csv')

        # Get existing links

        result = session.run(
            """
            MATCH (a:Artist)-[:FEAT_EARLY]-(b:Artist)
            RETURN id(a) AS node1, id(b) AS node2, 1 AS label
            """
        )
        train_existing_links = pd.DataFrame([dict(record) for record in result]).sample(max_links)
        train_existing_links.to_csv('training_existing_links.csv', index=False)
        logger.info("Training existing links computed.")

        # Get missing links within 2 or 3 hops

        result = session.run(
            """
            MATCH (a:Artist)-[:FEAT_EARLY*2..4]-()
            RETURN DISTINCT id(a) AS node
            """
        )
        all_ids = pd.DataFrame([dict(record)['node'] for record in result]).drop_duplicates()
        all_ids.to_csv('training_all_ids.csv', index=False)

        # Get random pairs of Artist nodes id
        while len(pd.read_csv('training_missing_links.csv').drop_duplicates().index) < max_links:
            print(len(pd.read_csv('training_missing_links.csv').drop_duplicates().index))
            artists_ids = [x[0] for x in pd.read_csv('training_all_ids.csv').values.tolist()]
            tested_pairs_df = pd.read_csv('random_pairs_early_tested.csv')
            tested_pairs = set()
            if not tested_pairs_df.empty:
                tested_pairs = set([tuple(x) for x in tested_pairs_df.to_numpy()])
            tested_pairs_hist = tested_pairs.copy()
            tmp_pairs = set()
            while len(tmp_pairs) < batch_size:
                node1 = random.choice(artists_ids)
                node2 = random.choice(artists_ids)
                if not (node1, node2) in tested_pairs_hist:
                    tmp_pairs.add((node1, node2))
                    tested_pairs.add((node1, node2))
            truncate_file('random_pairs_early.csv')
            write_list_to_csv([['node1', 'node2']], 'random_pairs_early.csv')
            write_list_to_csv(tmp_pairs, mode='a', csv_path='random_pairs_early.csv')
            write_list_to_csv(tested_pairs, mode='a', csv_path='random_pairs_early_tested.csv')
            logger.info("Random training pairs set computed.")

            # Check that link is indeed missing
            for chunk in pd.read_csv('random_pairs_early.csv', chunksize=max_links):
                pairs = [{"node1": node1, "node2": node2} for node1, node2 in chunk[["node1", "node2"]].values.tolist()]
                result = session.run(
                    """  
                    UNWIND $pairs as pair 
                    MATCH (p1) WHERE id(p1) = pair.node1
                    MATCH (p2) WHERE id(p2) = pair.node2
                    AND (p1)-[:FEAT_EARLY*2..4]-(p2) 
                    AND NOT((p1)-[:FEAT_EARLY]-(p2)) 
                    AND p1 <> p2
                    RETURN id(p1) AS node1, id(p2) AS node2, 0 AS label
                    LIMIT $max_missing_links
                    """,
                    {"pairs": pairs, "max_missing_links": max_links}
                )

                train_missing_links = pd.DataFrame([dict(record) for record in result]).drop_duplicates()
                train_missing_links.to_csv('training_missing_links.csv', mode='a', header=False, index=False)

        logger.info("Training missing links computed.")

    # Append existing and missing links
    train_missing_links = pd.read_csv('training_missing_links.csv').drop_duplicates().sample(max_links)
    training_df = train_missing_links.append(train_existing_links, ignore_index=True)
    training_df['label'] = training_df['label'].astype('category')

    count_class_0, count_class_1 = training_df.label.value_counts()
    print(f"Negative examples: {count_class_0}")
    print(f"Positive examples: {count_class_1}")

    return training_df
