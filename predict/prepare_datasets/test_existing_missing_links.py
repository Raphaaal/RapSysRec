from neo4j_handler import Neo4JHandler
import pandas as pd
import logging
import numpy as np

logger = logging.getLogger('feature_engineering')
# logger.propagate = False
logging.basicConfig(level='INFO')

pd.set_option('display.float_format', lambda x: '%.3f' % x)

graph = Neo4JHandler(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="root"
)
driver = graph.driver


def get_test_set(max_links, driver):
    with driver.session() as session:
        # Get positive links (existing)

        result = session.run(
            """
            MATCH (a:Artist)-[:FEAT_LATE]-(b:Artist)
            RETURN id(a) AS node1, id(b) AS node2, 1 AS label
            """
        )
        train_existing_links = pd.DataFrame([dict(record) for record in result]).sample(max_links)
        logger.info("Testing existing links computed.")

        # Get negative links within 2 or 3 hops (missing)

        # Get random pairs of Artist nodes id
        result = session.run(
            """
            MATCH (a:Artist)
            RETURN DISTINCT id(a) AS node1, id(a) as node2
            """
        )
        all_pairs = pd.DataFrame([dict(record) for record in result])
        repeat_all_pairs = pd.concat([all_pairs] * (int(max_links/len(all_pairs.index)) + 100), ignore_index=True)
        repeat_all_pairs['node2'] = np.random.permutation(repeat_all_pairs[['node2']].values)
        repeat_all_pairs['concat'] = repeat_all_pairs["node1"].astype(str) + '-' + repeat_all_pairs["node2"].astype(str)
        concat_list = repeat_all_pairs['concat'].drop_duplicates().tolist()

        # Check that link is indeed missing
        result = session.run(
            """   
            MATCH (a:Artist) 
            WHERE (a)-[:FEAT_LATE]-()
            MATCH (a)-[:FEAT_LATE*2..3]-(b) 
            WHERE apoc.convert.toString(id(a)) + '-' + apoc.convert.toString(id(b)) IN $pairs_list 
            AND NOT((a)-[:FEAT_LATE]-(b)) 
            AND a <> b 
            RETURN DISTINCT id(a) AS node1, id(b) AS node2, 0 AS label
            LIMIT $max_missing_links * 2
            """,
            {"pairs_list": concat_list, "max_missing_links": max_links}
        )

        train_missing_links = pd.DataFrame([dict(record) for record in result])
        train_missing_links = train_missing_links.drop_duplicates().sample(max_links)
        logger.info("Testing missing links computed.")

    # Append existing and missing links
    training_df = train_missing_links.append(train_existing_links, ignore_index=True)
    training_df['label'] = training_df['label'].astype('category')

    # Downsampling the negative class
    # TODO: potential error... Mixing 0 and 1 classes because the biggest one is returned first
    count_class_0, count_class_1 = training_df.label.value_counts()
    print(f"Negative examples: {count_class_0}")
    print(f"Positive examples: {count_class_1}")
    df_class_0 = training_df[training_df['label'] == 0]
    df_class_1 = training_df[training_df['label'] == 1]
    df_class_0_under = df_class_0.sample(count_class_1)
    df_train_under = pd.concat([df_class_0_under, df_class_1], axis=0)
    print('Random downsampling:')
    print(df_train_under.label.value_counts())
    logger.info("Testing Pandas DataFrame missing links downsampled.")

    return df_train_under

