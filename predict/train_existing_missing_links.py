from neo4j_handler import Neo4JHandler
import pandas as pd
import logging

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


def get_train_set():
    with driver.session() as session:
        # Get positive links (existing)
        result = session.run(
            """
            MATCH (a:Artist)-[:FEAT_EARLY]-(b:Artist)
            RETURN id(a) AS node1, id(b) AS node2, 1 AS label
            """
        )
        train_existing_links = pd.DataFrame([dict(record) for record in result])

        # Get negative links within 2 or 3 hops (missing)
        result = session.run(
            """
            MATCH (a:Artist)
            WHERE (a)-[:FEAT_EARLY]-()
            MATCH (a)-[:FEAT_EARLY*2..3]-(b)
            WHERE not((a)-[:FEAT_EARLY]-(b))
            RETURN id(a) AS node1, id(b) AS node2, 0 AS label
            """
        )

        train_missing_links = pd.DataFrame([dict(record) for record in result])
        train_missing_links = train_missing_links.drop_duplicates()

    training_df = train_missing_links.append(train_existing_links, ignore_index=True)
    training_df['label'] = training_df['label'].astype('category')
    logger.info("Training Pandas DataFrame computed.")

    # Counts
    count_class_0, count_class_1 = training_df.label.value_counts()
    print(f"Negative examples: {count_class_0}")
    print(f"Positive examples: {count_class_1}")

    # Downsampling the negative class
    df_class_0 = training_df[training_df['label'] == 0]
    df_class_1 = training_df[training_df['label'] == 1]

    df_class_0_under = df_class_0.sample(count_class_1)
    df_train_under = pd.concat([df_class_0_under, df_class_1], axis=0)
    logger.info("Training Pandas DataFrame negative links downsampled.")

    print('Random downsampling:')
    print(df_train_under.label.value_counts())

    return df_train_under
