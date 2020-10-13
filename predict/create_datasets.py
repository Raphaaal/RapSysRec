from neo4j_handler import Neo4JHandler
from predict.feature_engineering import get_artist_specific_pdf, engineer_features
from predict.test_existing_missing_links import get_test_set
from predict.train_existing_missing_links import get_train_set
import pandas as pd

if __name__ == '__main__':
    # Graph DB connection
    graph = Neo4JHandler(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="root"
    )
    driver = graph.driver

    # train_set = get_train_set(max_links=1000, driver=driver)
    # test_set = get_test_set(max_links=1000, driver=driver)

    train_set = pd.read_csv('train_set.csv')
    test_set = pd.read_csv('test_set.csv')

    train_set, test_set = engineer_features(driver, train_set=train_set, test_set=test_set)
    train_set.to_csv('train_set.csv')
    test_set.to_csv('test_set.csv')

    # All pairs in the 2nd or 3rd hop of the considered artist AND with more than 56 + 3 = 59 total neighbors
    # Thus, some pairs may not be sampled and they will not be proposed in the predictions at the end
    artist_set = get_artist_specific_pdf(artist_urn='5gs4Sm2WQUkcGeikMcVHbh', min_nb_tn=59.0, driver=driver)
    artist_set.to_csv('artist_set.csv')
