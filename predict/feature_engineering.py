from neo4j_handler import Neo4JHandler
import pandas as pd
from predict.train_existing_missing_links import get_train_set
from predict.test_existing_missing_links import get_test_set

pd.set_option('display.float_format', lambda x: '%.3f' % x)

graph = Neo4JHandler(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="root"
)
driver = graph.driver


def apply_graphy_features(data, rel_type, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
           pair.node2 AS node2,
           gds.alpha.linkprediction.commonNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS cn,
           gds.alpha.linkprediction.preferentialAttachment(p1, p2, {
             relationshipQuery: $relType}) AS pa,
           gds.alpha.linkprediction.totalNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS tn
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]

    with driver_instance.session() as session:
        result = session.run(query, {"pairs": pairs, "relType": rel_type})
        features = pd.DataFrame([dict(record) for record in result])
    return pd.merge(data, features, on=["node1", "node2"])


def engineer_features(driver):
    # Generate train and test sets
    df_train_under = get_train_set()
    df_test_under = get_test_set()
    df_train_under = apply_graphy_features(df_train_under, "FEAT_EARLY", driver)
    df_test_under = apply_graphy_features(df_test_under, "FEAT_LATE", driver)

    return df_train_under, df_test_under
