from neo4j_handler import Neo4JHandler
import pandas as pd
from predict.prepare_graph.early_late_split import split_early_late
import logging

logger = logging.getLogger('feature_engineering')
logger.propagate = False
logging.basicConfig(level='INFO')

pd.set_option('display.float_format', lambda x: '%.3f' % x)

graph = Neo4JHandler(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="root"
)
driver = graph.driver


def get_artist_specific_features(artist_df, min_nb_tn, driver):
    logger.info("Starting to compute artist %s's Pandas DataFrame.")
    artist_df['label'] = artist_df['label'].astype('category')
    logger.info('Artist pairs computed.')

    # Calculate features
    artist_df = extract_same_label_feature(artist_df, driver)
    artist_df = extract_same_genre_feature(artist_df, driver)
    artist_df = extract_popularity_diff_feature(artist_df, driver)
    artist_df = apply_graphy_features(artist_df, "FEAT", driver)  # We use the standard "FEAT" relationship type
    # Filter out pairs with < min_nb_tn total neighbors (because the next computations are intensive)
    artist_df = artist_df.loc[artist_df['tn'] >= min_nb_tn]
    artist_df = apply_triangles_features(artist_df, "triangles", "coefficient", driver)
    artist_df = apply_community_features(artist_df, "partition", "louvain", driver)

    logger.info("Artist %s's Pandas DataFrame computed.")
    return artist_df


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

    logger.info('Calculated graphy features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_triangles_features(data, triangles_prop, coefficient_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN 
    pair.node1 AS node1,
    pair.node2 AS node2,
    apoc.coll.min([p1[$trianglesProp], p2[$trianglesProp]]) AS minTriangles,
    apoc.coll.max([p1[$trianglesProp], p2[$trianglesProp]]) AS maxTriangles,
    apoc.coll.min([p1[$coefficientProp], p2[$coefficientProp]]) AS minCoefficient,
    apoc.coll.max([p1[$coefficientProp], p2[$coefficientProp]]) AS maxCoefficient
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
    "pairs": pairs,
    "trianglesProp": triangles_prop,
    "coefficientProp": coefficient_prop
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    result = pd.merge(data, features, on=["node1", "node2"])
    logger.info('Calculated triangles features.')
    return result


def apply_community_features(data, partition_prop, louvain_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
    pair.node2 AS node2,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $partitionProp) AS sp,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvainProp) AS sl
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
    "pairs": pairs,
    "partitionProp": partition_prop,
    "louvainProp": louvain_prop
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated community features.')
    return pd.merge(data, features, on=["node1", "node2"])


def extract_same_label_feature(data, driver_instance):
    # TODO: this function does not seem to work when working on the same id for p1 and p2
    query = """
        UNWIND $pairs AS pair
        MATCH (p1) WHERE id(p1) = pair.node1
        MATCH (p2) WHERE id(p2) = pair.node2
        MATCH (l:Label) WHERE (p1)--(l)--(p2)
        RETURN 
        pair.node1 AS node1, 
        pair.node2 AS node2,
        COUNT(DISTINCT l) AS nb_common_labels
        """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    # Some dataset may not have any label information
    if not features.empty:
        same_label = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])
        same_label = same_label.fillna(0.0)
    else:
        same_label = data[["node1", "node2"]]
        same_label['nb_common_labels'] = 0.0

    logger.info('Calculated same label feature.')
    return pd.merge(data, same_label, on=["node1", "node2"])


def extract_same_genre_feature(data, driver_instance):
    # TODO: this function does not seem to work when working on the same id for p1 and p2
    query = """
        UNWIND $pairs AS pair
        MATCH (p1) WHERE id(p1) = pair.node1
        MATCH (p2) WHERE id(p2) = pair.node2
        MATCH (g:Genre) WHERE (p1)--(g)--(p2)
        RETURN 
        pair.node1 AS node1, 
        pair.node2 AS node2,
        COUNT(DISTINCT g) AS nb_common_genres
        """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    # Some dataset may not have any label information
    if not features.empty:
        same_label = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])
        same_label = same_label.fillna(0.0)
    else:
        same_label = data[["node1", "node2"]]
        same_label['nb_common_genres'] = 0.0

    logger.info('Calculated same genre feature.')
    return pd.merge(data, same_label, on=["node1", "node2"])


def extract_popularity_diff_feature(data, driver_instance, default_pop=50):
    query = """
        UNWIND $pairs AS pair
        MATCH (p1) WHERE id(p1) = pair.node1 
        MATCH (p2) WHERE id(p2) = pair.node2
        RETURN DISTINCT
        pair.node1 AS node1, 
        pair.node2 AS node2,
        CASE 
            WHEN p1.popularity IS NOT NULL AND p2.popularity IS NOT NULL THEN abs(p1.popularity - p2.popularity)
            ELSE $default_pop 
            END AS squared_popularity_diff
        """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "default_pop": default_pop
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    # Some dataset may not have any popularity information
    if not features.empty:
        same_label = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])
        same_label = same_label.fillna(0.0)
    else:
        same_label = data[["node1", "node2"]]
        same_label['squared_popularity_diff'] = 0.0

    logger.info('Calculated popularity difference feature.')
    return pd.merge(data, same_label, on=["node1", "node2"])


def engineer_features(driver, dataset, train=True):

    # TODO: Algo : Intégrer la récence des arcs et le label (de l'album et de l'artiste [label de son dernier album]) encodé avec un poids fort selon l'année
    # -> Besoin de dupliquer les arcs (car pas de prise en compte du weight dans les algos GDS) ou bien de faire du feature engineering a part (same_label: true / false) ?

    # TODO: add feature feat_percentage = nb tracks with ft / nb total tracks from the artist
    # TODO : add feature for recency
    # TODO: use transaction functions (session.write_transactions or session.read_transaction) instead of auto-commit transactions (session.run)

    # Generate train or test sets
    if train:
        df_train_under = dataset
        df_train_under = extract_same_label_feature(df_train_under, driver)
        df_train_under = extract_same_genre_feature(df_train_under, driver)
        df_train_under = extract_popularity_diff_feature(df_train_under, driver)
        df_train_under = apply_graphy_features(df_train_under, "FEAT_EARLY", driver)
        df_train_under = apply_triangles_features(df_train_under, "trianglesTrain", "coefficientTrain", driver)
        df_train_under = apply_community_features(df_train_under, "partitionTrain", "louvainTrain", driver)
        return df_train_under
    else:
        df_test_under = dataset
        df_test_under = extract_same_label_feature(df_test_under, driver)
        df_test_under = extract_same_genre_feature(df_test_under, driver)
        df_test_under = extract_popularity_diff_feature(df_test_under, driver)
        df_test_under = apply_graphy_features(df_test_under, "FEAT_LATE", driver)
        df_test_under = apply_triangles_features(df_test_under, "trianglesTest", "coefficientTest", driver)
        df_test_under = apply_community_features(df_test_under, "partitionTest", "louvainTest", driver)
        return df_test_under
