from handlers.neo4j_handler import Neo4JHandler
import pandas as pd
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('feature_engineering')
logger.propagate = False

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
    artist_df = extract_nb_common_label_year(artist_df, driver)
    artist_df = extract_same_genre_feature(artist_df, driver)
    artist_df = extract_popularity_diff_feature(artist_df, driver)
    artist_df = apply_graphy_features(artist_df, "FEAT", driver)  # We use the standard "FEAT" relationship type
    # Filter out pairs with < min_nb_tn total neighbors (because the next computations are intensive)
    artist_df = artist_df.loc[artist_df['tn'] >= min_nb_tn]
    artist_df = apply_triangles_features(artist_df, "triangles", "coefficient", driver)
    artist_df = apply_community_features(artist_df, "partition", "louvain", driver)

    logger.info("Artist %s's Pandas DataFrame computed.")
    return artist_df


def apply_graphy_features(data, rel_type, year, driver_instance=driver):
    cn_name = 'cn_' + str(year)
    pa_name = 'pa_' + str(year)
    tn_name = 'tn_' + str(year)
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
           pair.node2 AS node2,
           gds.alpha.linkprediction.commonNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS $cn_name,
           gds.alpha.linkprediction.preferentialAttachment(p1, p2, {
             relationshipQuery: $relType}) AS $pa_name,
           gds.alpha.linkprediction.totalNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS $tn_name
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]

    with driver_instance.session() as session:
        result = session.run(query, {"pairs": pairs, "relType": rel_type, "cn_name": cn_name, "pa_name": pa_name, "tn_name": tn_name})
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated graphy features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_triangles_features(data, year, triangles_prop, coefficient_prop, driver_instance=driver):
    minTriangles = 'minTriangles_' + str(year)
    maxTriangles = 'maxTriangles_' + str(year)
    minCoefficient = 'minCoefficient_' + str(year)
    maxCoefficient = 'maxCoefficient_' + str(year)
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN 
    pair.node1 AS node1,
    pair.node2 AS node2,
    apoc.coll.min([p1[$trianglesProp], p2[$trianglesProp]]) AS $minTriangles,
    apoc.coll.max([p1[$trianglesProp], p2[$trianglesProp]]) AS $maxTriangles,
    apoc.coll.min([p1[$coefficientProp], p2[$coefficientProp]]) AS $minCoefficient,
    apoc.coll.max([p1[$coefficientProp], p2[$coefficientProp]]) AS $maxCoefficient
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "trianglesProp": triangles_prop,
        "coefficientProp": coefficient_prop,
        "minTriangles": minTriangles,
        "maxTriangles": maxTriangles,
        "minCoefficient": minCoefficient,
        "maxCoefficient": maxCoefficient,

    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    result = pd.merge(data, features, on=["node1", "node2"])
    logger.info('Calculated triangles features.')
    return result


def apply_community_features(data, year, partition_prop, louvain_prop, driver_instance=driver):
    sp = 'sp_' + str(year)
    sl = 'sl_' + str(year)
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
    pair.node2 AS node2,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $partitionProp) AS $sp,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvainProp) AS $sl
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "partitionProp": partition_prop,
        "louvainProp": louvain_prop,
        "sp": sp,
        "sl": sl,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated community features.')
    return pd.merge(data, features, on=["node1", "node2"])


def extract_nb_common_label_year(data, driver_instance, year):
    feature_name = 'nb_common_labels_' + str(year)
    # TODO: this function does not seem to work when working on the same id for p1 and p2
    query = """
        UNWIND $pairs AS pair
        MATCH (p1) WHERE id(p1) = pair.node1
        MATCH (p2) WHERE id(p2) = pair.node2
        MATCH (p1)-[r1:LABEL]-(l)-[r2:LABEL]-(p2)
        WHERE r.toInteger(date(r.date).year) == $year
        RETURN 
        pair.node1 AS node1, 
        pair.node2 AS node2,
        count(distinct l) AS $feature_name
        """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "feature_name": feature_name
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


def extract_nb_feats_per_year(data, driver_instance, year):
    feature_name = 'nb_feats_' + str(year)
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 
            MATCH (p2) WHERE id(p2) = pair.node2
            MATCH (p1)-[r:FEAT]-(p2)
            WHERE r.toInteger(date(r.track_date).year) == $year
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            count (distinct r) AS $feature_name
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year,
        "feature_name": feature_name
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])

    return nb_yearly_feats


def extract_both_active_year(data, driver_instance, year):
    # TODO : à tester
    feature_name = 'both_active_' + str(year)
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 
            MATCH (p2) WHERE id(p2) = pair.node2
            MATCH (p1)-[r1:FEAT]-()
            MATCH (p2)-[r2:FEAT]-()
            WHERE  
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            CASE 
                WHEN (r1.toInteger(date(r.track_date).year) == $year) AND (r2.toInteger(date(r.track_date).year) == $year) THEN 1
                ELSE 0
                END AS $feature_name
            LIMIT 3
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year,
        "feature_name": feature_name
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])

    return nb_yearly_feats


def engineer_features(driver, dataset):

    # TODO: Algo : Intégrer la récence des arcs et le label (de l'album et de l'artiste [label de son dernier album]) encodé avec un poids fort selon l'année
    # -> Besoin de dupliquer les arcs (car pas de prise en compte du weight dans les algos GDS) ou bien de faire du feature engineering a part (same_label: true / false) ?

    # TODO: add feature feat_percentage = nb tracks with ft / nb total tracks from the artist?
    # TODO: add number of tracks per year?

    df_test_under = dataset
    for i in range(2015, 2020):
        df_test_under = extract_nb_common_label_year(df_test_under, driver, year=i)
    df_test_under = extract_same_genre_feature(df_test_under, driver)
    for i in range(2015, 2020):
        df_test_under = extract_nb_feats_per_year(df_test_under, driver, year=i)
    for i in range(2015, 2020):
        df_test_under = extract_both_active_year(df_test_under, driver, year=i)
    df_test_under = extract_popularity_diff_feature(df_test_under, driver)

    df_test_under = apply_graphy_features(df_test_under, "FEAT", 'all', driver)
    df_test_under = apply_triangles_features(df_test_under, 'all', "triangles_all", "coefficient_all", driver)
    df_test_under = apply_community_features(df_test_under, 'all', "partition_all", "louvain_all", driver)

    years = ['2015', '2016', '2017', '2018', '2019']
    for year in years:
        df_test_under = apply_graphy_features(df_test_under, "FEAT_"+str(year), str(year), driver)
        df_test_under = apply_triangles_features(df_test_under, str(year), "triangles_"+str(year), "coefficient_"+str(year), driver)
        df_test_under = apply_community_features(df_test_under, str(year), "partition_"+str(year), "louvain_"+str(year), driver)

    return df_test_under
