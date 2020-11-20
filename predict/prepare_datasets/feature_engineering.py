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
    artist_df = apply_graphy_features_all(artist_df, "FEAT", driver)  # We use the standard "FEAT" relationship type
    # Filter out pairs with < min_nb_tn total neighbors (because the next computations are intensive)
    artist_df = artist_df.loc[artist_df['tn'] >= min_nb_tn]
    artist_df = apply_triangles_features(artist_df, "triangles", "coefficient", driver)
    artist_df = apply_community_features(artist_df, "partition", "louvain", driver)

    logger.info("Artist %s's Pandas DataFrame computed.")
    return artist_df


def apply_graphy_features_all(data, rel_type, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
           pair.node2 AS node2,
           gds.alpha.linkprediction.commonNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS cn_all,
           gds.alpha.linkprediction.preferentialAttachment(p1, p2, {
             relationshipQuery: $relType}) AS pa_all,
           gds.alpha.linkprediction.totalNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS tn_all
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]

    with driver_instance.session() as session:
        result = session.run(query, {"pairs": pairs, "relType": rel_type})
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated graphy features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_graphy_features_2015(data, rel_type, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
           pair.node2 AS node2,
           gds.alpha.linkprediction.commonNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS cn_2015,
           gds.alpha.linkprediction.preferentialAttachment(p1, p2, {
             relationshipQuery: $relType}) AS pa_2015,
           gds.alpha.linkprediction.totalNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS tn_2015
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]

    with driver_instance.session() as session:
        result = session.run(query, {"pairs": pairs, "relType": rel_type})
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated graphy features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_graphy_features_2016(data, rel_type, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
           pair.node2 AS node2,
           gds.alpha.linkprediction.commonNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS cn_2016,
           gds.alpha.linkprediction.preferentialAttachment(p1, p2, {
             relationshipQuery: $relType}) AS pa_2016,
           gds.alpha.linkprediction.totalNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS tn_2016
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]

    with driver_instance.session() as session:
        result = session.run(query, {"pairs": pairs, "relType": rel_type})
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated graphy features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_graphy_features_2017(data, rel_type, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
           pair.node2 AS node2,
           gds.alpha.linkprediction.commonNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS cn_2017,
           gds.alpha.linkprediction.preferentialAttachment(p1, p2, {
             relationshipQuery: $relType}) AS pa_2017,
           gds.alpha.linkprediction.totalNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS tn_2017
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]

    with driver_instance.session() as session:
        result = session.run(query, {"pairs": pairs, "relType": rel_type})
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated graphy features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_graphy_features_2018(data, rel_type, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
           pair.node2 AS node2,
           gds.alpha.linkprediction.commonNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS cn_2018,
           gds.alpha.linkprediction.preferentialAttachment(p1, p2, {
             relationshipQuery: $relType}) AS pa_2018,
           gds.alpha.linkprediction.totalNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS tn_2018
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]

    with driver_instance.session() as session:
        result = session.run(query, {"pairs": pairs, "relType": rel_type})
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated graphy features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_graphy_features_2019(data, rel_type, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
           pair.node2 AS node2,
           gds.alpha.linkprediction.commonNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS cn_2019,
           gds.alpha.linkprediction.preferentialAttachment(p1, p2, {
             relationshipQuery: $relType}) AS pa_2019,
           gds.alpha.linkprediction.totalNeighbors(p1, p2, {
             relationshipQuery: $relType}) AS tn_2019
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]

    with driver_instance.session() as session:
        result = session.run(query, {"pairs": pairs, "relType": rel_type})
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated graphy features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_triangles_features_all(data, triangles_prop, coefficient_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN 
    pair.node1 AS node1,
    pair.node2 AS node2,
    apoc.coll.min([p1[$trianglesProp], p2[$trianglesProp]]) AS minTriangles_all,
    apoc.coll.max([p1[$trianglesProp], p2[$trianglesProp]]) AS maxTriangles_all,
    apoc.coll.avg([p1[$trianglesProp], p2[$trianglesProp]]) AS avgTriangles_all,
    apoc.coll.min([p1[$coefficientProp], p2[$coefficientProp]]) AS minCoefficient_all,
    apoc.coll.max([p1[$coefficientProp], p2[$coefficientProp]]) AS maxCoefficient_all
    apoc.coll.avg([p1[$coefficientProp], p2[$coefficientProp]]) AS avgCoefficient_all
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "trianglesProp": triangles_prop,
        "coefficientProp": coefficient_prop,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    result = pd.merge(data, features, on=["node1", "node2"])
    logger.info('Calculated triangles features.')
    return result


def apply_triangles_features_2015(data, triangles_prop, coefficient_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN 
    pair.node1 AS node1,
    pair.node2 AS node2,
    apoc.coll.min([p1[$trianglesProp], p2[$trianglesProp]]) AS minTriangles_2015,
    apoc.coll.max([p1[$trianglesProp], p2[$trianglesProp]]) AS maxTriangles_2015,
    apoc.coll.avg([p1[$trianglesProp], p2[$trianglesProp]]) AS avgTriangles_2015,
    apoc.coll.min([p1[$coefficientProp], p2[$coefficientProp]]) AS minCoefficient_2015,
    apoc.coll.max([p1[$coefficientProp], p2[$coefficientProp]]) AS maxCoefficient_2015
    apoc.coll.avg([p1[$coefficientProp], p2[$coefficientProp]]) AS avgCoefficient_2015
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "trianglesProp": triangles_prop,
        "coefficientProp": coefficient_prop,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    result = pd.merge(data, features, on=["node1", "node2"])
    logger.info('Calculated triangles features.')
    return result


def apply_triangles_features_2016(data, triangles_prop, coefficient_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN 
    pair.node1 AS node1,
    pair.node2 AS node2,
    apoc.coll.min([p1[$trianglesProp], p2[$trianglesProp]]) AS minTriangles_2016,
    apoc.coll.max([p1[$trianglesProp], p2[$trianglesProp]]) AS maxTriangles_2016,
    apoc.coll.avg([p1[$trianglesProp], p2[$trianglesProp]]) AS avgTriangles_2016,
    apoc.coll.min([p1[$coefficientProp], p2[$coefficientProp]]) AS minCoefficient_2016,
    apoc.coll.max([p1[$coefficientProp], p2[$coefficientProp]]) AS maxCoefficient_2016
    apoc.coll.avg([p1[$coefficientProp], p2[$coefficientProp]]) AS avgCoefficient_2016
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "trianglesProp": triangles_prop,
        "coefficientProp": coefficient_prop,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    result = pd.merge(data, features, on=["node1", "node2"])
    logger.info('Calculated triangles features.')
    return result


def apply_triangles_features_2017(data, triangles_prop, coefficient_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN 
    pair.node1 AS node1,
    pair.node2 AS node2,
    apoc.coll.min([p1[$trianglesProp], p2[$trianglesProp]]) AS minTriangles_2017,
    apoc.coll.max([p1[$trianglesProp], p2[$trianglesProp]]) AS maxTriangles_2017,
    apoc.coll.avg([p1[$trianglesProp], p2[$trianglesProp]]) AS avgTriangles_2017,
    apoc.coll.min([p1[$coefficientProp], p2[$coefficientProp]]) AS minCoefficient_2017,
    apoc.coll.max([p1[$coefficientProp], p2[$coefficientProp]]) AS maxCoefficient_2017
    apoc.coll.avg([p1[$coefficientProp], p2[$coefficientProp]]) AS avgCoefficient_2017
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "trianglesProp": triangles_prop,
        "coefficientProp": coefficient_prop,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    result = pd.merge(data, features, on=["node1", "node2"])
    logger.info('Calculated triangles features.')
    return result


def apply_triangles_features_2018(data, triangles_prop, coefficient_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN 
    pair.node1 AS node1,
    pair.node2 AS node2,
    apoc.coll.min([p1[$trianglesProp], p2[$trianglesProp]]) AS minTriangles_2018,
    apoc.coll.max([p1[$trianglesProp], p2[$trianglesProp]]) AS maxTriangles_2018,
    apoc.coll.avg([p1[$trianglesProp], p2[$trianglesProp]]) AS avgTriangles_2018,
    apoc.coll.min([p1[$coefficientProp], p2[$coefficientProp]]) AS minCoefficient_2018,
    apoc.coll.max([p1[$coefficientProp], p2[$coefficientProp]]) AS maxCoefficient_2018
    apoc.coll.avg([p1[$coefficientProp], p2[$coefficientProp]]) AS avgCoefficient_2018
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "trianglesProp": triangles_prop,
        "coefficientProp": coefficient_prop,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    result = pd.merge(data, features, on=["node1", "node2"])
    logger.info('Calculated triangles features.')
    return result


def apply_triangles_features_2019(data, triangles_prop, coefficient_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN 
    pair.node1 AS node1,
    pair.node2 AS node2,
    apoc.coll.min([p1[$trianglesProp], p2[$trianglesProp]]) AS minTriangles_2019,
    apoc.coll.max([p1[$trianglesProp], p2[$trianglesProp]]) AS maxTriangles_2019,
    apoc.coll.avg([p1[$trianglesProp], p2[$trianglesProp]]) AS avgTriangles_2019,
    apoc.coll.min([p1[$coefficientProp], p2[$coefficientProp]]) AS minCoefficient_2019,
    apoc.coll.max([p1[$coefficientProp], p2[$coefficientProp]]) AS maxCoefficient_2019
    apoc.coll.avg([p1[$coefficientProp], p2[$coefficientProp]]) AS avgCoefficient_2019
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "trianglesProp": triangles_prop,
        "coefficientProp": coefficient_prop,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    result = pd.merge(data, features, on=["node1", "node2"])
    logger.info('Calculated triangles features.')
    return result


def apply_community_features_all(data, partition_prop, louvain_prop, louvain_first_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
    pair.node2 AS node2,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $partitionProp) AS sp_all,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvainProp) AS sl_all
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvain_first_prop) AS sl_first_all
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "partitionProp": partition_prop,
        "louvainProp": louvain_prop,
        "louvain_first_prop": louvain_first_prop
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated community features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_community_features_2015(data, partition_prop, louvain_prop, louvain_first_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
    pair.node2 AS node2,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $partitionProp) AS sp_2015,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvainProp) AS sl_2015
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvain_first_prop) AS sl_first_2015
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "partitionProp": partition_prop,
        "louvainProp": louvain_prop,
        "louvain_first_prop": louvain_first_prop
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated community features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_community_features_2016(data, partition_prop, louvain_prop, louvain_first_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
    pair.node2 AS node2,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $partitionProp) AS sp_2016,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvainProp) AS sl_2016
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvain_first_prop) AS sl_first_2016
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "partitionProp": partition_prop,
        "louvainProp": louvain_prop,
        "louvain_first_prop": louvain_first_prop
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated community features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_community_features_2017(data, partition_prop, louvain_prop, louvain_first_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
    pair.node2 AS node2,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $partitionProp) AS sp_2017,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvainProp) AS sl_2017
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvain_first_prop) AS sl_first_2017
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "partitionProp": partition_prop,
        "louvainProp": louvain_prop,
        "louvain_first_prop": louvain_first_prop,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated community features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_community_features_2018(data, partition_prop, louvain_prop, louvain_first_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
    pair.node2 AS node2,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $partitionProp) AS sp_2018,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvainProp) AS sl_2018
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvain_first_prop) AS sl_first_2018
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "partitionProp": partition_prop,
        "louvainProp": louvain_prop,
        "louvain_first_prop": louvain_first_prop,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated community features.')
    return pd.merge(data, features, on=["node1", "node2"])


def apply_community_features_2019(data, partition_prop, louvain_prop, louvain_first_prop, driver_instance=driver):
    query = """
    UNWIND $pairs AS pair
    MATCH (p1) WHERE id(p1) = pair.node1
    MATCH (p2) WHERE id(p2) = pair.node2
    RETURN pair.node1 AS node1,
    pair.node2 AS node2,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $partitionProp) AS sp_2019,
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvainProp) AS sl_2019
    gds.alpha.linkprediction.sameCommunity(p1, p2, $louvain_first_prop) AS sl_first_2019
    """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "partitionProp": partition_prop,
        "louvainProp": louvain_prop,
        "louvain_first_prop": louvain_first_prop,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    logger.info('Calculated community features.')
    return pd.merge(data, features, on=["node1", "node2"])


def extract_nb_common_label_all(data, driver_instance):
    # TODO: this function does not seem to work when working on the same id for p1 and p2
    query = """
        UNWIND $pairs AS pair
        MATCH (p1) WHERE id(p1) = pair.node1
        MATCH (p2) WHERE id(p2) = pair.node2
        MATCH (p1)-[:LABEL]-(l)-[:LABEL]-(p2)
        RETURN 
        pair.node1 AS node1, 
        pair.node2 AS node2,
        count(distinct l) AS nb_common_labels_all
        """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs
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
        same_label['nb_common_labels_all'] = 0.0

    logger.info('Calculated same label feature.')
    return pd.merge(data, same_label, how="left", on=["node1", "node2"])


def extract_nb_common_label_2015(data, driver_instance, year=2015):
    # TODO: this function does not seem to work when working on the same id for p1 and p2
    query = """
        UNWIND $pairs AS pair
        MATCH (p1) WHERE id(p1) = pair.node1
        MATCH (p2) WHERE id(p2) = pair.node2
        MATCH (p1)-[r1:LABEL]-(l)-[r2:LABEL]-(p2)
        WHERE toInteger(date(r1.date).year) = $year AND toInteger(date(r2.date).year) = $year
        RETURN 
        pair.node1 AS node1, 
        pair.node2 AS node2,
        count(distinct l) AS nb_common_labels_2015
        """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year
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
        same_label['nb_common_labels_2015'] = 0.0

    logger.info('Calculated same label feature.')
    return pd.merge(data, same_label, on=["node1", "node2"])


def extract_nb_common_label_2016(data, driver_instance, year=2016):
    # TODO: this function does not seem to work when working on the same id for p1 and p2
    query = """
        UNWIND $pairs AS pair
        MATCH (p1) WHERE id(p1) = pair.node1
        MATCH (p2) WHERE id(p2) = pair.node2
        MATCH (p1)-[r1:LABEL]-(l)-[r2:LABEL]-(p2)
        WHERE toInteger(date(r1.date).year) = $year AND toInteger(date(r2.date).year) = $year
        RETURN 
        pair.node1 AS node1, 
        pair.node2 AS node2,
        count(distinct l) AS nb_common_labels_2016
        """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year
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
        same_label['nb_common_labels_2016'] = 0.0

    logger.info('Calculated same label feature.')
    return pd.merge(data, same_label, on=["node1", "node2"])


def extract_nb_common_label_2017(data, driver_instance, year=2017):
    # TODO: this function does not seem to work when working on the same id for p1 and p2
    query = """
        UNWIND $pairs AS pair
        MATCH (p1) WHERE id(p1) = pair.node1
        MATCH (p2) WHERE id(p2) = pair.node2
        MATCH (p1)-[r1:LABEL]-(l)-[r2:LABEL]-(p2)
        WHERE toInteger(date(r1.date).year) = $year AND toInteger(date(r2.date).year) = $year
        RETURN 
        pair.node1 AS node1, 
        pair.node2 AS node2,
        count(distinct l) AS nb_common_labels_2017
        """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year
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
        same_label['nb_common_labels_2017'] = 0.0

    logger.info('Calculated same label feature.')
    return pd.merge(data, same_label, on=["node1", "node2"])


def extract_nb_common_label_2018(data, driver_instance, year=2018):
    # TODO: this function does not seem to work when working on the same id for p1 and p2
    query = """
        UNWIND $pairs AS pair
        MATCH (p1) WHERE id(p1) = pair.node1
        MATCH (p2) WHERE id(p2) = pair.node2
        MATCH (p1)-[r1:LABEL]-(l)-[r2:LABEL]-(p2)
        WHERE toInteger(date(r1.date).year) = $year AND toInteger(date(r2.date).year) = $year
        RETURN 
        pair.node1 AS node1, 
        pair.node2 AS node2,
        count(distinct l) AS nb_common_labels_2018
        """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year
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
        same_label['nb_common_labels_2018'] = 0.0

    logger.info('Calculated same label feature.')
    return pd.merge(data, same_label, on=["node1", "node2"])


def extract_nb_common_label_2019(data, driver_instance, year=2019):
    # TODO: this function does not seem to work when working on the same id for p1 and p2
    query = """
        UNWIND $pairs AS pair
        MATCH (p1) WHERE id(p1) = pair.node1
        MATCH (p2) WHERE id(p2) = pair.node2
        MATCH (p1)-[r1:LABEL]-(l)-[r2:LABEL]-(p2)
        WHERE toInteger(date(r1.date).year) = $year AND toInteger(date(r2.date).year) = $year
        RETURN 
        pair.node1 AS node1, 
        pair.node2 AS node2,
        count(distinct l) AS nb_common_labels_2019
        """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year
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
        same_label['nb_common_labels_2019'] = 0.0

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


def extract_nb_feats_all(data, driver_instance):
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 
            MATCH (p2) WHERE id(p2) = pair.node2
            MATCH (p1)-[r:FEAT]-(p2)
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            count (distinct r) AS nb_feats_all
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    if not features.empty:
        nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])
        nb_yearly_feats = nb_yearly_feats.fillna(0.0)
    else:
        nb_yearly_feats = data[["node1", "node2"]]
        nb_yearly_feats['nb_feats_all'] = 0.0

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def extract_nb_feats_2015(data, driver_instance, year=2015):
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 
            MATCH (p2) WHERE id(p2) = pair.node2
            MATCH (p1)-[r:FEAT]-(p2)
            WHERE toInteger(date(r.track_date).year) = $year
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            count (distinct r) AS nb_feats_2015
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    if not features.empty:
        nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])
        nb_yearly_feats = nb_yearly_feats.fillna(0.0)
    else:
        nb_yearly_feats = data[["node1", "node2"]]
        nb_yearly_feats['nb_feats_2015'] = 0.0

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def extract_nb_feats_2016(data, driver_instance, year=2016):
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 
            MATCH (p2) WHERE id(p2) = pair.node2
            MATCH (p1)-[r:FEAT]-(p2)
            WHERE toInteger(date(r.track_date).year) = $year
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            count (distinct r) AS nb_feats_2016
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    if not features.empty:
        nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])
        nb_yearly_feats = nb_yearly_feats.fillna(0.0)
    else:
        nb_yearly_feats = data[["node1", "node2"]]
        nb_yearly_feats['nb_feats_2016'] = 0.0

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def extract_nb_feats_2017(data, driver_instance, year=2017):
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 
            MATCH (p2) WHERE id(p2) = pair.node2
            MATCH (p1)-[r:FEAT]-(p2)
            WHERE toInteger(date(r.track_date).year) = $year
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            count (distinct r) AS nb_feats_2017
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    if not features.empty:
        nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])
        nb_yearly_feats = nb_yearly_feats.fillna(0.0)
    else:
        nb_yearly_feats = data[["node1", "node2"]]
        nb_yearly_feats['nb_feats_2017'] = 0.0

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def extract_nb_feats_2018(data, driver_instance, year=2018):
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 
            MATCH (p2) WHERE id(p2) = pair.node2
            MATCH (p1)-[r:FEAT]-(p2)
            WHERE toInteger(date(r.track_date).year) = $year
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            count (distinct r) AS nb_feats_2018
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    if not features.empty:
        nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])
        nb_yearly_feats = nb_yearly_feats.fillna(0.0)
    else:
        nb_yearly_feats = data[["node1", "node2"]]
        nb_yearly_feats['nb_feats_2018'] = 0.0

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def extract_nb_feats_2019(data, driver_instance, year=2019):
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 
            MATCH (p2) WHERE id(p2) = pair.node2
            MATCH (p1)-[r:FEAT]-(p2)
            WHERE toInteger(date(r.track_date).year) = $year
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            count (distinct r) AS nb_feats_2019
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "year": year,
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    if not features.empty:
        nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])
        nb_yearly_feats = nb_yearly_feats.fillna(0.0)
    else:
        nb_yearly_feats = data[["node1", "node2"]]
        nb_yearly_feats['nb_feats_2019'] = 0.0

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def nodes_degrees_year_all(data, driver_instance, year='all'):
    # TODO : à tester
    feature_name_p1 = 'degree_p1_' + str(year)
    feature_name_p2 = 'degree_p2_' + str(year)
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 WITH size((p1)-[:FEAT]->()) as degree1, pair
            MATCH (p2) WHERE id(p2) = pair.node2 WITH size((p2)-[:FEAT]->()) as degree2, degree1, pair
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            degree1 as degree_p1_all,
            degree2 as degree_p2_all
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "feature_name_p1": feature_name_p1,
        "feature_name_p2": feature_name_p2
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def nodes_degrees_year_2015(data, driver_instance, year='2015'):
    # TODO : à tester
    feature_name_p1 = 'degree_p1_' + str(year)
    feature_name_p2 = 'degree_p2_' + str(year)
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 WITH size((p1)-[:FEAT_2015]->()) as degree1, pair
            MATCH (p2) WHERE id(p2) = pair.node2 WITH size((p2)-[:FEAT_2015]->()) as degree2, degree1, pair
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            degree1 as degree_p1_2015,
            degree2 as degree_p2_2015
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "feature_name_p1": feature_name_p1,
        "feature_name_p2": feature_name_p2
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def nodes_degrees_year_2016(data, driver_instance, year='2016'):
    # TODO : à tester
    feature_name_p1 = 'degree_p1_' + str(year)
    feature_name_p2 = 'degree_p2_' + str(year)
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 WITH size((p1)-[:FEAT_2016]->()) as degree1, pair
            MATCH (p2) WHERE id(p2) = pair.node2 WITH size((p2)-[:FEAT_2016]->()) as degree2, degree1, pair
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            degree1 as degree_p1_2016,
            degree2 as degree_p2_2016
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "feature_name_p1": feature_name_p1,
        "feature_name_p2": feature_name_p2
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def nodes_degrees_year_2017(data, driver_instance, year='2017'):
    # TODO : à tester
    feature_name_p1 = 'degree_p1_' + str(year)
    feature_name_p2 = 'degree_p2_' + str(year)
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 WITH size((p1)-[:FEAT_2017]->()) as degree1, pair
            MATCH (p2) WHERE id(p2) = pair.node2 WITH size((p2)-[:FEAT_2017]->()) as degree2, degree1, pair
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            degree1 as degree_p1_2017,
            degree2 as degree_p2_2017
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "feature_name_p1": feature_name_p1,
        "feature_name_p2": feature_name_p2
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def nodes_degrees_year_2018(data, driver_instance, year='2018'):
    # TODO : à tester
    feature_name_p1 = 'degree_p1_' + str(year)
    feature_name_p2 = 'degree_p2_' + str(year)
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 WITH size((p1)-[:FEAT_2018]->()) as degree1, pair
            MATCH (p2) WHERE id(p2) = pair.node2 WITH size((p2)-[:FEAT_2018]->()) as degree2, degree1, pair
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            degree1 as degree_p1_2018,
            degree2 as degree_p2_2018
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "feature_name_p1": feature_name_p1,
        "feature_name_p2": feature_name_p2
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def nodes_degrees_year_2019(data, driver_instance, year='2019'):
    # TODO : à tester
    feature_name_p1 = 'degree_p1_' + str(year)
    feature_name_p2 = 'degree_p2_' + str(year)
    query = """
            UNWIND $pairs AS pair
            MATCH (p1) WHERE id(p1) = pair.node1 WITH size((p1)-[:FEAT_2019]->()) as degree1, pair
            MATCH (p2) WHERE id(p2) = pair.node2 WITH size((p2)-[:FEAT_2019]->()) as degree2, degree1, pair
            RETURN DISTINCT
            pair.node1 AS node1, 
            pair.node2 AS node2,
            degree1 as degree_p1_2019,
            degree2 as degree_p2_2019
            """
    pairs = [{"node1": node1, "node2": node2} for node1, node2 in data[["node1", "node2"]].values.tolist()]
    params = {
        "pairs": pairs,
        "feature_name_p1": feature_name_p1,
        "feature_name_p2": feature_name_p2
    }

    with driver_instance.session() as session:
        result = session.run(query, params)
        features = pd.DataFrame([dict(record) for record in result])

    nb_yearly_feats = pd.merge(data[["node1", "node2"]], features, how="left", on=["node1", "node2"])

    return pd.merge(data, nb_yearly_feats, on=["node1", "node2"])


def add_nb_tracks(spotify_loader, df_test_under, graph, year):
    min_date = str(year) + '-' + '01-01'
    max_date = str(year) + '-' + '12-31'
    node1_ids = df_test_under['node1'].values.tolist()
    node2_ids = df_test_under['node2'].values.tolist()
    for nodes in zip(node1_ids, node2_ids):
        nb_tracks_node1 = spotify_loader.get_nb_tracks_year(min_date, max_date, graph.get_urn_by_id(nodes[0]))
        nb_tracks_node2 = spotify_loader.get_nb_tracks_year(min_date, max_date, graph.get_urn_by_id(nodes[1]))
    df_test_under['nb_tracks_'+str(year)+'_p1'] = nb_tracks_node1
    df_test_under['nb_tracks_'+str(year)+'_p2'] = nb_tracks_node2
    return df_test_under


def engineer_features(driver, dataset):

    # TODO: add feature feat_percentage = nb tracks with ft / nb total tracks from the artist?
    # TODO: add feature with Node2Vec embeddings (addition of each node's embedding)

    df_test_under = dataset

    df_test_under = extract_nb_common_label_all(data=df_test_under, driver_instance=driver)
    df_test_under = extract_nb_common_label_2015(data=df_test_under, driver_instance=driver)
    df_test_under = extract_nb_common_label_2016(data=df_test_under, driver_instance=driver)
    df_test_under = extract_nb_common_label_2017(data=df_test_under, driver_instance=driver)
    df_test_under = extract_nb_common_label_2018(data=df_test_under, driver_instance=driver)
    df_test_under = extract_nb_common_label_2019(data=df_test_under, driver_instance=driver)

    df_test_under = extract_same_genre_feature(df_test_under, driver)

    df_test_under = extract_nb_feats_all(df_test_under, driver)
    df_test_under = extract_nb_feats_2015(df_test_under, driver)
    df_test_under = extract_nb_feats_2016(df_test_under, driver)
    df_test_under = extract_nb_feats_2017(df_test_under, driver)
    df_test_under = extract_nb_feats_2018(df_test_under, driver)
    df_test_under = extract_nb_feats_2019(df_test_under, driver)

    df_test_under = nodes_degrees_year_all(df_test_under, driver)
    df_test_under = nodes_degrees_year_2015(df_test_under, driver)
    df_test_under = nodes_degrees_year_2016(df_test_under, driver)
    df_test_under = nodes_degrees_year_2017(df_test_under, driver)
    df_test_under = nodes_degrees_year_2018(df_test_under, driver)
    df_test_under = nodes_degrees_year_2019(df_test_under, driver)

    df_test_under = extract_popularity_diff_feature(df_test_under, driver)

    df_test_under = apply_graphy_features_all(df_test_under, "FEAT", driver)
    df_test_under = apply_graphy_features_2015(df_test_under, "FEAT_2015", driver)
    df_test_under = apply_graphy_features_2016(df_test_under, "FEAT_2016", driver)
    df_test_under = apply_graphy_features_2017(df_test_under, "FEAT_2017", driver)
    df_test_under = apply_graphy_features_2018(df_test_under, "FEAT_2018", driver)
    df_test_under = apply_graphy_features_2019(df_test_under, "FEAT_2019", driver)

    df_test_under = apply_triangles_features_all(df_test_under, "triangles_all", "coefficient_all", driver)
    df_test_under = apply_triangles_features_2015(df_test_under, "triangles_2015", "coefficient_2015", driver)
    df_test_under = apply_triangles_features_2016(df_test_under, "triangles_2016", "coefficient_2016", driver)
    df_test_under = apply_triangles_features_2017(df_test_under, "triangles_2017", "coefficient_2017", driver)
    df_test_under = apply_triangles_features_2018(df_test_under, "triangles_2018", "coefficient_2018", driver)
    df_test_under = apply_triangles_features_2019(df_test_under, "triangles_2019", "coefficient_2019", driver)

    df_test_under = apply_community_features_all(df_test_under, "partition_all", "louvain_all", driver)
    df_test_under = apply_community_features_2015(df_test_under, "partition_2015", "louvain_2015", driver)
    df_test_under = apply_community_features_2016(df_test_under, "partition_2016", "louvain_2016", driver)
    df_test_under = apply_community_features_2017(df_test_under, "partition_2017", "louvain_2017", driver)
    df_test_under = apply_community_features_2018(df_test_under, "partition_2018", "louvain_2018", driver)
    df_test_under = apply_community_features_2019(df_test_under, "partition_2019", "louvain_2019", driver)

    return df_test_under
