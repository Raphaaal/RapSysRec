from handlers.neo4j_handler import Neo4JHandler
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
from sklearn.metrics import accuracy_score
from sklearn.metrics import plot_roc_curve
import numpy as np
import logging
from joblib import dump, load
from xgboost import XGBClassifier
import scikitplot as skplt
import matplotlib.pyplot as plt

pd.set_option('display.float_format', lambda x: '%.3f' % x)

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('model_training_testing')
# logger.propagate = False


def feature_importance(columns, classifier):
    features = list(zip(columns, classifier.feature_importances_))
    sorted_features = sorted(features, key=lambda x: x[1] * -1)
    keys = [value[0] for value in sorted_features]
    values = [value[1] for value in sorted_features]
    logger.info("Feature importance computed.")
    return pd.DataFrame(data={'feature': keys, 'value': values})


# TODO: use a confidence interval?
def evaluate_model(predictions, actual):
    eval_pdf = pd.DataFrame({
        "Measure": ["Accuracy", "Precision", "Recall"],
        "Score": [accuracy_score(actual, predictions),
                  precision_score(actual, predictions),
                  recall_score(actual, predictions)]
    })
    logger.info("Model evalution computed.")
    return eval_pdf


def make_predictions(pdf, classifier, features):
    pdf[['pred_proba_0', 'pred_proba_1']] = classifier.predict_proba(pdf[features])
    pdf['pred'] = classifier.predict(pdf[features])
    logger.info("Predictions computed.")
    return pdf


def get_relevant_predictions(pdf, concerned_artist_id, graph):
    pairs = pdf[['node1', 'node2']].values.tolist()
    pdf['id'] = [x[0] if x[1] == concerned_artist_id else x[1] for x in pairs]
    pdf_display = pdf[["id", "pred", "label", "pred_proba_0", "pred_proba_1"]]
    ft_artists_names = graph.get_artists_pdf_from_ids(pdf['id'].tolist())
    pdf_display['name'] = np.asarray(ft_artists_names)
    # TMP - DEBUG
    result = pdf_display
    # result = pdf_display[pdf_display['label'] == 0].sort_values(["pred_proba_1"], ascending=[False])
    return result


def import_datasets():
    train_set = pd.read_csv('../prepare_datasets/train_set_features.csv')
    test_set = pd.read_csv('../prepare_datasets/test_set_features.csv')
    artist_specific = pd.read_csv('../prepare_datasets/artist_set_features.csv')
    full_set = pd.read_csv('../prepare_datasets/full_set_features.csv')
    validation_set = pd.read_csv('../prepare_datasets/validation_set_features.csv')
    return train_set, test_set, artist_specific, full_set, validation_set


def train_classifier(train_set, columns, path='model.joblib'):
    train_set.drop(columns=["node1", "node2"])
    # classifier = RandomForestClassifier(n_estimators=300, max_depth=10, random_state=0)
    classifier = XGBClassifier(n_estimators=300, max_depth=100, random_state=0)
    X = train_set[columns]
    y = train_set["label"]
    classifier.fit(X, y)
    logger.info('Classifier trained')
    dump(classifier, path)
    logger.info('Classifier saved')
    return classifier


def test_classifier(classifier, test_set, columns, path):
    test_set.drop(columns=["node1", "node2"])
    preds = classifier.predict(test_set[columns])
    y_test = test_set["label"]
    results = evaluate_model(preds, y_test)
    print(results)
    feature_expl = feature_importance(columns, classifier)
    feature_expl.to_csv(path)
    # plot_roc_curve(y_true=y_test.to_list(), y_probas=preds)
    plot_roc_curve(classifier, test_set[columns], test_set["label"])
    plt.show()


def get_artist_predictions(artist_df, classifier, columns, urn):
    artist_df = make_predictions(artist_df, classifier, columns)
    concerned_artist_id = graph.get_node_id_by_urn(urn=urn)
    print("Artist's ID: ", concerned_artist_id)
    result_artist = get_relevant_predictions(artist_df, concerned_artist_id=concerned_artist_id, graph=graph)
    logger.info('Artist prediction computed')
    return result_artist


# def plot_roc_curve(y_true, y_probas):
#     skplt.metrics.plot_roc_curve(y_true, y_probas)
#     plt.show()


if __name__ == '__main__':

    # Graph DB connection
    graph = Neo4JHandler(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="root"
    )
    driver = graph.driver

    # Train / test / artist / full sets import
    train_set, test_set, artist_specific, full_set, validation_set = import_datasets()
    logger.info("Imported datasets.")

    columns = [
        # graph features
        "cn_all", "pa_all", "tn_all",
        "cn_2015", "pa_2015", "tn_2015",
        "cn_2016", "pa_2016", "tn_2016",
        "cn_2017", "pa_2017", "tn_2017",
        "cn_2018", "pa_2018", "tn_2018",
        "cn_2019", "pa_2019", "tn_2019",

        # triangle features
        "minTriangles_all", "maxTriangles_all", "minCoefficient_all", "maxCoefficient_all",
        "minTriangles_2015", "maxTriangles_2015", "minCoefficient_2015", "maxCoefficient_2015",
        "minTriangles_2016", "maxTriangles_2016", "minCoefficient_2016", "maxCoefficient_2016",
        "minTriangles_2017", "maxTriangles_2017", "minCoefficient_2017", "maxCoefficient_2017",
        "minTriangles_2018", "maxTriangles_2018", "minCoefficient_2018", "maxCoefficient_2018",
        "minTriangles_2019", "maxTriangles_2019", "minCoefficient_2019", "maxCoefficient_2019",

        # community features
        "sp_all", "sl_all",
        "sp_2015", "sl_2015",
        "sp_2016", "sl_2016",
        "sp_2017", "sl_2017",
        "sp_2018", "sl_2018",
        "sp_2019", "sl_2019",

        # Nb of common labels
        "nb_common_labels_all",
        "nb_common_labels_2015",
        "nb_common_labels_2016",
        "nb_common_labels_2017",
        "nb_common_labels_2018",
        "nb_common_labels_2019",

        # Nb of feats
        "nb_feats_all",
        "nb_feats_2015",
        "nb_feats_2016",
        "nb_feats_2017",
        "nb_feats_2018",
        "nb_feats_2019",

        # Degree of each node in the pair
        "degree_p1_all",
        "degree_p1_2015",
        "degree_p1_2016",
        "degree_p1_2017",
        "degree_p1_2018",
        "degree_p1_2019",
        "degree_p2_all",
        "degree_p2_2015",
        "degree_p2_2016",
        "degree_p2_2017",
        "degree_p2_2018",
        "degree_p2_2019",

        # Nb tracks per year
        'nb_tracks_2015_p1', 'nb_tracks_2015_p2'
        'nb_tracks_2016_p1', 'nb_tracks_2016_p2'
        'nb_tracks_2017_p1', 'nb_tracks_2017_p2'
        'nb_tracks_2018_p1', 'nb_tracks_2018_p2'
        'nb_tracks_2019_p1', 'nb_tracks_2019_p2'

        "nb_common_genres",
        "squared_popularity_diff"
    ]
    model_path = 'model.joblib'

    # Train classifier
    classifier = train_classifier(train_set, columns=columns, path=model_path)
    logger.info("Trained classifier.")

    # Model analysis
    classifier = load(model_path)
    logger.info("Loaded classifier.")
    test_classifier(classifier, validation_set, columns, 'model_features_importance.csv')
    # test_classifier(classifier, test_set, columns, 'model_features_importance.csv')
    logger.info("Tested classifier.")

    # Model re-training with full set
    # classifier = train_classifier(full_set, columns=columns)

    # Artist specific predictions
    result_artist_specific = get_artist_predictions(artist_specific, classifier, columns, "6qFt3TjvxMt77YGsktWG8Z") # Sofiane
    result_artist_specific.to_csv("artist_predictions.csv")
