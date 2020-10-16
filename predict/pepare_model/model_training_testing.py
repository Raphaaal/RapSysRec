from neo4j_handler import Neo4JHandler
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
from sklearn.metrics import accuracy_score
import numpy as np
import logging
from joblib import dump, load

pd.set_option('display.float_format', lambda x: '%.3f' % x)

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('model_training_testing')
# logger.propagate = False


# TODO: print explicability (most important features)
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

    def filter_pdf(row, concerned_artist=concerned_artist_id):
        if row['node1'] == concerned_artist:
            return int(row['node2'])
        else:
            return int(row['node1'])

    pdf['id'] = pdf.apply(lambda row: filter_pdf(row, concerned_artist_id), axis=1)
    pdf_display = pdf[["id", "pred", "label", "pred_proba_0", "pred_proba_1"]]
    ft_artists_names = graph.get_artists_pdf_from_ids(pdf['id'].tolist())
    pdf_display['name'] = np.asarray(ft_artists_names)
    result = pdf_display[pdf_display['label'] == 0].sort_values(["pred_proba_1"], ascending=[False])
    return result


if __name__ == '__main__':

    # Graph DB connection
    graph = Neo4JHandler(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="root"
    )
    driver = graph.driver

    # Train / test / artist sets import
    train_set = pd.read_csv('../train_set_features.csv')
    test_set = pd.read_csv('../test_set_features.csv')
    hamza = pd.read_csv('../artist_set_features.csv')

    # Train classifier
    # TODO: try a different classifier / hyper parameters
    train_set.drop(columns=["node1", "node2"])
    test_set.drop(columns=["node1", "node2"])
    classifier = RandomForestClassifier(n_estimators=30, max_depth=10, random_state=0)
    columns = [
        "cn", "pa", "tn",  # graph features
        "minTriangles", "maxTriangles", "minCoefficient", "maxCoefficient",  # triangle features
        "sp", "sl",  # community features
        "nb_common_labels", "nb_common_genres", "squared_popularity_diff"
    ]
    X = train_set[columns]
    y = train_set["label"]
    classifier.fit(X, y)
    logger.info('Classifier trained')
    dump(classifier, '../model.joblib')
    logger.info('Classifier saved')

    # Model analysis
    # TODO: at the end, re-train the validated model on all data (train + test)
    preds = classifier.predict(test_set[columns])
    y_test = test_set["label"]
    results = evaluate_model(preds, y_test)
    print(results)
    feature_expl = feature_importance(columns, classifier)
    print(feature_expl)

    # Artist specific predictions
    hamza = make_predictions(hamza, classifier, [
        "cn", "pa", "tn",
        "minTriangles", "maxTriangles", "minCoefficient", "maxCoefficient",
        "sp", "sl",
        "nb_common_labels", "nb_common_genres", "squared_popularity_diff"
    ])
    result_hamza = get_relevant_predictions(hamza, concerned_artist_id=7034, graph=graph)
    logger.info('Artist prediction computed')
    result_hamza.to_csv("artist_predictions.csv")
