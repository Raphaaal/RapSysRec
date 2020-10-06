from predict.feature_engineering import engineer_features
from neo4j_handler import Neo4JHandler
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
from sklearn.metrics import accuracy_score
import numpy as np


pd.set_option('display.float_format', lambda x: '%.3f' % x)


def artist_specific_pdf(train, test, artist_node_id):
    pdf_train = train.loc[(train['node1'] == artist_node_id) | (train['node2'] == artist_node_id)]
    pdf_test = test.loc[(test['node1'] == artist_node_id) | (train['node2'] == artist_node_id)]
    result = pdf_train.append(pdf_test, ignore_index=True)
    return result


# TODO: print explicability (most important features)
def feature_importance(columns, classifier):
    features = list(zip(columns, classifier.feature_importances_))
    sorted_features = sorted(features, key=lambda x: x[1] * -1)
    keys = [value[0] for value in sorted_features]
    values = [value[1] for value in sorted_features]
    return pd.DataFrame(data={'feature': keys, 'value': values})


# TODO: use a confidence interval?
def evaluate_model(predictions, actual):
    return pd.DataFrame({
        "Measure": ["Accuracy", "Precision", "Recall"],
        "Score": [accuracy_score(actual, predictions),
                  precision_score(actual, predictions),
                  recall_score(actual, predictions)]
    })


def make_predictions(pdf, classifier, features):
    pdf[['pred_proba_0', 'pred_proba_1']] = classifier.predict_proba(pdf[features])
    pdf['pred'] = classifier.predict(pdf[features])
    return pdf


def display_predictions(pdf, concerned_artist_id):
    graph = Neo4JHandler(
            uri="bolt://localhost:7687",
            user="neo4j",
            password="root"
        )

    def filter_pdf(row, concerned_artist_id):
        if row['node1'] == concerned_artist_id:
            return row['node2']
        else:
            return row['node1']

    pdf['id'] = pdf.apply(lambda row: filter_pdf(row), axis=1)
    pdf_display = pdf[["id", "pred", "label", "pred_proba_0", "pred_proba_1"]]
    ft_artists_names = graph.get_artists_pdf_from_ids(pdf['node2'].tolist())
    pdf_display['name'] = np.asarray(ft_artists_names)
    result = pdf_display[pdf_display['label'] == 0].sort_values(["pred_proba_1"], ascending=[False])
    print(result)


if __name__ == '__main__':

    # Graph DB connection
    graph = Neo4JHandler(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="root"
    )
    driver = graph.driver

    # Train / test split
    train, test = engineer_features(driver)

    # Artist specific train / test split
    # TODO: some pairs may not be sampled (for ex: Vald-Hamza) and they will not be proposed in the predictions at the end
    # -> Predict on all the pairs in the 2nd and 3rd hops of the considered artist (at the end)
    hamza = artist_specific_pdf(train, test, artist_node_id=111)

    # Build classifier
    # TODO: try a different classifier / hyper parameters
    # TODO : add features for same_label, nb_feats (i.e. duplicate the relationships?), recency, avg_popularity...
    train.drop(columns=["node1", "node2"])
    test.drop(columns=["node1", "node2"])
    classifier = RandomForestClassifier(n_estimators=30, max_depth=10, random_state=0)
    columns = [
        "cn", "pa", "tn",  # graph features
        "minTriangles", "maxTriangles", "minCoefficient", "maxCoefficient",  # triangle features
        "sp", "sl"  # community features

    ]
    X = train[columns]
    y = train["label"]
    classifier.fit(X, y)

    # Model analysis
    # TODO: at the end, re-train the validated model on all data (train + test)
    preds = classifier.predict(test[columns])
    y_test = test["label"]
    results = evaluate_model(preds, y_test)
    print(results)
    feature_expl = feature_importance(columns, classifier)
    print(feature_expl)

    # Artist specific predictions
    hamza = make_predictions(hamza, classifier, [
        "cn", "pa", "tn", "minTriangles", "maxTriangles", "minCoefficient", "maxCoefficient", "sp", "sl"
    ])
    display_predictions(hamza, 111)
