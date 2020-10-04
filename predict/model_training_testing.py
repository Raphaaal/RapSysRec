from predict.feature_engineering import engineer_features
from neo4j_handler import Neo4JHandler
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
from sklearn.metrics import accuracy_score


pd.set_option('display.float_format', lambda x: '%.3f' % x)

graph = Neo4JHandler(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="root"
)
driver = graph.driver

train, test = engineer_features(driver)

# Hamza-specific
hamza_train = train.loc[(train['node1'] == 111) | (train['node2'] == 111)]
hamza_test = test.loc[(test['node1'] == 111) | (train['node2'] == 111)]
hamza = hamza_train.append(hamza_test, ignore_index=True)
print(hamza_test)

train.drop(columns=["node1", "node2"])
test.drop(columns=["node1", "node2"])

classifier = RandomForestClassifier(n_estimators=30, max_depth=10, random_state=0)
columns = [
    "cn", "pa", "tn",  # graph features
    # "minTriangles", "maxTriangles", "minCoefficient", "maxCoefficient", # triangle features
    # "sp", "sl" # community features
    # TODO : add features for label, nb_feats, recency, ...
]

X = train[columns]
y = train["label"]
classifier.fit(X, y)


def evaluate_model(predictions, actual):
    return pd.DataFrame({
        "Measure": ["Accuracy", "Precision", "Recall"],
        "Score": [accuracy_score(actual, predictions),
                  precision_score(actual, predictions),
                  recall_score(actual, predictions)]
    })


preds = classifier.predict(test[columns])
y_test = test["label"]

results = evaluate_model(preds, y_test)
print(results)

hamza_ids = hamza[['node1', 'node2']]
hamza_preds = classifier.predict(hamza.drop(columns=["node1", "node2"]))
hamza_preds_id = pd.concat([hamza_ids, hamza_preds], axis=1)
print(hamza_preds_id)

