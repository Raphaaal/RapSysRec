import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from neo4j_handler import Neo4JHandler

plt.style.use('fivethirtyeight')
pd.set_option('display.float_format', lambda x: '%.3f' % x)
graph = Neo4JHandler(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="root"
)
driver = graph.driver

# Set nb of featurings between artists in a new FEAT_NB unique relationship
query = """
MATCH (a:Artist)-[:FEAT]-(b:Artist)
WITH count(*) as collabs, a, b
MERGE (a)-[:FEAT_NB {nb_feats: collabs}]-(b);
"""

with driver.session() as session:
    session.run(query)

# Set last year of featuring between artists
# TODO: how to take the impact of the recency of each featuring if we only take the last featuring into account?
query = """
MATCH (a:Artist)-[r:FEAT]-(b:Artist)-[r2:FEAT_NB]-(a)
WITH MAX(toInteger(date(r.track_date).year)) AS year, a, b, r2
SET  r2.year = year
"""

with driver.session() as session:
    session.run(query)

query = """
MATCH p=()-[r:FEAT_NB]->()
WITH r.year AS year, count(*) AS count
ORDER BY year
RETURN toString(year) AS year, count
"""

with driver.session() as session:
    result = session.run(query)
    by_year = pd.DataFrame([dict(record) for record in result])

ax = by_year.plot(kind='bar', x='year', y='count', legend=None, figsize=(15, 8))
ax.xaxis.set_label_text("")
plt.tight_layout()

plt.show()
