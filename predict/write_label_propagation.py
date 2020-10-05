from neo4j_handler import Neo4JHandler

graph = Neo4JHandler(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="root"
)
driver = graph.driver

query = """
CALL gds.labelPropagation.write({
  nodeProjection: "Artist",
  relationshipProjection: {
    FEAT_EARLY: {
      type: 'FEAT_EARLY',
      orientation: 'UNDIRECTED'
    }
  },
  writeProperty: "partitionTrain"
});
"""

with driver.session() as session:
    result = session.run(query)

query = """
CALL gds.labelPropagation.write({
  nodeProjection: "Artist",
  relationshipProjection: {
    FEAT_LATE: {
      type: 'FEAT_LATE',
      orientation: 'UNDIRECTED'
    }
  },
  writeProperty: "partitionTest"
});
"""

with driver.session() as session:
    result = session.run(query)
