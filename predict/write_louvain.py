from neo4j_handler import Neo4JHandler

graph = Neo4JHandler(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="root"
)
driver = graph.driver

query = """
CALL gds.louvain.stream({
  nodeProjection: 'Artist',
  relationshipProjection: {
    FEAT_EARLY: {
      type: 'FEAT_EARLY',
      orientation: 'UNDIRECTED'
    }
  },
  includeIntermediateCommunities: true
})
YIELD nodeId, communityId, intermediateCommunityIds
WITH gds.util.asNode(nodeId) AS node, intermediateCommunityIds[0] AS smallestCommunity
SET node.louvainTrain = smallestCommunity;
"""

with driver.session() as session:
    session.run(query)

query = """
CALL gds.louvain.stream({
  nodeProjection: 'Artist',
  relationshipProjection: {
    FEAT_LATE: {
      type: 'FEAT_LATE',
      orientation: 'UNDIRECTED'
    }
  },
  includeIntermediateCommunities: true
})
YIELD nodeId, communityId, intermediateCommunityIds
WITH gds.util.asNode(nodeId) AS node, intermediateCommunityIds[0] AS smallestCommunity
SET node.louvainTest = smallestCommunity;
"""

with driver.session() as session:
    session.run(query)
