def write_louvain(driver):

    query = """
    CALL gds.louvain.stream({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: 'FEAT',
          orientation: 'UNDIRECTED'
        }
      },
      includeIntermediateCommunities: true
    })
    YIELD nodeId, communityId, intermediateCommunityIds
    WITH gds.util.asNode(nodeId) AS node, intermediateCommunityIds[0] AS smallestCommunity
    SET node.louvain_all = smallestCommunity;
    """

    with driver.session() as session:
        session.run(query)


def write_louvain_year(year, driver):
    rel_type = 'FEAT_' + str(year)
    property_name = 'louvain_' + str(year)
    query = """
    CALL gds.louvain.stream({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: '$rel_type',
          orientation: 'UNDIRECTED'
        }
      },
      includeIntermediateCommunities: true
    })
    YIELD nodeId, communityId, intermediateCommunityIds
    WITH gds.util.asNode(nodeId) AS node, intermediateCommunityIds[0] AS smallestCommunity
    SET node.$property_name = smallestCommunity;
    """
    params = {"rel_type": rel_type, "property_name": property_name}

    with driver.session() as session:
        session.run(query, params)



