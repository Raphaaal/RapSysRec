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


def write_louvain_2015(driver, year=2015):
    rel_type = 'FEAT_' + str(year)
    query = """
    CALL gds.louvain.stream({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: $rel_type,
          orientation: 'UNDIRECTED'
        }
      },
      includeIntermediateCommunities: true
    })
    YIELD nodeId, communityId, intermediateCommunityIds
    WITH gds.util.asNode(nodeId) AS node, intermediateCommunityIds[0] AS smallestCommunity
    SET node.louvain_2015 = smallestCommunity;
    """
    params = {"rel_type": rel_type}

    with driver.session() as session:
        session.run(query, params)


def write_louvain_2016(driver, year=2016):
    rel_type = 'FEAT_' + str(year)
    query = """
    CALL gds.louvain.stream({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: $rel_type,
          orientation: 'UNDIRECTED'
        }
      },
      includeIntermediateCommunities: true
    })
    YIELD nodeId, communityId, intermediateCommunityIds
    WITH gds.util.asNode(nodeId) AS node, intermediateCommunityIds[0] AS smallestCommunity
    SET node.louvain_2016 = smallestCommunity;
    """
    params = {"rel_type": rel_type}

    with driver.session() as session:
        session.run(query, params)


def write_louvain_2017(driver, year=2017):
    rel_type = 'FEAT_' + str(year)
    query = """
    CALL gds.louvain.stream({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: $rel_type,
          orientation: 'UNDIRECTED'
        }
      },
      includeIntermediateCommunities: true
    })
    YIELD nodeId, communityId, intermediateCommunityIds
    WITH gds.util.asNode(nodeId) AS node, intermediateCommunityIds[0] AS smallestCommunity
    SET node.louvain_2017 = smallestCommunity;
    """
    params = {"rel_type": rel_type}

    with driver.session() as session:
        session.run(query, params)


def write_louvain_2018(driver, year=2018):
    rel_type = 'FEAT_' + str(year)
    query = """
    CALL gds.louvain.stream({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: $rel_type,
          orientation: 'UNDIRECTED'
        }
      },
      includeIntermediateCommunities: true
    })
    YIELD nodeId, communityId, intermediateCommunityIds
    WITH gds.util.asNode(nodeId) AS node, intermediateCommunityIds[0] AS smallestCommunity
    SET node.louvain_2018 = smallestCommunity;
    """
    params = {"rel_type": rel_type}

    with driver.session() as session:
        session.run(query, params)


def write_louvain_2019(driver, year=2019):
    rel_type = 'FEAT_' + str(year)
    query = """
    CALL gds.louvain.stream({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: $rel_type,
          orientation: 'UNDIRECTED'
        }
      },
      includeIntermediateCommunities: true
    })
    YIELD nodeId, communityId, intermediateCommunityIds
    WITH gds.util.asNode(nodeId) AS node, intermediateCommunityIds[0] AS smallestCommunity
    SET node.louvain_2019 = smallestCommunity;
    """
    params = {"rel_type": rel_type}

    with driver.session() as session:
        session.run(query, params)