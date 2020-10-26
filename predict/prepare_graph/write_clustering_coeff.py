def write_clustering_coeff(driver):

    # query = """
    # CALL gds.localClusteringCoefficient.write({
    #   nodeProjection: 'Artist',
    #   relationshipProjection: {
    #     FEAT_EARLY: {
    #       type: 'FEAT_EARLY',
    #       orientation: 'UNDIRECTED'
    #     }
    #   },
    #   writeProperty: 'coefficientTrain'
    # });
    # """
    #
    # with driver.session() as session:
    #     result = session.run(query)
    #
    # query = """
    # CALL gds.localClusteringCoefficient.write({
    #   nodeProjection: 'Artist',
    #   relationshipProjection: {
    #     FEAT_LATE: {
    #       type: 'FEAT_LATE',
    #       orientation: 'UNDIRECTED'
    #     }
    #   },
    #   writeProperty: 'coefficientTest'
    # });
    # """
    #
    # with driver.session() as session:
    #     result = session.run(query)

    query = """
    CALL gds.localClusteringCoefficient.write({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: 'FEAT',
          orientation: 'UNDIRECTED'
        }
      },
      writeProperty: 'coefficient'
    });
    """

    with driver.session() as session:
        result = session.run(query)
