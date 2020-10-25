def write_triangles_props(driver):

    query = """
    CALL gds.triangleCount.write({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT_EARLY: {
          type: 'FEAT_EARLY',
          orientation: 'UNDIRECTED'
        }
      },
      writeProperty: 'trianglesTrain'
    });
    """

    with driver.session() as session:
        result = session.run(query)

    query = """
    CALL gds.triangleCount.write({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT_LATE: {
          type: 'FEAT_LATE',
          orientation: 'UNDIRECTED'
        }
      },
      writeProperty: 'trianglesTest'
    });
    """

    with driver.session() as session:
        result = session.run(query)

    query = """
    CALL gds.triangleCount.write({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: 'FEAT',
          orientation: 'UNDIRECTED'
        }
      },
      writeProperty: 'triangles'
    });
    """

    with driver.session() as session:
        result = session.run(query)
