def write_label_propagation(driver):

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

    query = """
    CALL gds.labelPropagation.write({
      nodeProjection: "Artist",
      relationshipProjection: {
        FEAT: {
          type: 'FEAT',
          orientation: 'UNDIRECTED'
        }
      },
      writeProperty: "partition"
    });
    """

    with driver.session() as session:
        result = session.run(query)
