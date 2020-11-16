def write_label_propagation(driver):

    query = """
    CALL gds.labelPropagation.write({
      nodeProjection: "Artist",
      relationshipProjection: {
        FEAT: {
          type: 'FEAT',
          orientation: 'UNDIRECTED'
        }
      },
      writeProperty: "partition_all"
    });
    """

    with driver.session() as session:
        result = session.run(query)


def write_label_propagation_year(year, driver):
    rel_type = 'FEAT_' + str(year)
    property_name = 'partition_' + str(year)
    query = """
    CALL gds.labelPropagation.write({
      nodeProjection: "Artist",
      relationshipProjection: {
        FEAT: {
          type: $rel_type,
          orientation: 'UNDIRECTED'
        }
      },
      writeProperty: $property_name
    });
    """
    params = {"rel_type": rel_type, "property_name": property_name}

    with driver.session() as session:
        result = session.run(query, params)
