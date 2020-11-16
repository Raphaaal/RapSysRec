def write_clustering_coeff(driver):
    query = """
    CALL gds.localClusteringCoefficient.write({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: 'FEAT',
          orientation: 'UNDIRECTED'
        }
      },
      writeProperty: 'coefficient_all'
    });
    """

    with driver.session() as session:
        result = session.run(query)


def write_clustering_coeff_year(year, driver):
    rel_type = 'FEAT_' + str(year)
    property_name = 'coefficient_' + str(year)
    query = """
    CALL gds.localClusteringCoefficient.write({
      nodeProjection: 'Artist',
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
