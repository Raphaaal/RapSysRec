def write_triangles_props(driver):
    query = """
    CALL gds.triangleCount.write({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: 'FEAT',
          orientation: 'UNDIRECTED'
        }
      },
      writeProperty: 'triangles_all'
    });
    """

    with driver.session() as session:
        result = session.run(query)


def write_triangles_props_year(year, driver):
    rel_type = 'FEAT_' + str(year)
    prop_name = 'triangles_'+ str(year)
    query = """
    CALL gds.triangleCount.write({
      nodeProjection: 'Artist',
      relationshipProjection: {
        FEAT: {
          type: '$rel_type',
          orientation: 'UNDIRECTED'
        }
      },
      writeProperty: $prop_name
    });
    """
    params = {"rel_type": rel_type, "prop_name": prop_name}

    with driver.session() as session:
        result = session.run(query, params)
