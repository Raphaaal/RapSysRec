from handlers.neo4j_handler import Neo4JHandler
from predict.prepare_graph.write_clustering_coeff import write_clustering_coeff, write_clustering_coeff_year
from predict.prepare_graph.write_label_propagation import write_label_propagation, write_label_propagation_year
from predict.prepare_graph.write_louvain import write_louvain, write_louvain_year
from predict.prepare_graph.write_triangles_property import write_triangles_props, write_triangles_props_year

if __name__ == '__main__':
    # Graph DB connection
    graph = Neo4JHandler(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="root"
    )
    driver = graph.driver

    write_louvain(driver)
    write_triangles_props(driver)
    write_clustering_coeff(driver)
    write_label_propagation(driver)

    years = ['2015', '2016', '2017', '2018', '2019']
    for year in years:
        write_louvain_year(year, driver)
        write_triangles_props_year(year, driver)
        write_clustering_coeff_year(year, driver)
        write_label_propagation_year(year, driver)
