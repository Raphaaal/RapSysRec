from neo4j_handler import Neo4JHandler
from predict.prepare_graph.early_late_split import split_early_late
from predict.prepare_graph.ft_year_distribution import set_feat_nb, set_max_year, get_year_distribution
from predict.prepare_graph.write_clustering_coeff import write_clustering_coeff
from predict.prepare_graph.write_label_propagation import write_label_propagation
from predict.prepare_graph.write_louvain import write_louvain
from predict.prepare_graph.write_triangles_property import write_triangles_props

if __name__ == '__main__':
    # Graph DB connection
    graph = Neo4JHandler(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="root"
    )
    driver = graph.driver

    # set_feat_nb(driver)
    # set_max_year(driver)
    # get_year_distribution(driver)

    # split_early_late(driver, min_year=2016)

    write_louvain(driver)
    write_triangles_props(driver)
    write_clustering_coeff(driver)
    write_label_propagation(driver)
