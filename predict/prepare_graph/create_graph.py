from handlers.neo4j_handler import Neo4JHandler
from predict.prepare_graph.write_clustering_coeff import write_clustering_coeff, write_clustering_coeff_year
from predict.prepare_graph.write_label_propagation import write_label_propagation, write_label_propagation_year
from predict.prepare_graph.write_louvain import write_louvain, write_louvain_2015, \
    write_louvain_2016, write_louvain_2017, write_louvain_2018, write_louvain_2019
from predict.prepare_graph.write_triangles_property import write_triangles_props, write_triangles_props_year
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('database')


# logger.propagate = False
if __name__ == '__main__':
    # Graph DB connection
    graph = Neo4JHandler(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="root"
    )
    driver = graph.driver

    logger.info("Start graph preparation")

    logger.info("Start triangles_all writing")
    write_triangles_props(driver)
    logger.info("Start coefficient_all writing")
    write_clustering_coeff(driver)
    logger.info("Start partition_all writing")
    write_label_propagation(driver)

    logger.info("Start Louvain_all writing")
    write_louvain(driver)
    logger.info("Start Louvain_2015 writing")
    write_louvain_2015(driver, year=2015)
    logger.info("Start Louvain_2016 writing")
    write_louvain_2016(driver, year=2016)
    logger.info("Start Louvain_2017 writing")
    write_louvain_2017(driver, year=2017)
    logger.info("Start Louvain_2018 writing")
    write_louvain_2018(driver, year=2018)
    logger.info("Start Louvain_2019 writing")
    write_louvain_2019(driver, year=2019)

    years = ['2015', '2016', '2017', '2018', '2019']
    for year in years:
        logger.info("Start triangles_%s writing", str(year))
        write_triangles_props_year(year, driver)
        logger.info("Start coefficient_%s writing", str(year))
        write_clustering_coeff_year(year, driver)
        logger.info("Start partition_%s writing", str(year))
        write_label_propagation_year(year, driver)

    graph.close()
    logger.info("End of graph preparation")
