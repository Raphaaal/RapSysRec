import neo4j

from handlers.csv_handler import remove_nan
from handlers.neo4j_handler import Neo4JHandler
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('database')


# logger.propagate = False<


class Database:

    def __init__(self, neo4j_user, neo4j_password):
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.graph = Neo4JHandler(
            uri="bolt://localhost:7687",
            user=neo4j_user,
            password=neo4j_password
        )

    def reset(self):
        self.graph.truncate()
        try:
            self.graph.set_constraints()
            logger.info("DB constraints set.")
        except neo4j.exceptions.ClientError:
            logger.info("Exception during DB constraints setting.")

        logger.info('Reset done.')

    def delete_duplicate_artist(self):
        self.graph.delete_duplicate_artists()

    def delete_nodes_without_label(self):
        self.graph.delete_nodes_without_label()

    def post_treatment_db(self):
        self.graph.delete_duplicate_artists()
        self.delete_nodes_without_label()
        self.graph.convert_pop_to_int()


if __name__ == "__main__":
    db = Database(
        neo4j_user="neo4j",
        neo4j_password="root",
    )

    # db.reset()

    # logger.info('Starting artists writing to DB')
    artists = db.graph.create_artists('C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/artists.csv')
    # logger.info('Ended artists writing to DB')

    # logger.info('Starting genres writing to DB')
    # genres = db.graph.create_genres('C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/genres.csv')
    # logger.info('Ended genres writing to DB')

    # remove_nan(
    #     'C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/labels.csv',
    #     ['artist_urn', 'label', 'date']
    # )
    # logger.info('Starting labels writing to DB')
    # labels = db.graph.create_labels('C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/labels.csv')
    # logger.info('Ended labels writing to DB')

    # db.post_treatment_db()
    # logger.info("Removed duplicates in DB")

    # logger.info('Starting feats writing to DB')
    # remove_nan(
    #     'C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/feats.csv',
    #     ['artist_urn', 'artist_name', 'track_name', 'track_id', 'track_date', 'featuring_artist_urn', 'featuring_artist_name']
    # )
    # feats = db.graph.create_feats_all(csv_path='C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/feats.csv')
    # logger.info('Ended feats (all) writing to DB')
    feats_years = db.graph.create_feats_year_2015(csv_path='C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/feats.csv')
    logger.info('Ended feats (year 2015) writing to DB')
    feats_years = db.graph.create_feats_year_2016(
        csv_path='C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/feats.csv')
    logger.info('Ended feats (year 2016) writing to DB')
    feats_years = db.graph.create_feats_year_2017(
        csv_path='C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/feats.csv')
    logger.info('Ended feats (year 2017) writing to DB')
    feats_years = db.graph.create_feats_year_2018(
        csv_path='C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/feats.csv')
    logger.info('Ended feats (year 2018) writing to DB')
    feats_years = db.graph.create_feats_year_2019(
        csv_path='C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/feats.csv')
    logger.info('Ended feats (year 2019) writing to DB')
    feats_years = db.graph.create_feats_year_2020(
        csv_path='C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/data_collection/scraping_history/feats.csv')
    logger.info('Ended feats (year 2020) writing to DB')

    db.post_treatment_db()
    db.graph.close()
