import neo4j

from handlers.neo4j_handler import Neo4JHandler
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('database')
# logger.propagate = False<


class Database:

    def __init__(self,
                 neo4j_user, neo4j_password,
                 # spotify_client_id, spotify_client_secret
                 ):
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        # self.spotify_client_id = spotify_client_id
        # self.spotify_client_secret = spotify_client_secret
        # self.spotify = SpotifyLoader(
        #     client_id="28d60111ea634effb71f87304bed9285",
        #     client_secret="77f974dfa7c2412196a9e1b13e4f5e9e"
        # )
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
        self.graph.convert_pop_to_int()


if __name__ == "__main__":
    db = Database(
        neo4j_user="neo4j",
        neo4j_password="root",
    )

    # db.reset()


    logger.info('Starting artists writing to DB')
    artists = db.graph.create_artists('C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/scraping_history/artists.csv')
    logger.info('Ended artists writing to DB')

    logger.info('Starting genres writing to DB')
    genres = db.graph.create_genres('C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/scraping_history/genres.csv')
    logger.info('Ended genres writing to DB')

    logger.info('Starting labels writing to DB')
    labels = db.graph.create_labels('C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/scraping_history/labels.csv')
    logger.info('Ended labels writing to DB')

    db.post_treatment_db()
    logger.info("Removed duplicates in DB")

    logger.info('Starting feats writing to DB')
    feats = db.graph.create_feats('C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/scraping_history/feats.csv')
    logger.info('Ended feats writing to DB')
