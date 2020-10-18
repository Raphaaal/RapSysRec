from itertools import combinations
import neo4j
from history import check_artist_is_scraped, write_scraped_artist, check_album_is_scraped, write_scraped_album, \
    get_scraped_artists, truncate_file
from neo4j_handler import Neo4JHandler
from spotify_loader import SpotifyLoader
import logging
from multiprocessing.dummy import Pool as ThreadPool
import itertools
from pprint import pprint

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('database')
# logger.propagate = False<


class Database:

    def __init__(self, neo4j_user, neo4j_password, spotify_client_id, spotify_client_secret):
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.spotify_client_id = spotify_client_id
        self.spotify_client_secret = spotify_client_secret
        self.spotify = SpotifyLoader(
            client_id="28d60111ea634effb71f87304bed9285",
            client_secret="77f974dfa7c2412196a9e1b13e4f5e9e"
        )
        self.graph = Neo4JHandler(
            uri="bolt://localhost:7687",
            user="neo4j",
            password="root"
        )

    # def create_genres(self, urn, artist, genre):
    #     if genre:
    #         self.graph.merge_genre(genre)
    #         already_linked_artist_genres = self.graph.get_genre_artist(genre, artist)
    #         if not already_linked_artist_genres:
    #             self.graph.set_genre_artist(genre_name=genre, artist_urn=urn)
    #             # logger.info('Artist %s linked to genre %s.', urn, genre)

    # def create_feat_artist(self, album, artist, ft_artist):
    #     # Create artists if needed
    #     artist_test = self.graph.get_artist(urn=ft_artist['artist_id'])
    #     if not artist_test:
    #         name = ft_artist['artist_name']
    #         urn = ft_artist['artist_id']
    #         artist_object = self.spotify.get_artist_info(
    #             self.spotify.get_artist_by_id(ft_artist['artist_id'])
    #         )
    #         popularity = artist_object['artist_popularity']
    #         self.graph.merge_artist(name, urn, popularity)
    #         logger.info('Artist %s (%s) did not exist. Created in DB', name, urn)
    #
    #         # Merge and link artists' genres
    #         artist_genres = artist_object['artist_genres']
    #         for genre in artist_genres:
    #             self.create_genres(urn=urn, artist=artist, genre=genre)
    #         # Multi-threading pool -> Seemed to duplicate genre and links to genres
    #         # pool = ThreadPool(2)
    #         # results = pool.starmap(
    #         #     self.create_genres,
    #         #     zip(
    #         #         itertools.repeat(urn),
    #         #         itertools.repeat(artist),
    #         #         artist_genres
    #         #     )
    #         # )
    #         # pool.close()
    #         # pool.join()
    #
    #     # Create artist-label link if needed
    #     if not self.graph.get_label_artist(
    #             label_name=album['label'],
    #             album_date=album['release_date'],
    #             artist_urn=ft_artist['artist_id']
    #     ):
    #         self.graph.set_label_artist(
    #             label_name=album['label'],
    #             album_date=album['release_date'],
    #             artist_urn=ft_artist['artist_id']
    #         )
    #         # logger.info(
    #         #     'Artist %s linked to label %s at date %s',
    #         #     ft_artist['artist_id'], album['label'], album['release_date']
    #         # )

    # def create_from_feat(self, artist, album, feat):
    #     # Get featuring artists
    #     artists_id_list = []
    #     for ft_artist in feat['featuring_artists']:
    #         artists_id_list.append(ft_artist['artist_id'])
    #
    #     # Multi-threading pool
    #     pool = ThreadPool(2)
    #     results = pool.starmap(
    #         self.create_feat_artist,
    #         zip(
    #             itertools.repeat(album),
    #             itertools.repeat(artist),
    #             feat['featuring_artists']
    #         )
    #     )
    #     pool.close()
    #     pool.join()
    #
    #     # Create feat if needed
    #     feat_test = False
    #     for artists_pair in list(combinations(artists_id_list, 2)):
    #         feat_test = self.graph.get_feat(
    #             track_id=feat['track_id'],
    #             track_name=feat['track_name'],
    #             artist1_urn=artists_pair[0],
    #             artist2_urn=artists_pair[1]
    #         )
    #         if feat_test:
    #             break
    #     if not feat_test:
    #         # Handle feats with more than 2 artists
    #         for artists_pair in list(combinations(artists_id_list, 2)):
    #             self.graph.create_feat(
    #                 urn1=artists_pair[0],
    #                 urn2=artists_pair[1],
    #                 track_id=feat['track_id'],
    #                 track_name=feat['track_name'],
    #                 track_date=feat['track_date']
    #             )
    #             # logger.info(
    #             #     'Feat %s (%s) did not exist. Created in DB',
    #             #     feat['track_name'], feat['track_id']
    #             # )

    def create_from_album(self, scraped_album_csv_list, scraped_album_csv, artist, album):
        if album:
            # logger.info('Testing if album %s has already been scraped.', album['id'])
            if not check_album_is_scraped(scraped_album_csv=scraped_album_csv_list, album_urn=album['id']):
                # logger.info('Album %s has not already been scraped.', album['id'])

                # Get album's featuring info
                # logger.info('Getting album %s featurings.', album['id'])
                album_feat_info = self.spotify.get_album_ft_tracks(album)
                # logger.info('Got album %s featurings.', album['id'])
                if album_feat_info:
                    # Merge album label
                    # logger.info('Creating album %s label.', album['id'])
                    if album['label']:
                        self.graph.merge_label(label=album['label'], artist=artist)
                    # logger.info('Created album %s label.', album['id'])

                    # logger.info('Creating album %s featurings.', album['id'])
                    self.graph.create_multiple_feats(album_feat_info)
                    # logger.info('Created album %s featurings.', album['id'])
                    # TODO: misssing genre creations (and their relationships to artists)

                    # Multi-threading pool
                    # pool = ThreadPool(2)
                    # results = pool.starmap(
                    #     self.create_from_feat,
                    #     zip(
                    #         itertools.repeat(artist),
                    #         itertools.repeat(album),
                    #         album_feat_info
                    #     )
                    # )
                    # pool.close()
                    # pool.join()

                # else:
                    # logger.info('No feat info in album %s.', album['id'])
                write_scraped_album(scraped_albums_csv=scraped_album_csv, album_urn=album['id'])
                # logger.info('Album %s appended to scraped albums.', album['id'])
            # else:
            #     logger.info('Album %s already present in scraped albums.', album['id'])

    def create_from_artist(
            self, scraped_artist_csv_list, current_scraped_artist_csv, scraped_album_csv_list,
            current_scraped_album_csv, artist_urn
    ):
        # artist = self.spotify.get_artist_by_id(artist_urn)
        # logger.info('Testing if artist %s has already been scraped.', artist_urn)
        if not check_artist_is_scraped(scraped_artist_csv_list, artist_urn):
            logger.info('Artist %s has not yet been scraped.', artist_urn)
            # Get artist's albums
            # logger.info('Getting artist %s albums.', artist_urn)
            albums = self.spotify.get_artist_albums(artist_urn)
            # logger.info('Got artist %s albums.', artist_urn)

            for album in albums:
                self.create_from_album(scraped_album_csv_list, current_scraped_album_csv, artist_urn, album)

            # Multi-threading pool
            # pool = ThreadPool(2)
            # results = pool.starmap(
            #     self.create_from_album,
            #     zip(
            #         itertools.repeat(scraped_album_csv_list),
            #         itertools.repeat(current_scraped_album_csv),
            #         itertools.repeat(artist_urn),
            #         albums
            #     )
            # )
            # pool.close()
            # pool.join()
            write_scraped_artist(current_scraped_artist_csv, artist_urn)
            logger.info('Artist %s appended to scraped artists.', artist_urn)
        else:
            logger.info('Artist %s already present in scraped artists.', artist_urn)

    def expand_from_artist(self, artist_urn, scraped_artists_csv, scraped_albums_csv, nb_hops, reset=False):

        if reset:
            # Clear scraping history files and DB
            for i in range(nb_hops+1):
                truncate_file(scraped_artists_csv + "_" + str(i) + ".csv")
                truncate_file(scraped_albums_csv + "_" + str(i) + ".csv")
            self.graph.truncate()
            logger.info("Truncated DB and history files.")
            try:
                self.graph.set_constraints()
                logger.info("DB constraints set.")
            except neo4j.exceptions.ClientError:
                logger.info("Exception during DB constraints setting.")

        # Initiate with first artist scraping
        logger.info("==========================================================================")
        logger.info("=                                  HOP #0                                =")
        logger.info("==========================================================================")
        self.create_from_artist(
            [scraped_artists_csv + "_0.csv"],
            scraped_artists_csv + "_0.csv",
            [scraped_albums_csv + "_0.csv"],
            scraped_albums_csv + "_0.csv",
            artist_urn
        )

        # Hop on and scrape on
        for i in range(nb_hops):
            logger.info("==========================================================================")
            logger.info("=                                  HOP #%s                                =", str(i+1))
            logger.info("==========================================================================")
            scraped_artists = get_scraped_artists([scraped_artists_csv + "_" + str(k) + ".csv" for k in range(i+2)])
            for artist in scraped_artists:
                linked_artists = self.graph.get_unscraped_linked_artists(
                    artist_urn=artist,
                    scraped_artists_urn_list=scraped_artists
                )

                for linked_artist in linked_artists:
                    self.create_from_artist(
                        [scraped_artists_csv + "_" + str(k) + ".csv" for k in range(i + 2)],
                        scraped_artists_csv + "_" + str(i + 1) + ".csv",
                        [scraped_albums_csv + "_" + str(k) + ".csv" for k in range(i + 2)],
                        scraped_albums_csv + "_" + str(i + 1) + ".csv",
                        linked_artist
                    )
                # Multi-threading pool
                # pool = ThreadPool(2)
                # results = pool.starmap(
                #     self.create_from_artist,
                #     zip(
                #         itertools.repeat([scraped_artists_csv + "_" + str(k) + ".csv" for k in range(i+2)]),
                #         itertools.repeat(scraped_artists_csv + "_" + str(i + 1) + ".csv"),
                #         itertools.repeat([scraped_albums_csv + "_" + str(k) + ".csv" for k in range(i+2)]),
                #         itertools.repeat(scraped_albums_csv + "_" + str(i + 1) + ".csv"),
                #         linked_artists
                #     )
                # )
                # pool.close()
                # pool.join()

    def delete_duplicate_artist(self):
        self.graph.delete_duplicate_artists()

    def delete_nodes_without_label(self):
        self.graph.delete_nodes_without_label()


if __name__ == "__main__":

    # TODO: Améliorer vitesse d'exécution (pool nb ? moins de requêtes à Spotify / à la DB ?)
    # TODO: changer les CREATE pour des MERGE au cas où

    db = Database(
        neo4j_user="neo4j",
        neo4j_password="root",
        spotify_client_id="28d60111ea634effb71f87304bed9285",
        spotify_client_secret="77f974dfa7c2412196a9e1b13e4f5e9e"
    )
    db.expand_from_artist(
        artist_urn="5gs4Sm2WQUkcGeikMcVHbh",
        scraped_artists_csv="scraping_history/scraped_artists",
        scraped_albums_csv="scraping_history/scraped_albums",
        nb_hops=4,
        reset=False
    )
    # Multi-threading may create duplicate artists because of concurrent scraping and creation
    db.delete_duplicate_artist()
    db.delete_nodes_without_label()

