from itertools import combinations

from history import check_artist_is_scraped, write_scraped_artist, check_album_is_scraped, write_scraped_album
from neo4j_handler import Neo4JHandler
from spotify_loader import SpotifyLoader
import logging
from pprint import pprint

logger = logging.getLogger('database')
logging.basicConfig(level='INFO')


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

    def create_from_artist(self, scraped_artist_csv, scraped_album_csv, artist_urn):
        logger.info('===== Starting feat scraping for every album where artist %s appears =====', artist_urn)
        artist = self.spotify.get_artist_by_id(artist_urn)
        if not check_artist_is_scraped(scraped_artist_csv, artist_urn):

            # Get artist's albums
            albums = self.spotify.get_artist_albums(artist)
            for album in albums:
                if album:
                    if not check_album_is_scraped(scraped_album_csv=scraped_album_csv, album_urn=album['id']):

                        # Get album's featuring info
                        album_feat_info = self.spotify.get_album_ft_tracks(album)
                        if album_feat_info:
                            for feat in album_feat_info:

                                # Get featuring artists
                                artists_id_list = []
                                for ft_artist in feat['featuring_artists']:
                                    artists_id_list.append(ft_artist['artist_id'])

                                    # Create artists if needed
                                    artist_test = self.graph.get_artist(urn=ft_artist['artist_id'])
                                    if not artist_test:
                                        name = ft_artist['artist_name']
                                        urn = ft_artist['artist_id']
                                        artist_object = self.spotify.get_artist_info(
                                            self.spotify.get_artist_by_id(ft_artist['artist_id'])
                                        )
                                        popularity = artist_object['artist_popularity']
                                        self.graph.create_artist(name, urn, popularity)
                                        logger.info('Artist %s (%s) did not exist. Created in DB', name, urn)

                                        # Merge and link artists' genres
                                        artist_genres = artist_object['artist_genres']
                                        for genre in artist_genres:
                                            if genre:
                                                self.graph.merge_genre(genre)
                                                already_linked_artist_genres = self.graph.get_genre_artist(genre, artist)
                                                if not already_linked_artist_genres:
                                                    self.graph.set_genre_artist(genre_name=genre, artist_urn=urn)
                                                    logger.info('Artist %s linked to genre %s.', urn, genre)

                                # Create feat if needed
                                feat_test = False
                                for artists_pair in list(combinations(artists_id_list, 2)):
                                    feat_test = self.graph.get_feat(
                                        track_id=feat['track_id'],
                                        track_name=feat['track_name'],
                                        artist1_urn=artists_pair[0],
                                        artist2_urn=artists_pair[1]
                                    )
                                    if feat_test:
                                        break
                                if not feat_test:
                                    # Handle feats with more than 2 artists
                                    for artists_pair in list(combinations(artists_id_list, 2)):
                                        self.graph.create_feat(
                                            urn1=artists_pair[0],
                                            urn2=artists_pair[1],
                                            track_id=feat['track_id'],
                                            track_name=feat['track_name'],
                                            track_date=feat['track_date']
                                        )
                                        logger.info(
                                            'Feat %s (%s) did not exist. Created in DB',
                                            feat['track_name'], feat['track_id']
                                        )
                        else:
                            logger.info('No feat info in album %s.', album['id'])
                        write_scraped_album(scraped_albums_csv=scraped_album_csv, album_urn=album['id'])
                        logger.info('Album %s appended to scraped albums.', album['id'])
                    else:
                        logger.info('Album %s already present in scraped albums.', album['id'])

            write_scraped_artist(scraped_artist_csv, artist_urn)
            logger.info('Artist %s appended to scraped artists.', artist_urn)
        else:
            logger.info('Artist %s already present in scraped artists.', artist_urn)


if __name__ == "__main__":
    db = Database(
        neo4j_user="neo4j",
        neo4j_password="root",
        spotify_client_id="28d60111ea634effb71f87304bed9285",
        spotify_client_secret="77f974dfa7c2412196a9e1b13e4f5e9e"
    )
    # db.create_from_artist(scraped_artist_csv='scraped_artists.csv', artist_urn="58wXmynHaAWI5hwlPZP3qL")  # Booba
    # db.create_from_artist('scraped_artists.csv', 'scraped_albums.csv', "76Pl0epAMXVXJspaSuz8im")  # Freeze Corleone
    db.create_from_artist('scraped_artists.csv', 'scraped_albums.csv', "28gNT5KBp7IjEOQoevXf9N")  # Camilo
    # db.create_from_artist('scraped_artists.csv', "6Te49r3A6f5BiIgBRxH7FH")  # Ninho
