from itertools import combinations

from neo4j_handler import Neo4JHandler
from spotify_loader import SpotifyLoader
from pprint import pprint
import logging

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

    def create_from_artist(self, artist_urn):
        artist = self.spotify.get_artist_by_id(artist_urn)
        feat_info = self.spotify.get_artist_ft_tracks(artist)
        for album in feat_info:
            for feat in album:
                artists_id_list = []
                for ft_artist in feat['featuring_artists']:
                    artists_id_list.append(ft_artist['artist_id'])

                    # Test if artist already exists
                    artist_test = self.graph.get_print_artist(urn=ft_artist['artist_id'])
                    if not artist_test:
                        name = ft_artist['artist_name']
                        urn = ft_artist['artist_id']
                        # TODO: save API calls by getting artist popularity upstream ?
                        popularity = self.spotify.get_artist_info(
                            self.spotify.get_artist_by_id(ft_artist['artist_id'])
                        )['artist_popularity']
                        # TODO: Handle artists genres creation / relationships
                        self.graph.create_print_artist(name, urn, popularity)
                        logger.info('Artist %s (%s) did not exist. Created in DB', name, urn)

                # Test if feat already exists
                feat_test = False
                for artists_pair in list(combinations(artists_id_list, 2)):
                    feat_test = self.graph.get_print_feat(
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
                        self.graph.create_print_feat(
                            urn1=artists_pair[0],
                            urn2=artists_pair[1],
                            track_id=feat['track_id'],
                            track_name=feat['track_name'],
                            track_date=feat['track_date']
                        )
                        logger.info('Feat %s (%s) did not exist. Created in DB', feat['track_name'], feat['track_id'])


if __name__ == "__main__":
    db = Database(
        neo4j_user="neo4j",
        neo4j_password="root",
        spotify_client_id="28d60111ea634effb71f87304bed9285",
        spotify_client_secret="77f974dfa7c2412196a9e1b13e4f5e9e"
    )
    db.create_from_artist("58wXmynHaAWI5hwlPZP3qL")
