import io
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
import csv
import json
import pandas as pd

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('database')
# logger.propagate = False<


def add_track_date(featurings, date):
    for feat in featurings:
        feat['track_date'] = str(date)
    return featurings


def write_album_to_csv(featurings, csv_path):
    with io.open(csv_path, 'a', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        for feat in featurings:
            csv_writer.writerow(
                [
                    feat["artist_urn"],
                    feat["artist_name"],
                    feat["track_name"],
                    feat["track_id"],
                    feat['track_date'],
                    feat["featuring_artist_urn"],
                    feat["featuring_artist_name"]
                ]
            )


def write_linked_artists_to_csv(featurings, artist_urn, output_linked_artists):
    with io.open(output_linked_artists, 'a', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        for feat in featurings:
            if feat["artist_urn"] == artist_urn:
                csv_writer.writerow(
                    [
                        feat["featuring_artist_urn"]
                    ]
                )
            else:
                csv_writer.writerow(
                    [
                        feat["artist_urn"]
                    ]
                )


def initialize_linked_artists_csv(nb_hops, output_linked_artists, first_artist_urn):
    for i in range(nb_hops+1):
        truncate_file(output_linked_artists + "_" + str(i) + ".csv")
        with io.open(output_linked_artists + "_" + str(i) + ".csv", 'a', newline='', encoding='utf-8') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(["urn"])
            if i == 0:
                csv_writer.writerow([first_artist_urn])


def write_label_to_csv(label, csv_path):
    with io.open(csv_path, 'a', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(
            [
                label["artist_urn"],
                label["label"]
            ]
        )


def write_genre_to_csv(genres, csv_path):
    with io.open(csv_path, 'a', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        for genre in genres:
            csv_writer.writerow(
                [
                    genre["artist_urn"],
                    genre['artist_name'],
                    genre['artist_popularity'],
                    genre["genre"]
                ]
            )


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

    def create_from_album(self, output_label, output_feat, output_linked_artists, album, artist_urn):
        # Featurings
        featurings = self.spotify.get_album_featurings(album)
        if featurings:
            featurings = add_track_date(featurings, album['release_date'])
            write_album_to_csv(featurings, output_feat)
            write_linked_artists_to_csv(featurings, artist_urn, output_linked_artists + ".csv")

        # Label
        label = {
            'artist_urn': artist_urn,
            'label': album['label']
        }
        write_label_to_csv(label, output_label)

    def create_from_artist(self, output_label, output_feat, output_genre, output_linked_artists, artist_urn):
        # Albums featuring info
        albums = self.spotify.get_artist_albums(artist_urn)
        for album in albums:
            self.create_from_album(output_label, output_feat, output_linked_artists, album, artist_urn)

        # Genres
        artist = self.spotify.get_artist_info(self.spotify.get_artist_by_id(artist_urn))
        genres = [
            {
                'artist_urn': artist_urn,
                'artist_name': artist['artist_name'],
                'artist_popularity': artist['artist_popularity'],
                'genre': genre
            } for genre in artist['artist_genres']
        ]
        write_genre_to_csv(genres, output_genre)

        logger.info('Artist %s scraped.', artist['artist_name'])

    def reset(self):
        truncate_file("scraping_history/feats.csv")
        album = [
            {
                    "artist_urn": "artist_urn",
                    "artist_name": "artist_name",
                    "track_name": "track_name",
                    "track_id": "track_id",
                    "track_date": "track_date",
                    "featuring_artist_urn": "featuring_artist_urn",
                    "featuring_artist_name": "featuring_artist_name"
                }
        ]
        write_album_to_csv(album, "scraping_history/feats.csv")

        truncate_file("scraping_history/genres.csv")
        genre = [
            {
                'artist_urn': 'artist_urn',
                'artist_name': 'artist_name',
                'artist_popularity': 'artist_popularity',
                'genre': 'genre'
            }
        ]
        write_genre_to_csv(genre, "scraping_history/genres.csv")

        truncate_file("scraping_history/labels.csv")
        label = {
            'artist_urn': 'artist_urn',
            'label': 'label'
        }
        write_label_to_csv(label, "scraping_history/labels.csv")

        self.graph.truncate()
        try:
            self.graph.set_constraints()
            logger.info("DB constraints set.")
        except neo4j.exceptions.ClientError:
            logger.info("Exception during DB constraints setting.")

        logger.info('Reset done.')

    def expand_from_artist(self, output_feat, output_label, output_genre, output_linked_artists, artist_urn, nb_hops):

        # Initiate with first artist scraping
        initialize_linked_artists_csv(nb_hops, output_linked_artists, artist_urn)
        logger.info("==========================================================================")
        logger.info("=                                  HOP #0                                =")
        logger.info("==========================================================================")
        self.create_from_artist( output_label, output_feat, output_genre, output_linked_artists + "_1", artist_urn)

        # Hop on and scrape on
        for i in range(1, nb_hops):
            logger.info("==========================================================================")
            logger.info("=                                  HOP #%s                                =", str(i))
            logger.info("==========================================================================")
            # Isolate artists already scraped
            scraped_artists = pd.read_csv(output_linked_artists + "_0.csv").drop_duplicates()
            for k in range(1, i-1):
                scraped_artists = scraped_artists.append(
                    pd.read_csv(output_linked_artists + "_" + str(k) + ".csv").drop_duplicates()
                )
            # Get artists to scrap
            linked_artists = pd.read_csv(output_linked_artists + "_" + str(i) + ".csv").drop_duplicates()
            linked_artists = pd.concat([linked_artists, scraped_artists]).drop_duplicates(keep=False).values.tolist()
            for urn in linked_artists:
                self.create_from_artist(
                    output_label,
                    output_feat,
                    output_genre,
                    output_linked_artists + "_" + str(i+1),
                    urn[0]
                )

    def delete_duplicate_artist(self):
        self.graph.delete_duplicate_artists()

    def delete_nodes_without_label(self):
        self.graph.delete_nodes_without_label()


if __name__ == "__main__":
    db = Database(
        neo4j_user="neo4j",
        neo4j_password="root",
        spotify_client_id="28d60111ea634effb71f87304bed9285",
        spotify_client_secret="77f974dfa7c2412196a9e1b13e4f5e9e"
    )

    db.reset()

    db.expand_from_artist(
        output_feat="scraping_history/feats.csv",
        output_label="scraping_history/labels.csv",
        output_genre="scraping_history/genres.csv",
        output_linked_artists="scraping_history/linked_artists",
        nb_hops=5,
        artist_urn="5gs4Sm2WQUkcGeikMcVHbh"
    )

    # db.create_from_artist(
    #     output_feat="scraping_history/feats.csv",
    #     output_label="scraping_history/labels.csv",
    #     output_genre="scraping_history/genres.csv",
    #     output_linked_artists="scraping_history/linked_artists_1",
    #     artist_urn="5gs4Sm2WQUkcGeikMcVHbh"
    # )

    genres = db.graph.create_genres('C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/scraping_history/genres.csv')
    feats = db.graph.create_feats('C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/scraping_history/feats.csv')
    labels = db.graph.create_labels('C:/Users/patafilm/Documents/Projets/RapSysRec/RapSysRec/scraping_history/labels.csv')


