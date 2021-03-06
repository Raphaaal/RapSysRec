import io
import requests
from handlers.csv_handler import truncate_file
from data_collection.scraping_history.scraping_history_handler import post_treatment_scraping_history
from handlers.spotify_loader import SpotifyLoader
import logging
import csv
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from pprint import pprint

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('database')


# logger.propagate = False


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
    for i in range(nb_hops + 1):
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
                label["label"],
                label["date"]
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


def write_artist_to_csv(artists, csv_path):
    with io.open(csv_path, 'a', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        for artist in artists:
            csv_writer.writerow(
                [
                    artist["artist_urn"],
                    artist["artist_name"],
                    artist["artist_popularity"],
                    artist["nb_tracks_2015"],
                    artist["nb_tracks_2016"],
                    artist["nb_tracks_2017"],
                    artist["nb_tracks_2018"],
                    artist["nb_tracks_2019"],
                    artist["nb_tracks_2020"],
                ]
            )


def get_album_release_dt(album):
    album_release_dt = None
    try:
        if len(album['release_date']) == 4:
            album_release_dt = datetime.strptime(album['release_date'], '%Y')
        if len(album['release_date']) == 7:
            album_release_dt = datetime.strptime(album['release_date'], '%Y-%m')
        if len(album['release_date']) == 10:
            album_release_dt = datetime.strptime(album['release_date'], '%Y-%m-%d')
    except ValueError:
        album_release_dt = None
    return album_release_dt


def write_sample_linked_artist(linked_artists, csv_path):
    with io.open(csv_path, 'a', newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        for artist in linked_artists:
            csv_writer.writerow(
                [
                    artist["urn"]
                ]
            )


def reset():
    truncate_file("scraping_history/artists.csv")
    artist = [
        {
            'artist_urn': 'artist_urn',
            'artist_name': 'artist_name',
            'artist_popularity': 'artist_popularity',
            'nb_tracks_2015': 'nb_tracks_2015',
            'nb_tracks_2016': 'nb_tracks_2016',
            'nb_tracks_2017': 'nb_tracks_2017',
            'nb_tracks_2018': 'nb_tracks_2018',
            'nb_tracks_2019': 'nb_tracks_2019',
            'nb_tracks_2020': 'nb_tracks_2020',
        }
    ]
    write_artist_to_csv(artist, "scraping_history/artists.csv")

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
        'label': 'label',
        'date': 'date'
    }
    write_label_to_csv(label, "scraping_history/labels.csv")

    for i in range(5):
        path = "scraping_history/linked_artists_" + str(i) + ".csv"
        truncate_file(path)
        linked_artist = [
            {
                "urn": "urn"
            }
        ]

        write_sample_linked_artist(linked_artist, path)

    logger.info('Reset done.')


class Scraping:

    def __init__(self, spotify_client_id, spotify_client_secret):
        self.spotify_client_id = spotify_client_id
        self.spotify_client_secret = spotify_client_secret
        self.spotify = SpotifyLoader(
            client_id=self.spotify_client_id,
            client_secret=self.spotify_client_secret
        )

    def create_from_album(self, output_label, output_feat, output_linked_artists, album, artist_urn):
        # Featurings
        featurings = self.spotify.get_album_featurings(album)
        if featurings:
            featurings = add_track_date(featurings, album['release_date'])
            write_album_to_csv(featurings, output_feat)
            write_linked_artists_to_csv(featurings, artist_urn, output_linked_artists + ".csv")

        # Label
        if album['label']:
            label = {
                'artist_urn': artist_urn,
                'label': album['label'],
                'date': album['release_date']
            }
            write_label_to_csv(label, output_label)

    def create_from_artist(self, output_artist, output_label, output_feat, output_genre, output_linked_artists,
                           artist_urn, min_album_date):

        # Genres
        artist = self.spotify.get_artist_info(self.spotify.get_artist_by_id(artist_urn))
        if artist['artist_genres']:
            genres = [
                {
                    'artist_urn': artist_urn,
                    'artist_name': artist['artist_name'],
                    'artist_popularity': artist['artist_popularity'],
                    'genre': genre
                } for genre in artist['artist_genres']
            ]
            write_genre_to_csv(genres, output_genre)

        # Artist
        artist_info = {
            'artist_urn': artist_urn,
            'artist_name': artist['artist_name'],
            'artist_popularity': artist['artist_popularity'],
            'nb_tracks_2015': 0,
            'nb_tracks_2016': 0,
            'nb_tracks_2017': 0,
            'nb_tracks_2018': 0,
            'nb_tracks_2019': 0,
            'nb_tracks_2020': 0,
        }

        # Albums info
        albums = self.spotify.get_artist_albums(artist_urn)
        for album in albums:
            album_release_dt = get_album_release_dt(album)
            if album_release_dt:
                if album_release_dt > datetime.strptime(min_album_date, '%Y-%m-%d'):
                    # Scrap featurings
                    self.create_from_album(output_label, output_feat, output_linked_artists, album, artist_urn)
                    # Count nb of tracks per year
                    for year in range(2015, 2021):
                        min_date = str(year) + '-' + '01-01'
                        max_date = str(year) + '-' + '12-31'
                        if datetime.strptime(min_date, '%Y-%m-%d') <= album_release_dt <= datetime.strptime(max_date, '%Y-%m-%d') and album['type'] in ['single', 'album']:
                            artist_info['nb_tracks_'+str(year)] += len(self.spotify.sp.album_tracks(album['id'])['items'])

        write_artist_to_csv([artist_info], output_artist)
        logger.info('Artist %s scraped.', artist['artist_name'])

    def expand_from_artist(
            self, output_artist, output_feat, output_label, output_genre, output_linked_artists, artist_urn, nb_hops,
            redo_from_hop=None, last_urn_scraped=None, min_album_date=None
    ):

        if not redo_from_hop:
            # Initiate with first artist scraping
            initialize_linked_artists_csv(nb_hops, output_linked_artists, artist_urn)
            logger.info("==========================================================================")
            logger.info("=                                  HOP #0                                =")
            logger.info("==========================================================================")
            self.create_from_artist(output_artist, output_label, output_feat, output_genre,
                                    output_linked_artists + "_1", artist_urn, min_album_date)

            # Hop on and scrape on
            for i in range(1, nb_hops):
                logger.info("==========================================================================")
                logger.info("=                                  HOP #%s                                =", str(i))
                logger.info("=                Scraping artists until linked_artists_%s                 =", str(i))
                logger.info("=              Writing next artists in linked_artists_%s                  =", str(i + 1))
                logger.info("==========================================================================")
                # Isolate artists already scraped
                scraped_artists = pd.read_csv(output_artist, index_col=False)['artist_urn'].drop_duplicates()
                scraped_artists.columns = ['urn']
                # Get artists to scrap (excluding artists already scraped)
                last_linked_artists = pd.read_csv(output_linked_artists + "_" + str(i) + ".csv").drop_duplicates()
                linked_artists = pd.concat(
                    [
                        last_linked_artists,
                        scraped_artists,
                        scraped_artists
                    ], ignore_index=True
                )['urn'].drop_duplicates(keep=False).reset_index()
                linked_artists = linked_artists['urn'].values.tolist()
                for urn in tqdm(linked_artists):
                    loop = True
                    while loop:
                        try:
                            self.create_from_artist(
                                output_artist,
                                output_label,
                                output_feat,
                                output_genre,
                                output_linked_artists + "_" + str(i + 1),
                                urn,
                                min_album_date
                            )
                            loop = False
                        except requests.exceptions.ReadTimeout:
                            pass
                post_treatment_scraping_history()  # Remove duplicates

        if redo_from_hop:
            post_treatment_scraping_history()
            # Hop on and scrape on
            for i in range(redo_from_hop, nb_hops):
                logger.info("==========================================================================")
                logger.info("=                                  HOP #%s                                =", str(i))
                logger.info("=                Scraping artists until linked_artists_%s                 =", str(i))
                logger.info("=              Writing next artists in linked_artists_%s                  =", str(i + 1))
                logger.info("==========================================================================")
                # Isolate artists already scraped
                scraped_artists = pd.read_csv(output_artist, index_col=False)['artist_urn'].drop_duplicates()
                scraped_artists.columns = ['urn']
                # Get artists to scrap (excluding artists already scraped)
                last_linked_artists = pd.read_csv(output_linked_artists + "_" + str(i) + ".csv").drop_duplicates()
                linked_artists = pd.concat(
                    [
                        last_linked_artists,
                        scraped_artists,
                        scraped_artists
                    ], ignore_index=True
                )['urn'].drop_duplicates(keep=False).reset_index()
                if last_urn_scraped:
                    idx_last_urn = linked_artists.index[linked_artists['urn'] == last_urn_scraped][0]
                    linked_artists = linked_artists[linked_artists.index > idx_last_urn]['urn']
                linked_artists = linked_artists.values.tolist()
                for urn in tqdm(linked_artists):
                    loop = True
                    while loop:
                        try:
                            self.create_from_artist(
                                output_artist,
                                output_label,
                                output_feat,
                                output_genre,
                                output_linked_artists + "_" + str(i + 1),
                                urn,
                                min_album_date
                            )
                            loop = False
                        except requests.exceptions.ReadTimeout:
                            pass

                post_treatment_scraping_history()  # Remove duplicates


if __name__ == "__main__":
    db = Scraping(
        spotify_client_id="",
        spotify_client_secret=""
    )

    # reset()

    # For start
    # db.expand_from_artist(
    #     output_artist="scraping_history/artists.csv",
    #     output_feat="scraping_history/feats.csv",
    #     output_label="scraping_history/labels.csv",
    #     output_genre="scraping_history/genres.csv",
    #     output_linked_artists="scraping_history/linked_artists",
    #     nb_hops=3,
    #     artist_urn="6qFt3TjvxMt77YGsktWG8Z",
    #     min_album_date='2015-01-01'
    # )

    # For retry
    # db.expand_from_artist(
    #     output_artist="scraping_history/artists.csv",
    #     output_feat="scraping_history/feats.csv",
    #     output_label="scraping_history/labels.csv",
    #     output_genre="scraping_history/genres.csv",
    #     output_linked_artists="scraping_history/linked_artists",
    #     nb_hops=3,
    #     artist_urn="6qFt3TjvxMt77YGsktWG8Z",
    #     redo_from_hop=3,
    #     last_urn_scraped="7qaqnM2Nc2VlUaT4ivx22o",
    #     min_album_date='2015-01-01'
    # )

    post_treatment_scraping_history()
