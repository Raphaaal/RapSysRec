import itertools
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import logging
from pprint import pprint


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('spotify_loader')
# logger.propagate = False


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


def get_track_featurings(track):
    track_featurings = []
    artists_list = track['artists']
    if len(artists_list) > 1:
        for artists_comb in itertools.combinations(artists_list, 2):
            featuring = {
                "artist_urn": artists_comb[0]['id'],
                "artist_name": artists_comb[0]['name'],
                "track_name": track['name'],
                "track_id": track['id'],
                "featuring_artist_urn": artists_comb[1]['id'],
                "featuring_artist_name": artists_comb[1]['name']
            }
            track_featurings.append(featuring)
        return track_featurings


class SpotifyLoader:

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        client_credentials_manager = SpotifyClientCredentials(
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager, requests_timeout=20)

    def get_artist_by_name(self, name):
        results = self.sp.search(q='artist:' + name, type='artist')
        items = results['artists']['items']
        if len(items) > 0:
            return items[0]
        else:
            return None

    def get_artist_by_id(self, urn):
        result = self.sp.artist(urn)
        if result:
            return result

    @staticmethod
    def get_artist_info(artist):
        artist_info = {
            "artist_id": artist['id'],
            "artist_name": artist['name'],
            "artist_popularity": artist['popularity'],
            "artist_genres": artist['genres']
        }
        return artist_info

    def get_album_featurings(self, album):
        tracks = []
        results = self.sp.album_tracks(album['id'])
        tracks.extend(results['items'])
        album_featurings = []
        while results['next']:
            results = self.sp.next(results)
            tracks.extend(results['items'])

        for i, track in enumerate(tracks):
            track_featurings = get_track_featurings(track)
            if track_featurings:
                for ft in track_featurings:
                    album_featurings.append(ft)
        if album_featurings:
            return album_featurings

    def get_nb_tracks_year(self, min_date, max_date, urn):
        albums = self.get_artist_albums(urn)
        tracks = []
        for album in albums:
            album_release_dt = get_album_release_dt(album)
            if album_release_dt:
                if datetime.strptime(min_date, '%Y-%m-%d') <= album_release_dt <= datetime.strptime(max_date, '%Y-%m-%d'):
                    tracks.extend(self.sp.album_tracks(album['id']))
        return len(tracks)

    def get_artist_albums(self, artist_urn):
        albums = []
        albums_list = []
        album_types = ['album', 'single', 'appears_on']

        def get_artist_albums_by_type(alb_type):
            results = self.sp.artist_albums(artist_urn, album_type=alb_type)
            albums_list.extend(results['items'])
            while results['next']:
                results = self.sp.next(results)
                albums_list.extend(results['items'])

            def build_album_info(final_list, album):
                album_info = {
                    'release_date': album['release_date'],
                    'id': album['id'],
                    'name': album['name'],
                    'label': self.sp.album(album['id'])['label']
                }
                if album_info:
                    final_list.append(album_info)
                return album_info

            for album in albums_list:
                build_album_info(albums, album)

        for album_type in album_types:
            get_artist_albums_by_type(album_type)

        return albums

    def main_test(self, artist):
        artist = self.get_artist_by_name(artist)
        self.get_artist_info(artist)
        feat_info = self.get_artist_ft_tracks(artist)
        pprint(feat_info)


if __name__ == '__main__':
    spotify = SpotifyLoader(
        client_id="28d60111ea634effb71f87304bed9285",
        client_secret="77f974dfa7c2412196a9e1b13e4f5e9e"
    )
    artist = {'id': "2eh8cEKZk4VeruUrGq748D" }
    test = spotify.get_artist_albums(artist)
    pprint(test)
