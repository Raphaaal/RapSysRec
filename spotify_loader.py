import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import logging
from pprint import pprint
from history import check_album_is_scraped, write_scraped_album

logger = logging.getLogger('spotify_loader')
# logger.propagate = False
logging.basicConfig(level='INFO')


class SpotifyLoader:

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        client_credentials_manager = SpotifyClientCredentials(
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

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

    def get_track_ft_info(self, track_id, album_date):
        track = self.sp.track(track_id)
        artists_list = track['artists']
        if len(artists_list) > 1:
            logger.info('Scraping track %s (%s)', track['name'], track['id'])
            artists_objects_lists = []
            for artist in artists_list:
                artists_objects_lists.append(
                    {
                        "artist_id": artist['id'],
                        "artist_name": artist['name']
                    }
                )
            track_feat_info = {
                "track_name": track['name'],
                "track_id": track['id'],
                "track_date": album_date,
                "featuring_artists": artists_objects_lists

            }
            return track_feat_info

    def get_album_ft_tracks(self, album):
        tracks = []
        results = self.sp.album_tracks(album['id'])
        tracks.extend(results['items'])
        album_feat_info = []
        while results['next']:
            results = self.sp.next(results)
            tracks.extend(results['items'])
        for i, track in enumerate(tracks):
            track_ft_info = self.get_track_ft_info(track_id=track['id'], album_date=album['release_date'])
            if track_ft_info:
                album_feat_info.append(track_ft_info)
        if len(album_feat_info) > 0:
            return album_feat_info
    
    def get_artist_albums(self, artist):
        albums = []
        results = self.sp.artist_albums(artist['id'])
        albums.extend(results['items'])
        while results['next']:
            results = self.sp.next(results)
            albums.extend(results['items'])
        # Skip duplicate albums
        unique = set()
        albums_info = []
        for album in albums:
            album_urn = album['id']
            album_label = self.sp.album(album_urn)['label']
            album['label'] = album_label
            logger.info("Scraping album %s from artist %s.", album_urn, artist['id'])
            name = album['name'].lower()
            if name not in unique:
                unique.add(name)
                albums_info.append(album)
        return albums_info

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
    spotify.main_test('Booba')
