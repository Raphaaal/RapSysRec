import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import logging
from pprint import pprint

logger = logging.getLogger('examples.artist_discography')
logging.basicConfig(level='INFO')


def get_artist_by_name(name):
    results = sp.search(q='artist:' + name, type='artist')
    items = results['artists']['items']
    if len(items) > 0:
        return items[0]
    else:
        return None


def get_artist_by_id(urn):
    result = sp.artist(urn)
    if result:
        return result


def get_artist_info(artist):
    logger.info('====%s====', artist['name'])
    logger.info('Popularity: %s', artist['popularity'])
    if len(artist['genres']) > 0:
        logger.info('Genres: %s', ','.join(artist['genres']))
    artist_info = {
        "artist_id": artist['id'],
        "artist_name":artist['name'],
        "artist_popularity": artist['popularity'],
        "artist_genres": artist['genres']
    }
    return artist_info


def get_track_ft_info(track_id):
    track = sp.track(track_id)
    artists_list = track['artists']
    if len(artists_list) > 1:
        logger.info('%s. - id: %s', track['name'], track['id'])
        artists_objects_lists = []
        for artist in artists_list:
            # logger.info('Featuring artist: %s', artist['id'])
            artists_objects_lists.append(
                {
                    "artist_id": artist['id'],
                    "artist_name": artist['name']
                }
            )
        track_feat_info = {
            "track-name": track['name'],
            "track_id": track['id'],
            "featuring_artists": artists_objects_lists

        }
        return track_feat_info


def get_album_ft_tracks(album):
    tracks = []
    results = sp.album_tracks(album['id'])
    tracks.extend(results['items'])
    album_feat_info = []
    while results['next']:
        results = sp.next(results)
        tracks.extend(results['items'])
    for i, track in enumerate(tracks):
        track_ft_info = get_track_ft_info(track['id'])
        if track_ft_info :
            album_feat_info.append(track_ft_info)
    if len(album_feat_info) > 0:
        return album_feat_info


def get_artist_ft_tracks(artist):
    albums = []
    results = sp.artist_albums(artist['id'], album_type='album')
    albums.extend(results['items'])
    artist_ft_info = []
    while results['next']:
        results = sp.next(results)
        albums.extend(results['items'])
    # logger.info('Total albums: %s', len(albums))
    unique = set()  # skip duplicate albums
    for album in albums:
        name = album['name'].lower()
        if name not in unique:
            # logger.info('ALBUM: %s', name)
            unique.add(name)
            artist_ft_info.append(get_album_ft_tracks(album))
    if len(artist_ft_info) > 0:
        return artist_ft_info


def main(artist):
    artist = get_artist_by_name(artist)
    get_artist_info(artist)
    feat_info = get_artist_ft_tracks(artist)
    pprint(feat_info)


if __name__ == '__main__':
    client_credentials_manager = SpotifyClientCredentials(
        client_id="28d60111ea634effb71f87304bed9285",
        client_secret="77f974dfa7c2412196a9e1b13e4f5e9e"
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    main('Booba')

