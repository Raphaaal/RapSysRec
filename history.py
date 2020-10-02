import csv


def get_scraped_artists(scraped_artist_csv):
    with open(scraped_artist_csv, 'r', newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
        result = []
        for elt in data:
            result.append(elt[0])
        return result


def check_artist_is_scraped(scraped_artist_csv, artist_urn):
    with open(scraped_artist_csv, 'r', newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
    for item in data:
        if artist_urn in item:
            return True
    return False


def check_album_is_scraped(scraped_album_csv, album_urn):
    with open(scraped_album_csv, 'r', newline='') as f:
        reader = csv.reader(f)
        data = list(reader)
    if album_urn in data:
        return True
    return False


def write_scraped_artist(scraped_artist_csv, artist_urn):
    with open(scraped_artist_csv, 'a', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([artist_urn])


def write_scraped_album(scraped_albums_csv, album_urn):
    with open(scraped_albums_csv, 'a', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([album_urn])
