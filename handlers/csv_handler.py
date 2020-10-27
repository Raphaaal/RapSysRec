import csv
import io


def truncate_file(file_path):
    f = open(file_path, "w+")
    f.close()


def get_scraped_artists(scraped_artist_csv_list):
    results = []
    for csv_file in scraped_artist_csv_list:
        with open(csv_file, 'r', newline='') as f:
            reader = csv.reader(f)
            data = list(reader)
            for elt in data:
                results.append(elt[0])
    return results


def check_artist_is_scraped(scraped_artist_csv, artist_urn):
    if not type(scraped_artist_csv) is list:
        with open(scraped_artist_csv, 'r', newline='') as f:
            reader = csv.reader(f)
            data = list(reader)
        for item in data:
            if artist_urn in item:
                return True
        return False
    else:
        for path in scraped_artist_csv:
            if check_artist_is_scraped(path, artist_urn):
                return True


def check_album_is_scraped(scraped_album_csv, album_urn):
    if not type(scraped_album_csv) is list:
        with open(scraped_album_csv, 'r', newline='') as f:
            reader = csv.reader(f)
            data = list(reader)
        if album_urn in data:
            return True
        return False
    else:
        for path in scraped_album_csv:
            if check_album_is_scraped(path, album_urn):
                return True


def write_scraped_artist(scraped_artist_csv, artist_urn):
    with open(scraped_artist_csv, 'a', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([artist_urn])


def write_scraped_album(scraped_albums_csv, album_urn):
    with open(scraped_albums_csv, 'a', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([album_urn])


def write_list_to_csv(pairs, csv_path, mode='a'):
    with io.open(csv_path, mode, newline='', encoding='utf-8') as f:
        csv_writer = csv.writer(f)
        for pair in pairs:
            csv_writer.writerow(pair)
