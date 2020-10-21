import pandas as pd


def remove_na(path):
    df = pd.read_csv(path)
    df = df.dropna()
    df.to_csv(path, index=False)


def drop_duplicates(path):
    df = pd.read_csv(path)
    df = df.drop_duplicates()
    df.to_csv(path, index=False)


def count_scraped_artists(paths):
    df = pd.read_csv(paths[0]).drop_duplicates()
    for path in paths:
        if path != paths[0]:
            df = df.append(pd.read_csv(path).drop_duplicates())
    df = df.drop_duplicates()
    length = len(df.index)
    return length


def post_treatment_scraping_history(
        output_feats='scraping_history/feats.csv',
        output_genres='scraping_history/genres.csv',
        output_labels='scraping_history/labels.csv',
        output_linked_artists='scraping_history/linked_artists'
):
    # Dedup
    drop_duplicates(output_feats)
    drop_duplicates(output_genres)
    drop_duplicates(output_labels)

    # Counts
    paths_optimist = [output_linked_artists + '_' + str(i) + '.csv' for i in range(4)]
    paths_pessimist = [output_linked_artists + '_' + str(i) + '.csv' for i in range(3)]
    count_optimist = count_scraped_artists(paths_optimist)
    count_pessimist = count_scraped_artists(paths_pessimist)
    print("Linked artists: between " + str(count_pessimist) + " and " + str(count_optimist)  + " artists.")
    print("Genres: " + str(count_scraped_artists([output_genres])) + " artists.")
    print("Labels: " + str(count_scraped_artists([output_labels])) + " artists.")

