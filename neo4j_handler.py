from neo4j import GraphDatabase


class Neo4JHandler:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_artist(self, name, urn, popularity):
        with self.driver.session() as session:
            artist = session.write_transaction(self._create_artist, name, urn, popularity)
            return artist

    def create_feat(self, urn1, urn2, track_id, track_name, track_date):
        with self.driver.session() as session:
            feat = session.write_transaction(self._create_feat, urn1, urn2, track_id, track_name, track_date)
            return feat

    def merge_genre(self, genre):
        with self.driver.session() as session:
            genre = session.write_transaction(self._merge_genre, genre)
            return genre

    def merge_label(self, label):
        with self.driver.session() as session:
            label = session.write_transaction(self._merge_label, label)
            return label

    def set_genre_artist(self, genre_name, artist_urn):
        with self.driver.session() as session:
            artist_genre = session.write_transaction(self._set_genre_artist, genre_name, artist_urn)
            return artist_genre

    def set_label_artist(self, label_name, artist_urn, album_date):
        with self.driver.session() as session:
            artist_label = session.write_transaction(self._set_label_artist, label_name, artist_urn, album_date)
            return artist_label

    def get_genre_artist(self, genre_name, artist_urn):
        with self.driver.session() as session:
            artist_genre = session.write_transaction(self._get_genre_artist, genre_name, artist_urn)
            return artist_genre

    def get_label_artist(self, label_name, artist_urn, album_date):
        with self.driver.session() as session:
            artist_label = session.write_transaction(self._get_label_artist, label_name, artist_urn, album_date)
            return artist_label

    def get_artist(self, urn):
        with self.driver.session() as session:
            artist = session.write_transaction(self._get_artist, urn)
            return artist

    def get_feat(self, track_id, track_name, artist1_urn, artist2_urn):
        with self.driver.session() as session:
            feat = session.write_transaction(self._get_feat, track_id, track_name, artist1_urn, artist2_urn)
            return feat

    def get_unscraped_artist(self, scraped_artists_urn_list, genre_names_list):
        with self.driver.session() as session:
            artist = session.write_transaction(self._get_unscraped_artist, scraped_artists_urn_list, genre_names_list)
            return artist

    def get_linked_artists(self, artist_urn):
        with self.driver.session() as session:
            artists = session.write_transaction(self._get_linked_artists, artist_urn)
            return artists

    def truncate(self):
        with self.driver.session() as session:
            session.write_transaction(self._truncate)

    @staticmethod
    def _create_artist(tx, name, urn, popularity):
        result = tx.run(
            "CREATE (a:Artist) "
            "SET a.name = $name "
            "SET a.urn = $urn "
            "SET a.popularity = $popularity "
            "RETURN a.name + ' created.'",
            name=name,
            urn=urn,
            popularity=popularity
        )
        return [row[0] for row in result]

    @staticmethod
    def _get_artist(tx, urn):
        result = tx.run(
            "MATCH (a:Artist) "
            "WHERE a.urn = $urn "
            "RETURN a.name, a.urn",
            urn=urn,
        )
        return [row[0] for row in result]

    @staticmethod
    def _get_feat(tx, track_id, track_name, artist1_urn, artist2_urn):
        result = tx.run(
            "MATCH (a:Artist)-[r:FEAT]->(b:Artist) "
            "WHERE (r.track_id = $track_id) OR "
            "(a.urn = $artist1_urn AND b.urn = $artist2_urn AND r.track_name = $track_name) "
            "RETURN 'Feat ' + r.track_name + ' (' + r.track_id + ')' + ' found.'",
            track_id=track_id,
            track_name=track_name,
            artist1_urn=artist1_urn,
            artist2_urn=artist2_urn
        )
        return [row[0] for row in result]

    @staticmethod
    def _create_feat(tx, urn1, urn2, track_id, track_name, track_date):
        result = tx.run(
            "MATCH (a:Artist), (b:Artist) "
            "WHERE a.urn = $urn1 AND b.urn = $urn2 "
            "CREATE (a) -[r:FEAT {track_name: $track_name, track_id: $track_id, track_date: $track_date}]-> (b) "
            "RETURN a.name + ' feat ' + b.name + ' in ' + r.track_name + ' created.'",
            urn1=urn1,
            urn2=urn2,
            track_id=track_id,
            track_name=track_name,
            track_date=track_date
        )
        return [row[0] for row in result]

    @staticmethod
    def _merge_genre(tx, genre):
        result = tx.run(
            "MERGE (g:Genre {name: $genre}) "
            "RETURN g.name + ' merged.'",
            genre=genre
        )
        return [row[0] for row in result]

    @staticmethod
    def _merge_label(tx, label):
        result = tx.run(
            "MERGE (l:Label {name: $label}) "
            "RETURN l.name + ' merged.'",
            label=label
        )
        return [row[0] for row in result]

    @staticmethod
    def _set_genre_artist(tx, genre_name, artist_urn):
        result = tx.run(
            "MATCH (g:Genre), (a:Artist) "
            "WHERE a.urn = $artist_urn AND g.name = $genre_name "
            "CREATE (a) -[r:GENRE]-> (g) "
            "RETURN a.name + ' linked to genre ' + g.name ",
            artist_urn=artist_urn,
            genre_name=genre_name
        )
        return [row[0] for row in result]

    @staticmethod
    def _get_genre_artist(tx, genre_name, artist_urn):
        result = tx.run(
            "MATCH (g:Genre) <-[:GENRE]- (a:Artist) "
            "WHERE a.urn = $artist_urn AND g.name = $genre_name "
            "RETURN g.name, a.name",
            artist_urn=artist_urn,
            genre_name=genre_name
        )
        return [row[0] for row in result]

    @staticmethod
    def _set_label_artist(tx, label_name, artist_urn, album_date):
        result = tx.run(
            "MATCH (l:Label), (a:Artist) "
            "WHERE a.urn = $artist_urn AND l.name = $label_name "
            "CREATE (a) -[r:LABEL {date: $album_date}]-> (l) "
            "RETURN a.name + ' linked to label ' + l.name ",
            artist_urn=artist_urn,
            label_name=label_name,
            album_date=album_date
        )
        return [row[0] for row in result]

    @staticmethod
    def _get_label_artist(tx, label_name, artist_urn, album_date):
        result = tx.run(
            "MATCH (l:Label) -[r:LABEL {date: $album_date}]- (a:Artist) "
            "WHERE a.urn = $artist_urn AND l.name = $label_name "
            "RETURN l.name, a.name, r.album_date ",
            artist_urn=artist_urn,
            label_name=label_name,
            album_date=album_date
        )
        return [row[0] for row in result]

    @staticmethod
    def _get_unscraped_artist(tx, scraped_artists_urn_list, genre_names_list):
        result = tx.run(
            "MATCH (a:Artist) -[r:GENRE]- (g:Genre) "
            "WHERE NOT a.urn IN $scraped_artists_urn_list AND g.name IN $genre_names_list "
            "RETURN a.urn LIMIT 1 ",
            scraped_artists_urn_list=scraped_artists_urn_list,
            genre_names_list=genre_names_list
        )
        return [row[0] for row in result]

    @staticmethod
    def _get_linked_artists(tx, artist_urn):
        result = tx.run(
            "MATCH (a:Artist {urn: $artist_urn}) -[r:FEAT]- (a2:Artist) "
            "RETURN DISTINCT a2.urn ",
            artist_urn=artist_urn,
        )
        return [row[0] for row in result]

    @staticmethod
    def _truncate(tx):
        result = tx.run(
            "MATCH (n)  "
            "DETACH DELETE n "
        )
        return [row[0] for row in result]


if __name__ == "__main__":
    graph = Neo4JHandler("bolt://localhost:7687", "neo4j", "root")
    graph.create_artist("La Fouine", 1, 37)
    graph.create_artist("Booba", 2, 86)
    graph.get_artist(1)
    graph.create_feat(1, 2, "abcd", "Reste en chien", "01/01/2020")
    graph.get_feat("abcd", "Reste en chen", 1, 2)
    graph.merge_genre(genre="Rap")
    graph.set_genre_artist(artist_urn=1, genre_name="Rap")
    graph.merge_label(label="92i")
    graph.set_label_artist(label_name="92i", artist_urn=2, album_date="01/01/2020")
    graph.get_label_artist(label_name="92i", artist_urn=2, album_date="01/01/2020")
    print(graph.get_linked_artists(1))
    graph.close()
