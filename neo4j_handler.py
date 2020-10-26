from neo4j import GraphDatabase
import pandas as pd
import os


class Neo4JHandler:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def merge_artist(self, name, urn, popularity):
        with self.driver.session() as session:
            artist = session.write_transaction(self._merge_artist, name, urn, popularity)
            return artist

    def create_feat(self, urn1, urn2, track_id, track_name, track_date):
        with self.driver.session() as session:
            feat = session.write_transaction(self._create_feat, urn1, urn2, track_id, track_name, track_date)
            return feat

    def create_multiple_feats(self, feat_info_list):
        with self.driver.session() as session:
            feat = session.write_transaction(self._create_multiple_feats, feat_info_list)
            return feat

    def merge_genre(self, genre):
        with self.driver.session() as session:
            genre = session.write_transaction(self._merge_genre, genre)
            return genre

    def merge_label(self, label, artist):
        with self.driver.session() as session:
            label = session.write_transaction(self._merge_label, label, artist)
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
            artist_genre = session.read_transaction(self._get_genre_artist, genre_name, artist_urn)
            return artist_genre

    def get_node_id_by_urn(self, urn):
        with self.driver.session() as session:
            artist_id = session.read_transaction(self._get_node_id_by_urn, urn)
            return artist_id

    def get_label_artist(self, label_name, artist_urn, album_date):
        with self.driver.session() as session:
            artist_label = session.read_transaction(self._get_label_artist, label_name, artist_urn, album_date)
            return artist_label

    def get_artist(self, urn):
        with self.driver.session() as session:
            artist = session.read_transaction(self._get_artist, urn)
            return artist

    def get_feat(self, track_id, track_name, artist1_urn, artist2_urn):
        with self.driver.session() as session:
            feat = session.read_transaction(self._get_feat, track_id, track_name, artist1_urn, artist2_urn)
            return feat

    def get_unscraped_linked_artists(self, artist_urn, scraped_artists_urn_list):
        with self.driver.session() as session:
            artists = session.read_transaction(self._get_linked_artists, artist_urn, scraped_artists_urn_list)
            return artists

    def truncate(self):
        with self.driver.session() as session:
            session.write_transaction(self._truncate)

    def delete_duplicate_artists(self):
        with self.driver.session() as session:
            session.write_transaction(self._delete_duplicate)

    def delete_nodes_without_label(self):
        with self.driver.session() as session:
            session.write_transaction(self._delete_nodes_without_label)

    def convert_pop_to_int(self):
        with self.driver.session() as session:
            session.write_transaction(self._convert_pop_to_int)

    def set_constraints(self):
        with self.driver.session() as session:
            session.write_transaction(self._set_constraints)

    def get_artists_pdf_from_ids(self, artists_id_list):
        query = """
        MATCH (a: Artist)
        WHERE ID(a) IN $artists_id_list
        RETURN CASE 
            WHEN a.name IS NULL THEN 'null'
            WHEN a.name IS NOT NULL THEN a.name
            END
        """
        params = {"artists_id_list": artists_id_list}
        with self.driver.session() as session:
            result = session.run(query, params)
            artists_names = pd.DataFrame([dict(record) for record in result])
        return artists_names

    def create_genres(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            # result = session.write_transaction(self._create_genres_csv, path)
            result = self._create_genres_csv(session, path)
            return result

    def create_artists(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            # result = session.write_transaction(self._create_genres_csv, path)
            result = self._create_artists_csv(session, path)
            return result

    def create_feats(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            result = self._create_feats_csv(session, path)
            return result

    def create_labels(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            result = self._create_labels_csv(session, path)
            return result

    @staticmethod
    def _merge_artist(tx, name, urn, popularity):
        result = tx.run(
            "MERGE (a:Artist {name: $name, urn: $urn, popularity: $popularity}) "
            "RETURN a.name + ' merged.'",
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
        return [row[0] for row in result]\

    @staticmethod
    def _create_multiple_feats(tx, feat_info_list):
        result = tx.run(
            """
            UNWIND $feat_info_list as feat 
            UNWIND feat.featuring_artists as ft_artist
            MERGE (a:Artist  {urn:ft_artist.artist_id})
            ON CREATE SET a.name = ft_artist.artist_name
            """,
            feat_info_list=feat_info_list
        )

        result = tx.run(
            """
            UNWIND $feat_info_list as feat WITH apoc.coll.combinations(feat.featuring_artists, 2) as combinations, feat
            UNWIND combinations as comb
            MATCH (a:Artist {name: comb[0].artist_name, urn:comb[0].artist_id})
            MATCH (b:Artist {name: comb[1].artist_name, urn:comb[1].artist_id})
            MERGE (a)-[:FEAT {track_name: feat.track_name, track_id: feat.track_id, track_date: feat.track_date}]->(b)
            RETURN 'Feat created.'
            """,
            feat_info_list=feat_info_list
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
    def _merge_label(tx, label, artist):
        result = tx.run(
            "MERGE (l:Label {name: $label}) "
            "RETURN l.name + ' merged.'",
            label=label,
        )
        result = tx.run(
            "MATCH (l:Label {name: $label}) "
            "MATCH (a:Artist {urn: $artist})"
            "MERGE (a)-[:LABEL]->(l)",
            label=label,
            artist=artist
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
    def _get_node_id_by_urn(tx, urn):
        result = tx.run(
            "MATCH (a:Artist)  "
            "WHERE a.urn = $urn "
            "RETURN DISTINCT id(a) ",
            urn=urn
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
    def _get_linked_artists(tx, artist_urn, scraped_artists_urn_list):
        result = tx.run(
            "MATCH (a:Artist {urn: $artist_urn}) -[r:FEAT]- (a2:Artist) "
            "WHERE NOT a2.urn IN $scraped_artists_urn_list "
            "RETURN DISTINCT a2.urn ",
            artist_urn=artist_urn,
            scraped_artists_urn_list=scraped_artists_urn_list
        )
        return [row[0] for row in result]

    @staticmethod
    def _truncate(tx):
        result = tx.run(
            "MATCH (n)  "
            "DETACH DELETE n "
        )
        return [row[0] for row in result]

    @staticmethod
    def _delete_duplicate(tx):
        result = tx.run(
            "MATCH(a: Artist) "
            "WITH a.urn AS urn, COLLECT(a) AS artists "
            "WHERE SIZE(artists) > 1 "
            "UNWIND artists[1..] AS artist "
            "DETACH DELETE artist "
        )
        return [row[0] for row in result]

    @staticmethod
    def _delete_nodes_without_label(tx):
        result = tx.run(
            "MATCH (n) WHERE size(labels(n)) = 0 "
            "DETACH DELETE n "
        )
        return [row[0] for row in result]

    @staticmethod
    def _convert_pop_to_int(tx):
        result = tx.run(
            "MATCH (n: Artist) WHERE n.popularity IS NOT NULL "
            "SET n.popularity = toInteger(n.popularity) "
        )
        return [row[0] for row in result]

    @staticmethod
    def _set_constraints(tx):
        result = tx.run(
            "CREATE CONSTRAINT ON (a:Artist) ASSERT a.urn IS UNIQUE "
        )
        result = tx.run(
            "CREATE CONSTRAINT ON (g:Genre) ASSERT g.name IS UNIQUE "
        )
        result = tx.run(
            "CREATE CONSTRAINT ON (l:Label) ASSERT l.name IS UNIQUE "
        )
        return [row[0] for row in result]

    @staticmethod
    # def _create_genres_csv(tx, csv_path):
    # Need open transactions for USING PERIODIC COMMIT to work
    def _create_genres_csv(session, csv_path):
        # result = tx.run(
        result = session.run(
            """
            USING PERIODIC COMMIT 100
            
            LOAD CSV WITH HEADERS FROM $csv_path AS row
            WITH distinct row
            
            MERGE (a: Artist {urn: row.artist_urn})
            ON CREATE SET a.name = row.artist_name, a.popularity = row.artist_popularity
            ON MATCH SET a.name = row.artist_name, a.popularity = row.artist_popularity
            
            MERGE (g: Genre {name: row.genre})
            
            MERGE (a)-[:GENRE]->(g)
            """,
            csv_path=csv_path
        )
        return [row[0] for row in result]

    @staticmethod
    # Need open transactions for USING PERIODIC COMMIT to work
    def _create_artists_csv(session, csv_path):
        # result = tx.run(
        result = session.run(
            """
            USING PERIODIC COMMIT 100

            LOAD CSV WITH HEADERS FROM $csv_path AS row
            WITH distinct row

            MERGE (a: Artist {urn: row.artist_urn})
            ON CREATE SET a.name = row.artist_name, a.popularity = row.artist_popularity
            ON MATCH SET a.name = row.artist_name, a.popularity = row.artist_popularity
            """,
            csv_path=csv_path
        )
        return [row[0] for row in result]

    @staticmethod
    # def _create_feats_csv(tx, csv_path):
    # Need open transactions for USING PERIODIC COMMIT to work
    def _create_feats_csv(session, csv_path):
        result = session.run(
            """
            USING PERIODIC COMMIT 100
            
            LOAD CSV WITH HEADERS FROM $csv_path AS row
            WITH distinct row
            
            MERGE (a: Artist {urn: row.artist_urn})
            ON CREATE SET a.name = row.artist_name
            
            MERGE (b: Artist {urn: row.featuring_artist_urn})
            ON CREATE SET b.name = row.featuring_artist_name

            MERGE (a)-[r:FEAT {track_id: row.track_id}]->(b)
            ON CREATE SET r.track_name = row.track_name, r.track_date = row.track_date
            """,
            csv_path=csv_path
        )
        return [row[0] for row in result]

    @staticmethod
    # def _create_labels_csv(tx, csv_path):
    # Need open transactions for USING PERIODIC COMMIT to work
    def _create_labels_csv(session, csv_path):
        # result = tx.run(
        result = session.run(
            """
            USING PERIODIC COMMIT 100
            
            LOAD CSV WITH HEADERS FROM $csv_path AS row
            WITH distinct row

            MATCH (a: Artist {urn: row.artist_urn})

            MERGE (l: Label {name: row.label})

            MERGE (a)-[r:LABEL {date: $date}]->(l)
            """,
            {
                'csv_path': csv_path,
                'date': date
            }
        )
        return [row[0] for row in result]


if __name__ == "__main__":
    graph = Neo4JHandler("bolt://localhost:7687", "neo4j", "root")
    # graph.create_artist("La Fouine", 1, 37)
    # graph.create_artist("Booba", 2, 86)
    # graph.get_artist(1)
    # graph.create_feat(1, 2, "abcd", "Reste en chien", "01/01/2020")
    # graph.get_feat("abcd", "Reste en chen", 1, 2)
    # graph.merge_genre(genre="Rap")
    # graph.set_genre_artist(artist_urn=1, genre_name="Rap")
    # graph.merge_label(label="92i")
    # graph.set_label_artist(label_name="92i", artist_urn=2, album_date="01/01/2020")
    # graph.get_label_artist(label_name="92i", artist_urn=2, album_date="01/01/2020")
    # print(graph.get_linked_artists(1))
    print(graph.get_artists_pdf_from_ids(artists_id_list=[237, 111]))
    graph.close()
