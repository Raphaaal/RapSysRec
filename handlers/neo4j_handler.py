from neo4j import GraphDatabase
import pandas as pd
import os


class Neo4JHandler:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

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

    def create_feats_all(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            result = self._create_feats_all_csv(session, path)
            return result

    def create_feats_year_2015(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            result = self._create_feats_year_2015_csv(session, path)
            return result

    def create_feats_year_2016(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            result = self._create_feats_year_2016_csv(session, path)
            return result

    def create_feats_year_2017(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            result = self._create_feats_year_2017_csv(session, path)
            return result

    def create_feats_year_2018(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            result = self._create_feats_year_2018_csv(session, path)
            return result

    def create_feats_year_2019(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            result = self._create_feats_year_2019_csv(session, path)
            return result

    def create_feats_year_2020(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            result = self._create_feats_year_2020_csv(session, path)
            return result

    def create_labels(self, csv_path):
        path = "file:///" + csv_path
        with self.driver.session() as session:
            result = self._create_labels_csv(session, path)
            return result

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

    def get_node_id_by_urn(self, urn):
        with self.driver.session() as session:
            artist_id = session.read_transaction(self._get_node_id_by_urn, urn)
            return artist_id

    def get_urn_by_id(self, node_id):
        with self.driver.session() as session:
            artist_urn = session.read_transaction(self._get_urn_by_id, node_id)
            return artist_urn[0]

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
    def _get_urn_by_id(tx, node_id):
        result = tx.run(
            "MATCH (a:Artist)  "
            "WHERE ID(a) = $id "
            "RETURN DISTINCT a.urn ",
            id=node_id
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
    def _create_feats_all_csv(session, csv_path):
        result = session.run(
            """
            USING PERIODIC COMMIT 1000
            
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
    def _create_feats_year_2015_csv(session, csv_path, year=2015):
        date_min = str(year) + "-01-01"
        date_max = str(year) + "-12-31"
        result = session.run(
            """
            USING PERIODIC COMMIT 100

            LOAD CSV WITH HEADERS FROM $csv_path AS row
            WITH distinct row

            MATCH (a: Artist {urn: row.artist_urn})-[r:FEAT {track_id: row.track_id}]->(b: Artist {urn: row.featuring_artist_urn})

            FOREACH (ift in CASE WHEN date(row.track_date) >= date($date_min) AND date(row.track_date) <= date($date_max) THEN [1] ELSE [] END |
                    MERGE (a)-[r:FEAT_2015 {track_id: row.track_id}]->(b)
                    ON CREATE SET r.track_name = row.track_name, r.track_date = row.track_date
            )
            """,
            {
                "csv_path": csv_path,
                "date_min": date_min,
                "date_max": date_max,
            }
        )
        return [row[0] for row in result]

    @staticmethod
    def _create_feats_year_2016_csv(session, csv_path, year=2016):
        date_min = str(year) + "-01-01"
        date_max = str(year) + "-12-31"
        result = session.run(
            """
            USING PERIODIC COMMIT 100

            LOAD CSV WITH HEADERS FROM $csv_path AS row
            WITH distinct row

            MATCH (a: Artist {urn: row.artist_urn})-[r:FEAT {track_id: row.track_id}]->(b: Artist {urn: row.featuring_artist_urn})

            FOREACH (ift in CASE WHEN date(row.track_date) >= date($date_min) AND date(row.track_date) <= date($date_max) THEN [1] ELSE [] END |
                    MERGE (a)-[r:FEAT_2016 {track_id: row.track_id}]->(b)
                    ON CREATE SET r.track_name = row.track_name, r.track_date = row.track_date
            )
            """,
            {
                "csv_path": csv_path,
                "date_min": date_min,
                "date_max": date_max,
            }
        )
        return [row[0] for row in result]

    @staticmethod
    def _create_feats_year_2017_csv(session, csv_path, year=2017):
        date_min = str(year) + "-01-01"
        date_max = str(year) + "-12-31"
        result = session.run(
            """
            USING PERIODIC COMMIT 100

            LOAD CSV WITH HEADERS FROM $csv_path AS row
            WITH distinct row

            MATCH (a: Artist {urn: row.artist_urn})-[r:FEAT {track_id: row.track_id}]->(b: Artist {urn: row.featuring_artist_urn})

            FOREACH (ift in CASE WHEN date(row.track_date) >= date($date_min) AND date(row.track_date) <= date($date_max) THEN [1] ELSE [] END |
                    MERGE (a)-[r:FEAT_2017 {track_id: row.track_id}]->(b)
                    ON CREATE SET r.track_name = row.track_name, r.track_date = row.track_date
            )
            """,
            {
                "csv_path": csv_path,
                "date_min": date_min,
                "date_max": date_max,
            }
        )
        return [row[0] for row in result]

    @staticmethod
    def _create_feats_year_2018_csv(session, csv_path, year=2018):
        date_min = str(year) + "-01-01"
        date_max = str(year) + "-12-31"
        result = session.run(
            """
            USING PERIODIC COMMIT 100

            LOAD CSV WITH HEADERS FROM $csv_path AS row
            WITH distinct row

            MATCH (a: Artist {urn: row.artist_urn})-[r:FEAT {track_id: row.track_id}]->(b: Artist {urn: row.featuring_artist_urn})

            FOREACH (ift in CASE WHEN date(row.track_date) >= date($date_min) AND date(row.track_date) <= date($date_max) THEN [1] ELSE [] END |
                    MERGE (a)-[r:FEAT_2018 {track_id: row.track_id}]->(b)
                    ON CREATE SET r.track_name = row.track_name, r.track_date = row.track_date
            )
            """,
            {
                "csv_path": csv_path,
                "date_min": date_min,
                "date_max": date_max,
            }
        )
        return [row[0] for row in result]

    @staticmethod
    def _create_feats_year_2019_csv(session, csv_path, year=2019):
        date_min = str(year) + "-01-01"
        date_max = str(year) + "-12-31"
        result = session.run(
            """
            USING PERIODIC COMMIT 100

            LOAD CSV WITH HEADERS FROM $csv_path AS row
            WITH distinct row

            MATCH (a: Artist {urn: row.artist_urn})-[r:FEAT {track_id: row.track_id}]->(b: Artist {urn: row.featuring_artist_urn})

            FOREACH (ift in CASE WHEN date(row.track_date) >= date($date_min) AND date(row.track_date) <= date($date_max) THEN [1] ELSE [] END |
                    MERGE (a)-[r:FEAT_2019 {track_id: row.track_id}]->(b)
                    ON CREATE SET r.track_name = row.track_name, r.track_date = row.track_date
            )
            """,
            {
                "csv_path": csv_path,
                "date_min": date_min,
                "date_max": date_max,
            }
        )
        return [row[0] for row in result]

    @staticmethod
    def _create_feats_year_2020_csv(session, csv_path, year=2020):
        date_min = str(year) + "-01-01"
        date_max = str(year) + "-12-31"
        result = session.run(
            """
            USING PERIODIC COMMIT 100

            LOAD CSV WITH HEADERS FROM $csv_path AS row
            WITH distinct row

            MATCH (a: Artist {urn: row.artist_urn})-[r:FEAT {track_id: row.track_id}]->(b: Artist {urn: row.featuring_artist_urn})

            FOREACH (ift in CASE WHEN date(row.track_date) >= date($date_min) AND date(row.track_date) <= date($date_max) THEN [1] ELSE [] END |
                    MERGE (a)-[r:FEAT_2020 {track_id: row.track_id}]->(b)
                    ON CREATE SET r.track_name = row.track_name, r.track_date = row.track_date
            )
            """,
            {
                "csv_path": csv_path,
                "date_min": date_min,
                "date_max": date_max,
            }
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

            MERGE (a)-[r:LABEL {date: row.date}]->(l)
            """,
            {
                'csv_path': csv_path
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
