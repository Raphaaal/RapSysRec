from neo4j import GraphDatabase


class Neo4JHandler:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_print_artist(self, name, urn, popularity):
        with self.driver.session() as session:
            artist = session.write_transaction(self._create_artist, name, urn, popularity)
            print(artist)

    def create_print_feat(self, urn1, urn2, track_id, track_name):
        with self.driver.session() as session:
            feat = session.write_transaction(self._create_feat, urn1, urn2, track_id, track_name)
            print(feat)

    def get_print_artist(self, urn):
        with self.driver.session() as session:
            artist = session.write_transaction(self._get_artist, urn)
            print(artist)

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
    def _create_feat(tx, urn1, urn2, track_id, track_name):
        result = tx.run(
            "MATCH (a:Artist), (b:Artist) "
            "WHERE a.urn = $urn1 AND b.urn = $urn2 "
            "CREATE (a) -[r:FEAT {track_name: $track_name, track_id: $track_id}]-> (b) "
            "RETURN a.name + ' feat ' + b.name + ' in ' + r.track_name + ' created.'",
            urn1=urn1,
            urn2=urn2,
            track_id=track_id,
            track_name=track_name
        )
        return [row[0] for row in result]


if __name__ == "__main__":
    graph = Neo4JHandler("bolt://localhost:7687", "neo4j", "root")
    graph.create_print_artist("La Fouine", 1, 37)
    graph.create_print_artist("Booba", 2, 86)
    graph.create_print_feat(1, 2, "abcd", "Reste en chien")
    graph.close()
