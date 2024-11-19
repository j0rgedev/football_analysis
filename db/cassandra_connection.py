from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

class CassandraConnection:
    def __init__(self, keyspace):
        self.keyspace = keyspace
        self.cluster = None
        self.session = None

    def connect(self):
        self.cluster = Cluster(['127.0.0.1'])
        self.session = self.cluster.connect()
        self.session.set_keyspace(self.keyspace)
        print(f"Conexión exitosa al keyspace: {self.keyspace}")

    def execute_query(self, query, values=None):
        if values:
            self.session.execute(query, values)
        else:
            self.session.execute(query)

    def execute_batch(self, query_template, batch):
        batch_statement = BatchStatement()
        prepared_statement = self.session.prepare(query_template)

        for values in batch:
            batch_statement.add(prepared_statement, values)

        self.session.execute(batch_statement)

    def close(self):
        self.cluster.shutdown()
        print("Conexión a Cassandra cerrada")