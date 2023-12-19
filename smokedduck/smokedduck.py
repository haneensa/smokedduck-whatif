import duckdb
import json
import lineage_query
import provenance_models
from operators import OperatorFactory
import sql_statements


class SmokedDuck:

    def __init__(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        self.duckdb_conn = duckdb_conn
        self.duckdb_conn.execute("pragma threads=1")
        self.operator_factory = OperatorFactory()

        self.query_id = -1
        self.query_plan = None
        self.captured_lineage_model = None
        self.latest_relation = None
        self._conform_to_duckdb_interface()

    def _conform_to_duckdb_interface(self) -> None:
        smokedduck_methods = set([name for name in SmokedDuck.__dict__.keys()])
        for name, method in duckdb.DuckDBPyConnection.__dict__.items():
            if callable(method) and name not in smokedduck_methods:
                setattr(self, name, getattr(self.duckdb_conn, name))

    def cursor(self):
        return SmokedDuck(self.duckdb_conn)

    def execute(
            self,
            query: str,
            capture_lineage: str = None,
            parameters: object = None,
            multiple_parameter_sets: bool = False
    ) -> duckdb.DuckDBPyRelation:
        prov_model = None
        if capture_lineage is not None:
            prov_model = provenance_models.get_prov_model(capture_lineage)
            prov_model.consider_query(query)
            for pragma_str in prov_model.pre_capture_pragmas():
                self.duckdb_conn.execute(pragma_str)
        df = self.duckdb_conn.execute(query, parameters, multiple_parameter_sets).df()
        if capture_lineage is not None:
            for pragma_str in prov_model.post_capture_pragmas():
                self.duckdb_conn.execute(pragma_str)
            metadata = self.duckdb_conn.execute(sql_statements.get_query_id_and_plan(), [query]).df()
            self.query_id = metadata['query_id'][0]
            self.query_plan = json.loads(metadata['plan'][0])
            self.captured_lineage_model = prov_model
        else:
            self.query_id = -1
            self.query_plan = None
        # Convert result dataframe back into a DuckDBPyRelation so caller can return relation in whatever
        # format they want. TODO: optimize this to avoid conversion overhead
        return self.duckdb_conn.from_df(df)

    def _lineage_query(
            self,
            model: str,
            backward_ids: list = None,
            forward_table: str = None,
            forward_ids: list = None
    ) -> duckdb.DuckDBPyConnection:
        if self.query_id == -1:
            print('No captured lineage to query')
            return None
        else:
            prov_model = provenance_models.get_prov_model(model)
            prov_model.consider_capture_model(self.captured_lineage_model)
            return self.duckdb_conn.execute(lineage_query.get_query(
                self.query_id,
                self.query_plan,
                self.operator_factory,
                prov_model,
                backward_ids,
                forward_table,
                forward_ids
            ))

    def lineage(self) -> duckdb.DuckDBPyConnection:
        return self._lineage_query('lineage')

    def why(self) -> duckdb.DuckDBPyConnection:
        return self._lineage_query('why')

    def polynomial(self) -> duckdb.DuckDBPyConnection:
        return self._lineage_query('polynomial')

    def ksemimodule(self) -> duckdb.DuckDBPyConnection:
        return self._lineage_query('ksemimodule')

    def backward(self, backward_ids: list, model: str = 'lineage') -> duckdb.DuckDBPyConnection:
        return self._lineage_query(model, backward_ids)

    def forward(self, forward_table: str, forward_ids: list, model: str = 'lineage') -> duckdb.DuckDBPyConnection:
        return self._lineage_query(model, None, forward_table, forward_ids)


def connect(database: str = ':memory:', read_only: bool = False, config: dict = None) -> SmokedDuck:
    return SmokedDuck(duckdb.connect(database, read_only, config if config is not None else {}))
