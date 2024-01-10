from abc import ABC, abstractmethod
from operators import Op, OperatorFactory


class ProvenanceModel(ABC):
    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def consider_query(self, query: str) -> None:
        pass

    @abstractmethod
    def pre_capture_pragmas(self) -> list:
        pass

    @abstractmethod
    def post_capture_pragmas(self) -> list:
        pass

    @abstractmethod
    def get_froms(self, plan: dict, query_id: int, op: Op) -> list:
        pass

    @abstractmethod
    def from_prefix(self) -> str:
        pass

    @abstractmethod
    def visit_from(self, projections: list, i: int) -> str:
        pass

    @abstractmethod
    def from_suffix(self) -> str:
        pass

    @abstractmethod
    def query_suffix(self, out_index: str) -> str:
        pass

    @abstractmethod
    def consider_capture_model(self, capture_model) -> None:
        pass


class Lineage(ProvenanceModel):
    def get_name(self) -> str:
        return "lineage"

    def consider_query(self, query: str) -> None:
        pass

    def pre_capture_pragmas(self) -> list:
        return ["pragma enable_intermediate_tables", "pragma enable_lineage"]

    def post_capture_pragmas(self) -> list:
        return ["pragma disable_lineage", "pragma disable_intermediate_tables"]

    def get_froms(self, plan: dict, query_id: int, op: Op) -> list:
        return [op.get_from_string()]

    def from_prefix(self) -> str:
        return ""

    def visit_from(self, projections: list, i: int) -> str:
        assert i < len(projections)
        if i < len(projections) - 1:
            return projections[i].in_index + " AS " + projections[i].alias + ", "
        else:
            return projections[i].in_index + " AS " + projections[i].alias

    def from_suffix(self) -> str:
        return ""

    def query_suffix(self, out_index: str) -> str:
        return ""

    def consider_capture_model(self, capture_model: ProvenanceModel) -> None:
        pass


class WhyProvenance(ProvenanceModel):
    def get_name(self) -> str:
        return "why"

    def consider_query(self, query: str) -> None:
        pass

    def pre_capture_pragmas(self) -> list:
        return ["pragma enable_lineage"]

    def post_capture_pragmas(self) -> list:
        return ["pragma disable_lineage"]

    def get_froms(self, plan: dict, query_id: int, op: Op) -> list:
        return [op.get_from_string()]

    def from_prefix(self) -> str:
        return "list(["

    def visit_from(self, projections: list, i: int) -> str:
        assert i < len(projections)
        if i < len(projections) - 1:
            return projections[i].in_index + ", "
        else:
            return projections[i].in_index

    def from_suffix(self) -> str:
        return "]) AS prov"

    def query_suffix(self, out_index: str) -> str:
        return " GROUP BY " + out_index

    def consider_capture_model(self, capture_model: ProvenanceModel) -> None:
        pass


class ProvenancePolynomials(ProvenanceModel):
    def get_name(self) -> str:
        return "polynomial"

    def consider_query(self, query: str) -> None:
        pass

    def pre_capture_pragmas(self) -> list:
        return ["pragma enable_lineage"]

    def post_capture_pragmas(self) -> list:
        return ["pragma disable_lineage"]

    def get_froms(self, plan: dict, query_id: int, op: Op) -> list:
        return [op.get_from_string()]

    def from_prefix(self) -> str:
        return "string_agg("

    def visit_from(self, projections: list, i: int) -> str:
        assert i < len(projections)
        if i < len(projections) - 1:
            return projections[i].in_index + "|| '*' ||"
        else:
            return projections[i].in_index

    def from_suffix(self) -> str:
        return ", '+') AS prov"

    def query_suffix(self, out_index: str) -> str:
        return " GROUP BY " + out_index

    def consider_capture_model(self, capture_model: ProvenanceModel) -> None:
        pass


class KSemimodule(ProvenanceModel):
    def __init__(self) -> None:
        self.aggregate = None
        self.agg_table = None

    def get_name(self) -> str:
        return "ksemimodule"

    def consider_query(self, query: str) -> None:
        count_count = query.lower().count("count")
        sum_count = query.lower().count("sum")
        if (count_count > 0 and sum_count > 0) or (count_count == 0 and sum_count == 0) or count_count > 1 or sum_count > 1:
            raise Exception("KSemimodule can only handle a single count or sum aggregation (for now)")
        if count_count == 1:
            self.aggregate = "count"
        else:
            self.aggregate = "sum"

    def pre_capture_pragmas(self) -> list:
        return ["pragma enable_k_semimodule_tables", "pragma enable_lineage"]

    def post_capture_pragmas(self) -> list:
        return ["pragma disable_lineage", "pragma disable_k_semimodule_tables"]

    def get_froms(self, plan: dict, query_id: int, op: Op) -> list:
        assert self.aggregate is not None
        if self.aggregate != "count" and op.get_name() in ["HASH_GROUP_BY", "PERFECT_HASH_GROUP_BY"]:
            assert len(plan['children']) == 1
            assert self.agg_table is None, "Currently only queries with single group bys are supported" # TODO this
            # We only build the operator to get single_op_table_name, so safe to pass Nones here
            child = OperatorFactory(None).get_op(plan["children"][0]["name"], query_id, None)
            agg_table = "agg_" + op.id
            self.agg_table = agg_table
            k_semimodule_from_string = "JOIN " + child.single_op_table_name + "_100 AS " + agg_table \
                                       + " ON " + op.single_op_table_name + ".rowid = " + agg_table + ".rowid"
            return [op.get_from_string(), k_semimodule_from_string]
        else:
            return [op.get_from_string()]

    # Will need to change this for other aggregates
    def from_prefix(self) -> str:
        assert self.aggregate is not None
        return "count(" if self.aggregate == "count" else "sum("

    # Will need to change this for other aggregates
    def visit_from(self, projections: list, i: int) -> str:
        assert self.aggregate is not None
        assert i < len(projections)
        if i < len(projections) - 1:
            return ""
        else:
            return "*" if self.aggregate == "count" else self.agg_table + ".col_1" # TODO is this always right?

    def from_suffix(self) -> str:
        assert self.aggregate is not None
        return ") AS aggregate"

    def query_suffix(self, out_index: str) -> str:
        assert self.aggregate is not None
        return " GROUP BY " + out_index

    def consider_capture_model(self, capture_model: ProvenanceModel) -> None:
        if capture_model.get_name() != 'ksemimodule':
            raise Exception("Must capture with ksemimodule lineage to ensure aggregate intermediate tables are captured")
        self.aggregate = capture_model.aggregate


def get_prov_model(model_str: str) -> ProvenanceModel:
    if model_str == "lineage":
        return Lineage()
    elif model_str == "why":
        return WhyProvenance()
    elif model_str == "polynomial":
        return ProvenancePolynomials()
    elif model_str == "ksemimodule":
        return KSemimodule()
    else:
        raise Exception("Found unhandled provenance model")
