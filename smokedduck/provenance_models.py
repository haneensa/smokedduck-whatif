from abc import ABC, abstractmethod
import operators


def get_prov_model(model_str):
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


class ProvenanceModel(ABC):
    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def consider_query(self, query):
        pass

    @abstractmethod
    def pre_capture_pragmas(self):
        pass

    @abstractmethod
    def post_capture_pragmas(self):
        pass

    @abstractmethod
    def get_froms(self, plan, query_id, op):
        pass

    @abstractmethod
    def from_prefix(self):
        pass

    @abstractmethod
    def visit_from(self, projections, i):
        pass

    @abstractmethod
    def from_suffix(self):
        pass

    @abstractmethod
    def query_suffix(self, out_index):
        pass

    @abstractmethod
    def consider_capture_model(self, capture_model):
        pass


class Lineage(ProvenanceModel):
    def get_name(self):
        return "lineage"

    def consider_query(self, query):
        pass

    def pre_capture_pragmas(self):
        return ["pragma enable_lineage"]

    def post_capture_pragmas(self):
        return ["pragma disable_lineage"]

    def get_froms(self, plan, query_id, op):
        return [op.get_from_string()]

    def from_prefix(self):
        return ""

    def visit_from(self, projections, i):
        assert i < len(projections)
        if i < len(projections) - 1:
            return projections[i].in_index + " AS " + projections[i].alias + ", "
        else:
            return projections[i].in_index + " AS " + projections[i].alias

    def from_suffix(self):
        return ""

    def query_suffix(self, out_index):
        return ""

    def consider_capture_model(self, capture_model):
        pass


class WhyProvenance(ProvenanceModel):
    def get_name(self):
        return "why"

    def consider_query(self, query):
        pass

    def pre_capture_pragmas(self):
        return ["pragma enable_lineage"]

    def post_capture_pragmas(self):
        return ["pragma disable_lineage"]

    def get_froms(self, plan, query_id, op):
        return [op.get_from_string()]

    def from_prefix(self):
        return "list(["

    def visit_from(self, projections, i):
        assert i < len(projections)
        if i < len(projections) - 1:
            return projections[i].in_index + ", "
        else:
            return projections[i].in_index

    def from_suffix(self):
        return "]) AS prov"

    def query_suffix(self, out_index):
        return " GROUP BY " + out_index

    def consider_capture_model(self, capture_model):
        pass


class ProvenancePolynomials(ProvenanceModel):
    def get_name(self):
        return "polynomial"

    def consider_query(self, query):
        pass

    def pre_capture_pragmas(self):
        return ["pragma enable_lineage"]

    def post_capture_pragmas(self):
        return ["pragma disable_lineage"]

    def get_froms(self, plan, query_id, op):
        return [op.get_from_string()]

    def from_prefix(self):
        return "string_agg("

    def visit_from(self, projections, i):
        assert i < len(projections)
        if i < len(projections) - 1:
            return projections[i].in_index + "|| '*' ||"
        else:
            return projections[i].in_index

    def from_suffix(self):
        return ", '+') AS prov"

    def query_suffix(self, out_index):
        return " GROUP BY " + out_index

    def consider_capture_model(self, capture_model):
        pass


class KSemimodule(ProvenanceModel):
    def __init__(self):
        self.aggregate = None
        self.agg_table = None

    def consider_query(self, query):
        count_count = query.lower().count("count")
        sum_count = query.lower().count("sum")
        if (count_count > 0 and sum_count > 0) or (count_count == 0 and sum_count == 0) or count_count > 1 or sum_count > 1:
            raise Exception("KSemimodule can only handle a single count or sum aggregation (for now)")
        if count_count == 1:
            self.aggregate = "count"
        else:
            self.aggregate = "sum"

    def get_name(self):
        return "ksemimodule"

    def pre_capture_pragmas(self):
        return ["pragma enable_k_semimodule_tables", "pragma enable_lineage"]

    def post_capture_pragmas(self):
        return ["pragma disable_lineage", "pragma disable_k_semimodule_tables"]

    def get_froms(self, plan, query_id, op):
        assert self.aggregate is not None
        if self.aggregate != "count" and op.get_name() in ["HASH_GROUP_BY", "PERFECT_HASH_GROUP_BY"]:
            assert len(plan['children']) == 1
            assert self.agg_table is None, "Currently only queries with single group bys are supported" # TODO this
            # We only build the operator to get single_op_table_name, so safe to pass None here
            child = operators.get_op(plan["children"][0]["name"], query_id, None)
            agg_table = "agg_" + op.id
            self.agg_table = agg_table
            k_semimodule_from_string = "JOIN " + child.single_op_table_name + "_100 AS " + agg_table \
                                       + " ON " + op.single_op_table_name + ".rowid = " + agg_table + ".rowid"
            return [op.get_from_string(), k_semimodule_from_string]
        else:
            return [op.get_from_string()]

    # Will need to change this for other aggregates
    def from_prefix(self):
        assert self.aggregate is not None
        return "count(" if self.aggregate == "count" else "sum("

    # Will need to change this for other aggregates
    def visit_from(self, projections, i):
        assert self.aggregate is not None
        assert i < len(projections)
        if i < len(projections) - 1:
            return ""
        else:
            return "*" if self.aggregate == "count" else self.agg_table + ".col_1" # TODO is this always right?

    def from_suffix(self):
        assert self.aggregate is not None
        return ") AS aggregate"

    def query_suffix(self, out_index):
        assert self.aggregate is not None
        return " GROUP BY " + out_index

    def consider_capture_model(self, capture_model):
        if capture_model.get_name() != 'ksemimodule':
            raise Exception("Must capture with ksemimodule lineage to ensure aggregate intermediate tables are captured")
        self.aggregate = capture_model.aggregate
