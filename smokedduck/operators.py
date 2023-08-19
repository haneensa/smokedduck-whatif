from abc import ABC, abstractmethod


def get_op(op_str, query_id, parent_join_cond):
    op, op_id = op_str.rsplit("_", 1)
    if op == 'LIMIT':
        return Limit(query_id, op_id, parent_join_cond)
    elif op == 'FILTER':
        return Filter(query_id, op_id, parent_join_cond)
    elif op == 'ORDER_BY':
        return OrderBy(query_id, op_id, parent_join_cond)
    elif op == 'PROJECTION':
        return Projection(query_id, op_id, parent_join_cond)
    elif op == 'SEQ_SCAN':
        return TableScan(query_id, op_id, parent_join_cond)
    elif op == 'HASH_GROUP_BY':
        return GroupBy(query_id, op_id, parent_join_cond)
    elif op == 'PERFECT_HASH_GROUP_BY':
        return PerfectGroupBy(query_id, op_id, parent_join_cond)
    elif op == 'BLOCKWISE_NL_JOIN':
        return BlockwiseNLJoin(query_id, op_id, parent_join_cond)
    elif op == 'PIECEWISE_MERGE_JOIN':
        return PiecewiseMergeJoin(query_id, op_id, parent_join_cond)
    elif op == 'HASH_JOIN':
        return HashJoin(query_id, op_id, parent_join_cond)
    elif op == 'CROSS_PRODUCT':
        return CrossProduct(query_id, op_id, parent_join_cond)
    elif op == 'NESTED_LOOP_JOIN':
        return NestedLoopJoin(query_id, op_id, parent_join_cond)
    else:
        raise Exception('Found unhandled operator')


class Op(ABC):
    def __init__(self, query_id, op, op_id, parent_join_cond):
        self.single_op_table_name = f"LINEAGE_{query_id}_{op}_{op_id}"
        self.id = op_id
        self.is_root = parent_join_cond is None
        self.parent_join_cond = parent_join_cond

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def get_from_string(self):
        pass

    @abstractmethod
    def get_child_join_conds(self):
        pass

    @abstractmethod
    def get_out_index(self):
        pass


class Limit(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.inner_op_table_name = self.single_op_table_name + "_0"

    def get_name(self):
        return "LIMIT"

    def get_from_string(self):
        if self.is_root:
            return self.inner_op_table_name + " AS " + self.single_op_table_name
        else:
            return "JOIN " + self.inner_op_table_name + " AS " + self.single_op_table_name \
                + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"

    def get_child_join_conds(self):
        return [self.single_op_table_name + ".in_index"]

    def get_out_index(self):
        return self.single_op_table_name + ".out_index"


class Filter(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.inner_op_table_name = self.single_op_table_name + "_0"

    def get_name(self):
        return "FILTER"

    def get_from_string(self):
        if self.is_root:
            return self.inner_op_table_name + " AS " + self.single_op_table_name
        else:
            return "JOIN " + self.inner_op_table_name + " AS " + self.single_op_table_name \
                + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"

    def get_child_join_conds(self):
        return [self.single_op_table_name + ".in_index"]

    def get_out_index(self):
        return self.single_op_table_name + ".out_index"


class OrderBy(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.inner_op_table_name = self.single_op_table_name + "_0"

    def get_name(self):
        return "ORDER_BY"

    def get_from_string(self):
        if self.is_root:
            return self.inner_op_table_name + " AS " + self.single_op_table_name
        else:
            return "JOIN " + self.inner_op_table_name + " AS " + self.single_op_table_name \
                + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"

    def get_child_join_conds(self):
        return [self.single_op_table_name + ".in_index"]

    def get_out_index(self):
        return self.single_op_table_name + ".out_index"


class Projection(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.inner_op_table_name = self.single_op_table_name + "_0"

    def get_name(self):
        return "PROJECTION"

    def get_from_string(self):
        if self.is_root:
            return self.inner_op_table_name + " AS " + self.single_op_table_name
        else:
            return "JOIN " + self.inner_op_table_name + " AS " + self.single_op_table_name \
                + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"

    def get_child_join_conds(self):
        return [self.single_op_table_name + ".in_index"]

    def get_out_index(self):
        return self.single_op_table_name + ".out_index"


class TableScan(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.inner_op_table_name = self.single_op_table_name + "_0"

    def get_name(self):
        return "SEQ_SCAN"

    def get_from_string(self):
        if self.is_root:
            return self.inner_op_table_name + " AS " + self.single_op_table_name
        else:
            return "JOIN " + self.inner_op_table_name + " AS " + self.single_op_table_name \
                + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"

    def get_child_join_conds(self):
        return [self.single_op_table_name + ".in_index"]

    def get_out_index(self):
        return self.single_op_table_name + ".out_index"


class GroupBy(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.source = self.single_op_table_name + "_0"
        self.sink = self.single_op_table_name + "_1"
        self.combine = self.single_op_table_name + "_2"  # TODO: integrate combine
        self.finalize = self.single_op_table_name + "_3"

    def get_name(self):
        return "HASH_GROUP_BY"

    def get_from_string(self):
        join_sink_and_finalize = " JOIN " + self.finalize + " ON " + self.source + ".in_index = " + self.finalize + ".out_index" + \
                                 " JOIN " + self.sink + " AS " + self.single_op_table_name + " ON " + self.finalize + ".in_index = " + \
                                 self.single_op_table_name + ".out_index"
        if self.is_root:
            return self.source + join_sink_and_finalize
        else:
            return "JOIN " + self.source + " ON " + self.parent_join_cond + " = " + self.source + ".out_index" + join_sink_and_finalize

    def get_child_join_conds(self):
        return [self.single_op_table_name + ".in_index"]

    def get_out_index(self):
        return self.source + ".out_index"


class PerfectGroupBy(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.source = self.single_op_table_name + "_0"
        self.sink = self.single_op_table_name + "_1"
        self.combine = self.single_op_table_name + "_2"  # TODO: integrate combine
        self.finalize = self.single_op_table_name + "_3"

    def get_name(self):
        return "PERFECT_HASH_GROUP_BY"

    def get_from_string(self):
        join_sink_and_finalize = " JOIN " + self.finalize + " ON " + self.source + ".in_index = " + self.finalize + ".out_index" + \
                                 " JOIN " + self.sink + " AS " + self.single_op_table_name + " ON " + self.finalize + ".in_index = " + \
                                 self.single_op_table_name + ".out_index"
        if self.is_root:
            return self.source + join_sink_and_finalize
        else:
            return "JOIN " + self.source + " ON " + self.parent_join_cond + " = " + self.source + ".out_index" + join_sink_and_finalize

    def get_child_join_conds(self):
        return [self.single_op_table_name + ".in_index"]

    def get_out_index(self):
        return self.source + ".out_index"


class BlockwiseNLJoin(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.inner_op_table_name = self.single_op_table_name + "_0"

    def get_name(self):
        return "BLOCKWISE_NL_JOIN"

    def get_from_string(self):
        if self.is_root:
            return self.inner_op_table_name + " AS " + self.single_op_table_name
        else:
            return "JOIN " + self.inner_op_table_name + " AS " + self.single_op_table_name \
                + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"

    def get_child_join_conds(self):
        return [self.single_op_table_name + ".lhs_index", self.single_op_table_name + ".rhs_index"]

    def get_out_index(self):
        return self.single_op_table_name + ".out_index"


class PiecewiseMergeJoin(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.inner_op_table_name = self.single_op_table_name + "_0"

    def get_name(self):
        return "PIECEWISE_MERGE_JOIN"

    def get_from_string(self):
        if self.is_root:
            return self.inner_op_table_name + " AS " + self.single_op_table_name
        else:
            return "JOIN " + self.inner_op_table_name + " AS " + self.single_op_table_name \
                + " ON " + self.parent_join_cond + " = " + self.single_op_table_name + ".out_index"

    def get_child_join_conds(self):
        return [self.single_op_table_name + ".lhs_index", self.single_op_table_name + ".rhs_index"]

    def get_out_index(self):
        return self.single_op_table_name + ".out_index"


class HashJoin(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.probe = self.single_op_table_name + "_0"
        self.build = self.single_op_table_name + "_1"
        self.combine = self.single_op_table_name + "_2"  # TODO: integrate combine
        self.finalize = self.single_op_table_name + "_3"  # TODO: integrate finalize

        self.probe_name = self.single_op_table_name + "_probe"
        self.build_name = self.single_op_table_name + "_build"

    def get_name(self):
        return "HASH_JOIN"

    def get_from_string(self):
        join_build = " JOIN " + self.build + " AS " + self.build_name + " ON " + self.probe_name + ".rhs_index = " + \
                     self.build_name + ".out_index"

        if self.is_root:
            return self.probe + " AS " + self.probe_name + join_build
        else:
            return "JOIN " + self.probe + " AS " + self.probe_name + " ON " + self.parent_join_cond + " = " + \
                self.probe_name + ".out_index" + join_build

    def get_child_join_conds(self):
        return [self.build_name + ".in_index", self.probe_name + ".lhs_index"]

    def get_out_index(self):
        return self.probe_name + ".out_index"


class CrossProduct(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.probe = self.single_op_table_name + "_0"
        self.build = self.single_op_table_name + "_1"
        self.combine = self.single_op_table_name + "_2"  # TODO: integrate combine
        self.finalize = self.single_op_table_name + "_3"  # TODO: integrate finalize

        self.probe_name = self.single_op_table_name + "_probe"
        self.build_name = self.single_op_table_name + "_build"

    def get_name(self):
        return "CROSS_PRODUCT"

    def get_from_string(self):
        join_build = " JOIN " + self.build + " AS " + self.build_name + " ON " + self.probe_name + ".rhs_index = " + \
                     self.build_name + ".out_index"

        if self.is_root:
            return self.probe + " AS " + self.probe_name + join_build
        else:
            return "JOIN " + self.probe + " AS " + self.probe_name + " ON " + self.parent_join_cond + " = " + \
                self.probe_name + ".out_index" + join_build

    def get_child_join_conds(self):
        return [self.build_name + ".in_index", self.probe_name + ".lhs_index"]

    def get_out_index(self):
        return self.probe_name + ".out_index"


class NestedLoopJoin(Op):
    def __init__(self, query_id, op_id, parent_join_cond):
        super().__init__(query_id, self.get_name(), op_id, parent_join_cond)
        self.probe = self.single_op_table_name + "_0"
        self.build = self.single_op_table_name + "_1"
        self.combine = self.single_op_table_name + "_2"  # TODO: integrate combine
        self.finalize = self.single_op_table_name + "_3"  # TODO: integrate finalize

        self.probe_name = self.single_op_table_name + "_probe"
        self.build_name = self.single_op_table_name + "_build"

    def get_name(self):
        return "NESTED_LOOP_JOIN"

    def get_from_string(self):
        join_build = " JOIN " + self.build + " AS " + self.build_name + " ON " + self.probe_name + ".rhs_index = " + \
                     self.build_name + ".out_index"

        if self.is_root:
            return self.probe + " AS " + self.probe_name + join_build
        else:
            return "JOIN " + self.probe + " AS " + self.probe_name + " ON " + self.parent_join_cond + " = " + \
                self.probe_name + ".out_index" + join_build

    def get_child_join_conds(self):
        return [self.build_name + ".in_index", self.probe_name + ".lhs_index"]

    def get_out_index(self):
        return self.probe_name + ".out_index"
