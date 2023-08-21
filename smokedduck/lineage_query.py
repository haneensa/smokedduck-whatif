from collections import namedtuple
import operators
from provenance_models import ProvenanceModel

Projection = namedtuple('Projection', ['in_index', 'alias', 'orig_table_name'])


def get_query(
        id: int,
        plan: dict,
        prov_model: ProvenanceModel,
        backward_ids: list,
        forward_table: str,
        forward_ids: list
) -> str:
    # Check that both forward table and forward ids are set together
    assert forward_table is None or (forward_table is not None and forward_ids is not None)

    topmost_op, _, projections, froms = _generate_lineage_query(plan, id, prov_model, None)

    ret = "SELECT "
    ret += prov_model.from_prefix()

    for i in range(len(projections)):
        ret += prov_model.visit_from(projections, i)

    ret += prov_model.from_suffix()

    out_index = topmost_op.get_out_index()
    ret += ", " + out_index + " FROM "

    for i in range(len(froms)):
        ret += froms[i]
        if i != len(froms) - 1:
            ret += " "

    should_filter = backward_ids is not None or forward_table is not None
    if should_filter:
        ret += " WHERE "

    if backward_ids is not None:
        ret += out_index + " IN ("
        ret += ", ".join([str(id) for id in backward_ids])
        ret += ")"
        if forward_table:
            ret += " AND "

    if forward_table is not None:
        found_forward = False
        for projection in projections:
            if projection.orig_table_name == forward_table:
                if found_forward:
                    # Found table before, but there could be multiple
                    ret += " AND "
                ret += projection.in_index + " IN ("
                ret += ", ".join([str(id) for id in forward_ids])
                ret += ")"
                found_forward = True
        if not found_forward:
            raise Exception("Selected forward lineage table " + forward_table + " not found in query")

    ret += prov_model.query_suffix(out_index)

    return ret


def _generate_lineage_query(
        plan_node: dict,
        query_id: int,
        prov_model: ProvenanceModel,
        parent_join_cond: str
) -> (operators.Op, list, list, list):
    children = plan_node['children']
    projections = []
    froms = []
    found_names = []
    name_set = set()

    op = operators.get_op(plan_node['name'], query_id, parent_join_cond)
    child_join_conds = op.get_child_join_conds()
    assert len(children) == len(child_join_conds) or op.get_name() == 'SEQ_SCAN'

    froms.extend(prov_model.get_froms(plan_node, query_id, op))

    for i in range(len(children)):
        _, child_names, child_projections, child_froms = _generate_lineage_query(children[i], query_id,
                                                                                 prov_model, child_join_conds[i])

        projections.extend(child_projections)
        froms.extend(child_froms)

        # Resolve self-joins
        for table_name in child_names:
            table_name = _find_next_lineage_table_name(name_set, table_name)
            found_names.append(table_name)
            name_set.add(table_name)

    table_name = plan_node['table']
    orig_table_name = table_name
    if len(table_name) != 0:
        table_name = _find_next_lineage_table_name(name_set, table_name)
        assert len(child_join_conds) == 1
        projections.append(Projection(in_index=child_join_conds[0], alias=table_name, orig_table_name=orig_table_name))
        found_names.append(table_name)
        name_set.add(table_name)

    return op, found_names, projections, froms


def _find_next_lineage_table_name(so_far: set, name: str) -> str:
    orig_name = name
    i = 0
    while name in so_far:
        name = f"{orig_name}_{i}"
        i += 1
    return name
