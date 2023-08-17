#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"
#include "duckdb/core_functions/aggregate/nested_functions.hpp"

#include <utility>

namespace duckdb {
class BoundReferenceExpression;


void GetColumnBindings(unique_ptr<Expression> &expr, idx_t i)  {
	string columns = "";
	if (expr->IsScalar()) {
		return;
	} else {
		bool has_children = false;
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
			GetColumnBindings(child, i);
		});
		if (expr->expression_class == ExpressionClass::BOUND_REF) {
			auto index = ((BoundReferenceExpression &)*expr).index;
			if (index >= i) ((BoundReferenceExpression &)*expr).index += 1;
		}
	}
}

vector<idx_t> AdjustPlan(ClientContext &context, PhysicalOperator *op, PhysicalOperator *parent) {
	vector<idx_t> annotations;

	// if child is hash join, then create projection and place it in between
	// op could be binary or unary operator with one of the children as join
	if ((op->type == PhysicalOperatorType::HASH_JOIN
	     || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	     || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	     || op->type == PhysicalOperatorType::CROSS_PRODUCT
	     || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	     ) ) {
		auto join = op;

		// push a projection on top that does the projection
		vector<LogicalType> types;
		vector<unique_ptr<Expression>> expressions;
		// this is affected if left_annotation_index also has an annotations?
		auto left_child = join->children[0].get();
		// always with +1 since we assume all children pass single annotation column
		auto left_annotation_index = left_child->types.size();
		for (storage_t column_id = 0; column_id < join->types.size(); column_id++) {
			auto col_type = join->types[column_id];
			types.push_back(col_type);
			if (column_id >= left_annotation_index) {
				expressions.push_back(make_uniq<BoundReferenceExpression>(col_type, column_id+1));
			} else {
				expressions.push_back(make_uniq<BoundReferenceExpression>(col_type, column_id));
			}
		}

		if (parent == nullptr) {
			std::cout << "join without parent" << std::endl;
		} else if (op == parent->children[0].get()) {
			auto hj_unique = std::move(parent->children[0]);
			// construct projection and stitch the plan again
			auto projection =
			    make_uniq<PhysicalProjection>(std::move(types), std::move(expressions), join->estimated_cardinality);
			projection->children.push_back(std::move(hj_unique));
			projection->special = true;
			projection->drop_left = true;
			projection->drop_annotations = true;
			projection->add_annotations = true;
			projection->left_annotation_index = left_annotation_index;
			parent->children[0] = std::move(projection);
		} else {
			auto hj_unique = std::move(parent->children[1]);
			// construct projection and stitch the plan again
			auto projection =
			    make_uniq<PhysicalProjection>(std::move(types), std::move(expressions), join->estimated_cardinality);
			projection->children.push_back(std::move(hj_unique));
			projection->special = true;
			projection->drop_left = true;
			projection->drop_annotations = true;
			projection->add_annotations = true;
			projection->left_annotation_index = left_annotation_index;
			parent->children[1] = std::move(projection);
		}

	} else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
		auto gb = op;
		if (parent) {
		// push a projection on top that does the projection
		vector<LogicalType> types;
		vector<unique_ptr<Expression>> expressions;
		for (storage_t column_id = 0; column_id < gb->types.size(); column_id++) {
			auto col_type = gb->types[column_id];
			types.push_back(col_type);
			expressions.push_back(make_uniq<BoundReferenceExpression>(col_type, column_id));
		}
			auto child_unique = std::move(parent->children[0]);
			auto projection =
			    make_uniq<PhysicalProjection>(std::move(types), std::move(expressions), gb->estimated_cardinality);
			projection->children.push_back(std::move(child_unique));
			projection->drop_annotations = true;
			projection->add_annotations = true;
			parent->children[0] = std::move(projection);
		}
	}


	for (idx_t i = 0; i < op->children.size(); i++) {
		annotations = AdjustPlan(context, op->children[i].get(), op);
	}
	return annotations;
}

vector<idx_t> RecurseAddProvenance(ClientContext &context, PhysicalOperator *op) {
	vector<idx_t> annotations;

	for (idx_t i = 0; i < op->children.size(); i++) {
		annotations = RecurseAddProvenance(context, op->children[i].get());
	}

	switch (op->type) {
	case PhysicalOperatorType::TABLE_SCAN: {
		if (!((PhysicalTableScan* )op)->function.projection_pushdown) {
		} else {
			op->types.push_back(LogicalTypeId::BIGINT);
			((PhysicalTableScan* )op)->returned_types.push_back(LogicalTypeId::BIGINT);
			((PhysicalTableScan* )op)->column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
			((PhysicalTableScan* )op)->projection_ids.push_back(((PhysicalTableScan* )op)->column_ids.size()-1);
		}
		return { op->types.size() };
	}
	case PhysicalOperatorType::PROJECTION: {
		op->types.push_back(LogicalTypeId::BIGINT);
		idx_t last = op->children[0]->types.size()-1;
		// adjust select list to reference on
		((PhysicalProjection* )op)->select_list.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, last));

		return { op->types.size() };
	}
	case PhysicalOperatorType::TOP_N:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER: {
		op->types.push_back(LogicalTypeId::BIGINT);
		return { op->types.size() };
	}
	case PhysicalOperatorType::ORDER_BY: {
		((PhysicalOrder* )op)->projections.push_back(op->types.size());
		op->types.push_back(LogicalTypeId::BIGINT);
		return { op->types.size() };
	}
	case PhysicalOperatorType::HASH_JOIN: {
		idx_t left = op->children[0]->types.size();
		op->types.insert(op->types.begin() + left-1, LogicalTypeId::BIGINT);
		op->types.push_back(LogicalTypeId::BIGINT);
		((PhysicalHashJoin* )op)->build_types.push_back(LogicalTypeId::BIGINT);

		if (!((PhysicalHashJoin* )op)->right_projection_map.empty()) {
			((PhysicalHashJoin* )op)->right_projection_map.push_back(op->children[1]->types.size()-1);
		}
		return { op->types.size() };
	}
	case PhysicalOperatorType::IE_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		idx_t left = op->children[0]->types.size();
		op->types.insert(op->types.begin() + left-1, LogicalTypeId::BIGINT);
		op->types.push_back(LogicalTypeId::BIGINT);

		// if join type == semi or anti semi, then disable that --> what are the implications?

		return { op->types.size() };
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		// output types
		op->types.push_back(LogicalType::LIST(LogicalType::BIGINT));
		// input types
		((PhysicalPerfectHashAggregate* )op)->payload_types.push_back(LogicalTypeId::BIGINT);

		auto list_aggfn = ListFun::GetFunction();
		list_aggfn.name = "list";
		auto colref = make_uniq<BoundReferenceExpression>("i", LogicalTypeId::BIGINT, op->children[0]->types.size()-1);
		vector<unique_ptr<Expression>> aggr_children;
		aggr_children.push_back(std::move(colref));
		unique_ptr<FunctionData> bind_info = list_aggfn.bind(context, list_aggfn, aggr_children);
		auto list_fun =
		    make_uniq<BoundAggregateExpression>(std::move(list_aggfn), std::move(aggr_children), nullptr,
		                                        std::move(bind_info), AggregateType::NON_DISTINCT);
		((PhysicalPerfectHashAggregate* )op)->aggregates.push_back(move(list_fun));
		auto size = ((PhysicalPerfectHashAggregate* )op)->aggregates.size();

		// bindings
		((PhysicalPerfectHashAggregate* )op)->aggregate_objects.push_back(AggregateObject(&((PhysicalPerfectHashAggregate* )op)->aggregates[size-1]->Cast<BoundAggregateExpression>()));
		return { op->types.size() };
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		// output types
		op->types.push_back(LogicalType::LIST(LogicalType::BIGINT));
		((PhysicalHashAggregate* )op)->grouped_aggregate_data.payload_types.push_back(LogicalTypeId::BIGINT);
		// bindings
		((PhysicalHashAggregate* )op)->grouped_aggregate_data.aggregate_return_types.push_back(LogicalType::LIST(LogicalTypeId::BIGINT));

		auto list_aggfn = ListFun::GetFunction();
		list_aggfn.name = "list";
		auto colref = make_uniq<BoundReferenceExpression>("i", LogicalTypeId::BIGINT, op->children[0]->types.size()-1);
		vector<unique_ptr<Expression>> aggr_children;
		aggr_children.push_back(std::move(colref));
		unique_ptr<FunctionData> bind_info = list_aggfn.bind(context, list_aggfn, aggr_children);
		auto list_fun =
		    make_uniq<BoundAggregateExpression>(std::move(list_aggfn), std::move(aggr_children), nullptr,
		                                        std::move(bind_info), AggregateType::NON_DISTINCT);
		((PhysicalHashAggregate* )op)->grouped_aggregate_data.aggregates.push_back(move(list_fun));
		auto size = ((PhysicalHashAggregate* )op)->grouped_aggregate_data.aggregates.size();
		((PhysicalHashAggregate* )op)->grouped_aggregate_data.bindings.push_back(&((PhysicalHashAggregate* )op)->grouped_aggregate_data.aggregates[size-1]->Cast<BoundAggregateExpression>());
		((PhysicalHashAggregate* )op)->non_distinct_filter.push_back(size-1);
		return { op->types.size() };
	}
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN: {
		// if left/right includes rowid dont add, else include it
		idx_t left = op->children[0]->types.size();
		// find where in op.types ends for left side then add BIGINT
		op->types.insert(op->types.begin() + left-1, LogicalTypeId::BIGINT);
		// same for right
		op->types.push_back(LogicalTypeId::BIGINT);
		// left_projection_map
		// right_projection_map

		// in condition, change index of element at  left->types.size() to  left->types.size()+1
		// recursively call children until we reach BoundReferenceExpression, then any index that reference elements from the right
		// add 1 to them
		GetColumnBindings(((PhysicalBlockwiseNLJoin*)op)->condition, left-1);
		return { op->types.size() };
	}
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::ASOF_JOIN:
	case PhysicalOperatorType::INDEX_JOIN: {

	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	return {};
}

unique_ptr<PhysicalOperator> LineageManager::AddProvenance(ClientContext &context, unique_ptr<PhysicalOperator> op) {
	if (trace_lineage && op->type != PhysicalOperatorType::TRANSACTION && op->type != PhysicalOperatorType::PRAGMA) {
		AdjustPlan(context, op.get(), nullptr);
		RecurseAddProvenance(context, op.get());

		// push a projection on top that does the projection
		vector<LogicalType> types;
		vector<unique_ptr<Expression>> expressions;
		for (storage_t column_id=0;  column_id < op->types.size(); column_id++) {
			auto col_type = op->types[column_id];
			if ((op->type == PhysicalOperatorType::HASH_JOIN
			     || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
			     || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
			     || op->type == PhysicalOperatorType::CROSS_PRODUCT
			     || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
			     ) && column_id == op->children[0]->types.size()-1) {
			} else {
				types.push_back(col_type);
				expressions.push_back( make_uniq<BoundReferenceExpression>(col_type, column_id));
			}
		}

		types.pop_back();
		expressions.pop_back();

		auto projection =
		    make_uniq<PhysicalProjection>(std::move(types), std::move(expressions), op->estimated_cardinality);

		projection->special = true;
		projection->drop_left = true;
		if ((op->type == PhysicalOperatorType::HASH_JOIN
		     || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
		     || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
		     || op->type == PhysicalOperatorType::CROSS_PRODUCT
		     || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
		     ) ) {
			projection->drop_annotations = true;
			projection->left_annotation_index = op->children[0]->types.size()-1;
		} else {
			projection->left_annotation_index = projection->types.size();
		}

		projection->children.push_back(std::move(op));
		return projection;
	}

	return op;
}

void LineageManager::CreateOperatorLineage(PhysicalOperator *op, bool trace_lineage) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		CreateOperatorLineage(op->children[i].get(), trace_lineage);
	}
	op->lineage_op = make_shared<OperatorLineage>(op->type, trace_lineage);
}

// Iterate through in Postorder to ensure that children have PipelineLineageNodes set before parents
idx_t PlanAnnotator(PhysicalOperator *op, idx_t counter) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		counter = PlanAnnotator(op->children[i].get(), counter);
	}
	op->id = counter;
	return counter + 1;
}

void LineageManager::InitOperatorPlan(PhysicalOperator *op) {
	PlanAnnotator(op, 0);
	CreateOperatorLineage(op, trace_lineage);
}

void LineageManager::CreateLineageTables(ClientContext &context, PhysicalOperator *op, idx_t query_id) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		CreateLineageTables(context, op->children[i].get(), query_id);
	}

	// Example: LINEAGE_1_HASH_JOIN_3
	string prefix = "LINEAGE_" + to_string(query_id) + "_" + op->GetName() + "_" + to_string(op->id);

	prefix.erase( remove( prefix.begin(), prefix.end(), ' ' ), prefix.end() );
	// add column_stats, cardinality
	string catalog_name = "";
	auto binder = Binder::CreateBinder(context);
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	auto table_column_types = op->lineage_op->GetTableColumnTypes();
	for (idx_t i = 0; i < table_column_types.size(); i++) {
		if (table_column_types[i].size() == 0) continue;

		// Example: LINEAGE_1_HASH_JOIN_3_0
		string table_name = prefix + "_" + to_string(i);
		std::cout << table_name << std::endl;
		// Create Table
		auto create_info = make_uniq<CreateTableInfo>(catalog_name, DEFAULT_SCHEMA, table_name);
		for (idx_t col_i = 0; col_i < table_column_types[i].size(); col_i++) {
			create_info->columns.AddColumn(move(table_column_types[i][col_i]));
		}
		table_lineage_op[table_name] =  op->lineage_op;
		DuckTableEntry* table = (DuckTableEntry*)catalog.CreateTable(context, move(create_info)).get();
	}
}

void LineageManager::StoreQueryLineage(ClientContext &context, PhysicalOperator *op, string query) {
	if (!trace_lineage)
		return;

	idx_t query_id = query_to_id.size();
	query_to_id.push_back(query);
	queryid_to_plan[query_id] = op;
	std::cout << "query_id: " << query_id << std::endl;
	CreateLineageTables(context, op, query_id);
}

} // namespace duckdb
#endif
