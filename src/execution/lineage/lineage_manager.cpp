#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/projection/physical_lineage.hpp"
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

void AdjustPlan(ClientContext &context, PhysicalOperator *op, PhysicalOperator *parent) {

	// if child is hash join, then create projection and place it in between
	// op could be binary or unary operator with one of the children as join
	if (parent != nullptr && (op->type == PhysicalOperatorType::HASH_JOIN
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
		auto left_annotation_index = left_child->original_types.size();

		// to create a copy of the input
		for (storage_t column_id = 0; column_id < join->types.size(); column_id++) {
			auto col_type = join->types[column_id];
			expressions.push_back(make_uniq<BoundReferenceExpression>(col_type, column_id));
		}

		for (storage_t column_id = 0; column_id < join->original_types.size(); column_id++) {
			auto col_type = join->original_types[column_id];
			types.push_back(col_type);
		}

		types.push_back({LogicalType::BIGINT});

		idx_t child_index = 0;
		if (op != parent->children[0].get()) {
			child_index = 1;
		}
		auto hj_unique = std::move(parent->children[child_index]);
		// construct projection and stitch the plan again
		auto linop =
		    make_uniq<PhysicalLineage>(std::move(types), std::move(expressions), join->estimated_cardinality);
		linop->children.push_back(std::move(hj_unique));
		linop->drop_left = true;
		linop->extract_and_reindex = true;
		linop->left_annotation_index = left_annotation_index;

		parent->children[child_index] = std::move(linop);
	} else if (parent != nullptr && (op->type == PhysicalOperatorType::HASH_GROUP_BY ||
	                                 op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY)) {
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
			types.pop_back();
			types.push_back(LogicalType::BIGINT);

			auto child_unique = std::move(parent->children[0]);
			auto linop =
			    make_uniq<PhysicalLineage>(std::move(types), std::move(expressions), gb->estimated_cardinality);
			linop->children.push_back(std::move(child_unique));
			linop->extract_and_reindex = true;
			parent->children[0] = std::move(linop);
		}
	} else if (op->type == PhysicalOperatorType::UNGROUPED_AGGREGATE) {
		// insert a lineage adjust prior that extract annotations and after that insert annotations
	}


	for (idx_t i = 0; i < op->children.size(); i++) {
		AdjustPlan(context, op->children[i].get(), op);
	}
}

void RecurseAddProvenance(ClientContext &context, PhysicalOperator *op) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		RecurseAddProvenance(context, op->children[i].get());
	}

	for (storage_t column_id = 0; column_id < op->types.size(); column_id++) {
		auto col_type = op->types[column_id];
		op->original_types.push_back(col_type);
	}

	switch (op->type) {
	case PhysicalOperatorType::TABLE_SCAN: {
		if (!((PhysicalTableScan* )op)->function.projection_pushdown) {
			std::cout << "NEED TO HANDLE" << std::endl;
		} else {
			// input types
			op->types.push_back(LogicalTypeId::BIGINT);
			// output types
			((PhysicalTableScan* )op)->returned_types.push_back(LogicalTypeId::BIGINT);
			// columns id used to access base table columns
			((PhysicalTableScan* )op)->column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
			// don't project out rowid
			((PhysicalTableScan* )op)->projection_ids.push_back(((PhysicalTableScan* )op)->column_ids.size()-1);
		}
		return;
	}
	case PhysicalOperatorType::PROJECTION: {
		// input types
		op->types.push_back(LogicalTypeId::BIGINT);
		// assumption: child carry it's annotation column in the last column.
		// if we want to propagate more than a single annotation, need to rethink
		idx_t last = op->children[0]->original_types.size();
		// adjust select list to reference on
		((PhysicalProjection* )op)->select_list.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, last));
		return;
	}
	case PhysicalOperatorType::TOP_N:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER: {
		op->types.push_back(LogicalTypeId::BIGINT);
		return;
	}
	case PhysicalOperatorType::ORDER_BY: {
		// don't project rowid from the output
		((PhysicalOrder* )op)->projections.push_back(op->original_types.size());
		// input types
		op->types.push_back(LogicalTypeId::BIGINT);
		return;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// TODO: handle semi join
		// transform it to regular join with distinct agg?

		// get the original return types of the left child
		idx_t left = op->children[0]->original_types.size();
		// pass rowid of the left child
		op->types.insert(op->types.begin() + left, LogicalTypeId::BIGINT);

		// pass rowid of the right child
		op->types.push_back(LogicalTypeId::BIGINT);
		// include it in the build input
		((PhysicalHashJoin* )op)->build_types.push_back(LogicalTypeId::BIGINT);

		// if a projection is applied, then include rowid from the child being projected
		if (!((PhysicalHashJoin* )op)->right_projection_map.empty()) {
			((PhysicalHashJoin* )op)->right_projection_map.push_back(op->children[1]->original_types.size());
		}
		return;
	}
	case PhysicalOperatorType::IE_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		idx_t left = op->children[0]->original_types.size();
		op->types.insert(op->types.begin() + left, LogicalTypeId::BIGINT);
		op->types.push_back(LogicalTypeId::BIGINT);

		// if join type == semi or anti semi, then disable that --> what are the implications?

		return;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		// output types
		op->types.push_back(LogicalType::LIST(LogicalType::BIGINT));
		// input types
		((PhysicalPerfectHashAggregate* )op)->payload_types.push_back(LogicalTypeId::BIGINT);

		auto list_aggfn = ListFun::GetFunction();
		list_aggfn.name = "list";
		auto colref = make_uniq<BoundReferenceExpression>("i", LogicalTypeId::BIGINT, op->children[0]->original_types.size());
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
		return;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		// output types
		op->types.push_back(LogicalType::LIST(LogicalType::BIGINT));
		((PhysicalHashAggregate* )op)->grouped_aggregate_data.payload_types.push_back(LogicalTypeId::BIGINT);
		// bindings
		((PhysicalHashAggregate* )op)->grouped_aggregate_data.aggregate_return_types.push_back(LogicalType::LIST(LogicalTypeId::BIGINT));

		auto list_aggfn = ListFun::GetFunction();
		list_aggfn.name = "list";


		auto colref = make_uniq<BoundReferenceExpression>("i", LogicalTypeId::BIGINT, op->children[0]->original_types.size());
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
		return;
	}
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN: {
		// if left/right includes rowid dont add, else include it
		idx_t left = op->children[0]->original_types.size();
		// find where in op.types ends for left side then add BIGINT
		op->types.insert(op->types.begin() + left, LogicalTypeId::BIGINT);
		// same for right
		op->types.push_back(LogicalTypeId::BIGINT);
		// left_projection_map
		// right_projection_map

		// in condition, change index of element at  left->types.size() to  left->types.size()+1
		// recursively call children until we reach BoundReferenceExpression, then any index that reference elements from the right
		// add 1 to them
		GetColumnBindings(((PhysicalBlockwiseNLJoin*)op)->condition, left);
		return;
	}
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::ASOF_JOIN:
	case PhysicalOperatorType::INDEX_JOIN: {
		return;
	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	return;
}

unique_ptr<PhysicalOperator> LineageManager::AddProvenance(ClientContext &context, unique_ptr<PhysicalOperator> op) {
	if (trace_lineage && op->type != PhysicalOperatorType::TRANSACTION && op->type != PhysicalOperatorType::PRAGMA) {
		RecurseAddProvenance(context, op.get());

		AdjustPlan(context, op.get(), nullptr);

		// push a projection on top that project out the rowid before returning the final output
		vector<LogicalType> types;
		vector<unique_ptr<Expression>> expressions;
		for (storage_t column_id=0;  column_id < op->types.size(); column_id++) {
			auto col_type = op->types[column_id];
			types.push_back(col_type);
			expressions.push_back( make_uniq<BoundReferenceExpression>(col_type, column_id));
		}

		if (op->type == PhysicalOperatorType::HASH_JOIN
		     || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
		     || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
		     || op->type == PhysicalOperatorType::CROSS_PRODUCT
		     || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN) {
			// record the index of the rowid and remove it from types
			idx_t left_rowid = op->children[0]->types.size();
			types.erase(types.begin() + left_rowid-1);
		}

		// project out rowid
		types.pop_back();

		auto linop =
		    make_uniq<PhysicalLineage>(std::move(types), std::move(expressions), op->estimated_cardinality);

		if (op->type == PhysicalOperatorType::HASH_JOIN
		    || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
		    || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
		    || op->type == PhysicalOperatorType::CROSS_PRODUCT
		    || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN) {
			// record the index of the rowid and remove it from types
			idx_t left_rowid = op->children[0]->types.size();
			linop->drop_left = true;
			linop->left_annotation_index = left_rowid;
		}
		// root of the plan, drop annotations before returning results to user
		linop->drop_annotations = true;

		/*
		linop->drop_left = true;
		if ((op->type == PhysicalOperatorType::HASH_JOIN
		     || op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
		     || op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
		     || op->type == PhysicalOperatorType::CROSS_PRODUCT
		     || op->type == PhysicalOperatorType::NESTED_LOOP_JOIN
		     ) ) {
			linop->left_annotation_index = op->children[0]->types.size()-1;
		} else {
			// the index of the rowid to remove
			linop->left_annotation_index = linop->types.size();
		}*/

		linop->children.push_back(std::move(op));
		return linop;
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
