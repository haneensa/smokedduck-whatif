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

#include <utility>

namespace duckdb {
class BoundReferenceExpression;

vector<idx_t> RecurseAddProvenance(PhysicalOperator *op) {
	vector<idx_t> annotations;

	// if child is hash join, then create projection and place it in between
	if (!op->special && op->children.size() == 1 && op->children[0]->type == PhysicalOperatorType::HASH_JOIN) {
		auto hj = op->children[0].get();

		// push a projection on top that does the projection
		vector<LogicalType> types;
		vector<unique_ptr<Expression>> expressions;
		for (storage_t column_id=0;  column_id < op->children[0]->types.size(); column_id++) {
			auto left_hj = hj->children[0].get();
			if (column_id == left_hj->types.size())
				continue;
			auto col_type = hj->types[column_id];
			types.push_back(col_type);
			expressions.push_back( make_uniq<BoundReferenceExpression>(col_type, column_id));
		}

		auto hj_unique = std::move(op->children[0]);
		// construct projection and stitch the plan again
		auto projection =
		    make_uniq<PhysicalProjection>(std::move(types), std::move(expressions), hj->estimated_cardinality);
		projection->children.push_back(std::move(hj_unique));
		projection->special = true;
		op->children[0] = std::move(projection);
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		annotations = RecurseAddProvenance(op->children[i].get());
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
		if (annotations.size() == 0) return {};

		op->types.push_back(LogicalTypeId::BIGINT);
		idx_t last = op->children[0]->types.size()-1;
		// adjust select list to reference on
		((PhysicalProjection* )op)->select_list.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, last));

		return { op->types.size() };
	}
	case PhysicalOperatorType::FILTER: {
		op->types.push_back(LogicalTypeId::BIGINT);
		return { op->types.size() };
	}
	case PhysicalOperatorType::HASH_JOIN: {
		idx_t left = op->children[0]->types.size();
		op->types.insert(op->types.begin() + left-1, LogicalTypeId::BIGINT);
		op->types.push_back(LogicalTypeId::BIGINT);
		((PhysicalHashJoin* )op)->build_types.push_back(LogicalTypeId::BIGINT);

		return { op->types.size() };
	}
	case PhysicalOperatorType::LIMIT: {
		break;
	}
	case PhysicalOperatorType::ORDER_BY: {
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		// output types
		op->types.push_back(LogicalType::LIST(LogicalType::BIGINT));
		((PhysicalHashAggregate* )op)->grouped_aggregate_data.payload_types.push_back(LogicalTypeId::BIGINT);
		//((PhysicalHashAggregate* )op)->grouped_aggregate_data.aggregates.push_back(LogicalTypeId::BIGINT);
		((PhysicalHashAggregate* )op)->grouped_aggregate_data.aggregate_return_types.push_back(LogicalType::LIST(LogicalTypeId::BIGINT));

	break;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		break;
	}
	case PhysicalOperatorType::INDEX_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN: {
		/*
		 *
#include "duckdb/planner/expression_iterator.hpp"

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

		// if left/right includes rowid dont add, else include it

		// find where in op.types ends for left side then add BIGINT
		op.types.insert(op.types.begin() + left->types.size()-1, LogicalTypeId::BIGINT);
		// same for right
		op.types.push_back(LogicalTypeId::BIGINT);
		// left_projection_map
		// right_projection_map

		// in condition, change index of element at  left->types.size() to  left->types.size()+1
		// recursively call children until we reach BoundReferenceExpression, then any index that reference elements from the right
		// add 1 to them
		GetColumnBindings(op.condition, left->types.size()-1);
		 */
	}
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	return {};
}

unique_ptr<PhysicalOperator> LineageManager::AddProvenance(unique_ptr<PhysicalOperator> op) {
	if (trace_lineage) {
		RecurseAddProvenance(op.get());

		// push a projection on top that does the projection
		vector<LogicalType> types;
		vector<unique_ptr<Expression>> expressions;
		for (storage_t column_id=0;  column_id < op->types.size(); column_id++) {
			auto col_type = op->types[column_id];
			types.push_back(col_type);
			expressions.push_back( make_uniq<BoundReferenceExpression>(col_type, column_id));
		}

		types.pop_back();
		expressions.pop_back();

		auto projection =
		    make_uniq<PhysicalProjection>(std::move(types), std::move(expressions), op->estimated_cardinality);
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
	CreateLineageTables(context, op, query_id);
}

} // namespace duckdb
#endif
