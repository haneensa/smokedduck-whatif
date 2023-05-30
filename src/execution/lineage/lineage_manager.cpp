#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"

#include <utility>

namespace duckdb {

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
