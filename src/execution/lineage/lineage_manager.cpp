#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <utility>

namespace duckdb {
class PhysicalDelimJoin;

void LineageManager::CreateOperatorLineage(ClientContext &context,
    PhysicalOperator *op, 
    bool trace_lineage) {
	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		auto distinct = (PhysicalOperator*)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get();
		CreateOperatorLineage(context, distinct, trace_lineage);
		for (idx_t i = 0; i < dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans.size(); ++i) {
			// dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans[i]->lineage_op = distinct->lineage_op;
		}
		CreateOperatorLineage(context, dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), trace_lineage);
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		CreateOperatorLineage(context, op->children[i].get(), trace_lineage);
	}
  if (op->lineage_op == nullptr) {
	  op->lineage_op = make_shared<OperatorLineage>(Allocator::Get(context),
        op->type, 
        op->id,
        trace_lineage);
  }

	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		string table_str = dynamic_cast<PhysicalTableScan *>(op)->ParamsToString();
		// TODO there's probably a better way to do this...
		op->lineage_op->table_name = table_str.substr(0, table_str.find('\n'));
	}
}

// Iterate through in Postorder to ensure that children have PipelineLineageNodes set before parents
idx_t PlanAnnotator(PhysicalOperator *op, idx_t counter) {
	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		counter = PlanAnnotator( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), counter);
		counter = PlanAnnotator((PhysicalOperator*) dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), counter);
	}
	for (idx_t i = 0; i < op->children.size(); i++) {
		counter = PlanAnnotator(op->children[i].get(), counter);
	}
	op->id = counter;
	return counter + 1;
}

void LineageManager::InitOperatorPlan(ClientContext &context, PhysicalOperator *op) {
	if (op->type == PhysicalOperatorType::EXECUTE) {
		InitOperatorPlan(context, &dynamic_cast<PhysicalExecute*>(op)->plan);
	}
	PlanAnnotator(op, 0);
	CreateOperatorLineage(context, op, trace_lineage);
}

void LineageManager::CreateLineageTables(ClientContext &context, PhysicalOperator *op, idx_t query_id) {
	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		CreateLineageTables( context, dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), query_id);
		CreateLineageTables(context, (PhysicalOperator*) dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), query_id);
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		CreateLineageTables(context, op->children[i].get(), query_id);
	}

	// Example: LINEAGE_1_HASH_JOIN_3
	string prefix = "LINEAGE_" + to_string(query_id) + "_" + op->GetName() ;
	prefix.erase( remove( prefix.begin(), prefix.end(), ' ' ), prefix.end() );
	// add column_stats, cardinality
	string catalog_name = "";
	auto binder = Binder::CreateBinder(context);
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	auto table_column_types = op->lineage_op->GetTableColumnTypes();
	if (op->type != PhysicalOperatorType::PROJECTION) {
    for (idx_t i = 0; i < table_column_types.size(); i++) {
      if (table_column_types[i].size() == 0) continue;

      // Example: LINEAGE_1_HASH_JOIN_3_0
      string table_name = prefix + "_" + to_string(i);
      // Create Table
      auto create_info = make_uniq<CreateTableInfo>(catalog_name, DEFAULT_SCHEMA, table_name);
      for (idx_t col_i = 0; col_i < table_column_types[i].size(); col_i++) {
        create_info->columns.AddColumn(move(table_column_types[i][col_i]));
      }
      table_lineage_op[table_name] = op->lineage_op;
      DuckTableEntry* table = (DuckTableEntry*)catalog.CreateTable(context, move(create_info)).get();
    }
  }

	// persist intermediate values
	if (persist_intermediate || CheckIfShouldPersistForKSemimodule(op)) {
		vector<ColumnDefinition> table;
		for (idx_t col_i = 0; col_i < op->types.size(); col_i++) {
			table.emplace_back("col_" + to_string(col_i), op->types[col_i]);
		}

		string table_name = "LINEAGE_" + to_string(query_id) + "_"  + op->GetName() + "_100";
		auto create_info = make_uniq<CreateTableInfo>(catalog_name, DEFAULT_SCHEMA, table_name);
		for (idx_t col_i = 0; col_i < table.size(); col_i++) {
			create_info->columns.AddColumn(move(table[col_i]));
		}

		catalog.CreateTable(context, move(create_info)).get();
		table_lineage_op[table_name] = op->lineage_op;
	}
}

void LineageManager::StoreQueryLineage(ClientContext &context, unique_ptr<PhysicalOperator> op, string query) {
	if (!trace_lineage)
		return;

	idx_t query_id = query_to_id.size();
	query_to_id.push_back(query);
	CreateLineageTables(context, op.get(), query_id);
	queryid_to_plan[query_id] = move(op);
}

bool LineageManager::CheckIfShouldPersistForKSemimodule(PhysicalOperator *op) {
	return persist_k_semimodule && op->child_of_aggregate;
}


} // namespace duckdb
#endif
