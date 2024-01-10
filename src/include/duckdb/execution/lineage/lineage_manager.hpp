//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once
#include "duckdb/common/types/value.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "operator_lineage.hpp"

#include <iostream>
#include <utility>

#ifndef QUERY_LIST_TABLE_NAME
#define QUERY_LIST_TABLE_NAME "queries_list"
#endif

namespace duckdb {
class ClientContext;
class PhysicalOperator;


class LineageManager {
public:
	LineageManager() :trace_lineage(false), persist_intermediate(false), persist_k_semimodule(false)  {};
//	~LineageManager() {}

	//! 1. call PlanAnnotator: For each operator in the plan, give it an ID. If there are
	//! two operators with the same type, give them a unique ID starting
	//! from the zero and incrementing it for the lowest levels of the tree
	//! 2.  call CreateOperatorLineage to allocate lineage_op for main thread
	//! TODO: understand multi-threading and support it
	void InitOperatorPlan(ClientContext &context, PhysicalOperator *op);
	void CreateOperatorLineage(ClientContext &context, PhysicalOperator *op, bool trace_lineage);
	void CreateLineageTables(ClientContext &context, PhysicalOperator *op, idx_t query_id);
	void StoreQueryLineage(ClientContext &context, unique_ptr<PhysicalOperator> op, string query);
	bool CheckIfShouldPersistForKSemimodule(PhysicalOperator *op);

	void SetCurrentLineageOp(shared_ptr<OperatorLineage> lop) {
		current_lop = lop;
	}

	shared_ptr<OperatorLineage> GetCurrentLineageOp() {
		return current_lop;
	}

	void Why(int qid, int k, string columns_spec, int distinct);

private:
	//! cached operator lineage to be accessed from function calls that don't have access to operator members
	shared_ptr<OperatorLineage> current_lop;

public:
	//! Whether or not lineage is currently being captured
	bool trace_lineage;
	//! if set then Persist all intermediate chunks in a query tree
	bool persist_intermediate;
	//! if set then persist intermediate chunks just for aggregates (K-Semimodule data)
	bool persist_k_semimodule;
	//! map between lineage relational table name and its in-mem lineage
	unordered_map<string, shared_ptr<OperatorLineage>> table_lineage_op;
	vector<string> query_to_id;
	//! in_memory storage of physical query plan per query
	std::unordered_map<idx_t, unique_ptr<PhysicalOperator>> queryid_to_plan;
};

} // namespace duckdb
#endif
