//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/operator_lineage.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/lineage/lineage_data.hpp"
#include "duckdb/parser/column_definition.hpp"
#include <iostream>
#include <utility>

// lineage_idx map to operators stages
#define LINEAGE_SOURCE 0
#define LINEAGE_SINK 1
#define LINEAGE_COMBINE 2
#define LINEAGE_FINALIZE 3

namespace duckdb {
enum class PhysicalOperatorType : uint8_t;
class LineageData;

class OperatorLineage {
public:
	OperatorLineage(Allocator &allocator, PhysicalOperatorType type, bool trace_lineage) :
	      trace_lineage(trace_lineage), type(type), chunk_collection(allocator), use_perfect_hash(false), cache_offset(0), cache_size(0) {
	}

	void Capture(const shared_ptr<LineageData>& datum, idx_t in_start, idx_t state_idx, idx_t thread_id);
	void Capture(shared_ptr<LogRecord> log_record, idx_t stage_idx, idx_t thread_id);
	void Capture(shared_ptr<vector<shared_ptr<LogRecord>>> log_record, idx_t stage_idx, idx_t thread_id);

	vector<vector<ColumnDefinition>> GetTableColumnTypes();
	idx_t GetLineageAsChunk(idx_t count_so_far, DataChunk &insert_chunk, idx_t thread_id, idx_t data_idx, idx_t stage_idx, bool &cache);
	idx_t Size();

public:
 	bool trace_lineage;
	//! Type of the operator this lineage_op belongs to
	PhysicalOperatorType type;
	//! 0: LINEAGE_SOURCE, 1: LINEAGE_SINK, 2: LINEAGE_COMBINE, 3: LINEAGE_FINALIZE
	unordered_map<idx_t, Log> log_per_thead;
	//! intermediate relation
	ChunkCollection chunk_collection;
	//! if the join hash table uses perfect hash
	bool use_perfect_hash;
	idx_t cache_offset;
	idx_t cache_size;
	//! Name of the scanned table if a scan
	string table_name;
};

} // namespace duckdb
#endif
