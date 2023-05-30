//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/operator_lineage.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once
//#include "duckdb/common/types/value.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/enums/join_type.hpp"

//#include "duckdb/common/types/chunk_collection.hpp"
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
struct LogRecord;
class LineageData;

class Log {
public:
	Log() {}

	void Append(shared_ptr<LogRecord> log_record, idx_t stage_idx) {
		D_ASSERT(stage_idx < 4);
		log[stage_idx].push_back(log_record);
	}

	idx_t GetLogSize(idx_t stage_idx) {
		D_ASSERT(stage_idx < 4);
		return log[stage_idx].size();
	}

	shared_ptr<LogRecord> GetLogRecord(idx_t stage_idx, idx_t data_idx) {
		D_ASSERT(stage_idx < 4 && data_idx < log[stage_idx].size());
		return log[stage_idx][data_idx];
	}

	idx_t GetLogSizeBytes();

	std::vector<shared_ptr<LogRecord>> log[4];
};

class OperatorLineage {
public:
	OperatorLineage(PhysicalOperatorType type, bool trace_lineage) :
	      trace_lineage(trace_lineage), type(type) {
	}

	void Capture(const shared_ptr<LineageData>& datum, idx_t in_start, idx_t state_idx, idx_t thread_id);
	void Capture(shared_ptr<LogRecord> log_record, idx_t stage_idx, idx_t thread_id);

	vector<vector<ColumnDefinition>> GetTableColumnTypes();
	idx_t GetLineageAsChunk(idx_t count_so_far, DataChunk &insert_chunk, idx_t thread_id, idx_t data_idx, idx_t stage_idx);
	idx_t Size();

public:
 	bool trace_lineage;
	PhysicalOperatorType type;
	//! 0: LINEAGE_SOURCE, 1: LINEAGE_SINK, 2: LINEAGE_COMBINE, 3: LINEAGE_FINALIZE
	unordered_map<idx_t, Log> log_per_thead;
};

struct LogRecord {
	shared_ptr<LineageData> data;
	idx_t in_start;
	LogRecord(shared_ptr<LineageData> data, idx_t in_start) :
	      data(data), in_start(in_start) {
	}
};

} // namespace duckdb
#endif
