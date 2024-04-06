//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/operator_lineage.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include <iostream>
#include <utility>


namespace duckdb {
enum class PhysicalOperatorType : uint8_t;
class ChunkCollection;

class OperatorLineage {
public:
	OperatorLineage(Allocator &allocator, PhysicalOperatorType type, idx_t opid, bool trace_lineage) :
	      opid(opid), trace_lineage(trace_lineage), type(type), chunk_collection(allocator), cache_offset(0), cache_size(0), processed(false) {}

	vector<vector<ColumnDefinition>> GetTableColumnTypes();
	idx_t GetLineageAsChunk(DataChunk &insert_chunk,
	                        idx_t& global_count, idx_t& local_count,
	                        idx_t &thread_id, idx_t &data_idx,  bool &cache
	                        );
	
  	idx_t Size();
  	void InitLog(idx_t thread_id, PhysicalOperator*op= nullptr);

  	shared_ptr<Log> GetLog(idx_t thread_id) {
    	return log_per_thread[thread_id];
	  }
  	
    shared_ptr<Log> GetDefaultLog() {
    	return log_per_thread[thread_vec[0]];
	}

	void PostProcess();

	void Clear();

public:
  idx_t opid;
  bool trace_lineage;
  //! Type of the operator this lineage_op belongs to
  PhysicalOperatorType type;
  unordered_map<idx_t, shared_ptr<Log>> log_per_thread;
  shared_ptr<LogIndex> log_index;
  vector<idx_t> thread_vec;
  //! intermediate relation
  ChunkCollection chunk_collection;
  idx_t cache_offset;
  idx_t cache_size;
  //! Name of the scanned table if a scan
  string table_name;
  bool processed;
  mutex glock;

  vector<int> backward_lineage[2];
  vector<int> forward_lineage[2];
  std::unordered_map<int, std::vector<int>> forward_lineage_list[2];
};


} // namespace duckdb
#endif
