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
#include <iostream>
#include <utility>


namespace duckdb {
enum class PhysicalOperatorType : uint8_t;
class Log;

class OperatorLineage {
public:
	OperatorLineage(PhysicalOperatorType type, idx_t opid, bool trace_lineage) :
	      trace_lineage(trace_lineage), type(type), use_perfect_hash(false), cache_offset(0), cache_size(0) , opid(opid)
  {
	}

	vector<vector<ColumnDefinition>> GetTableColumnTypes();
	idx_t GetLineageAsChunk(idx_t count_so_far, DataChunk &insert_chunk,
      idx_t thread_id, idx_t data_idx, idx_t stage_idx, bool &cache);
	
  idx_t Size();
  void InitLog(idx_t thread_id);

  shared_ptr<Log> GetLog(idx_t thread_id) {
    return log_per_thread[thread_id];
  }

public:
  idx_t opid;
 	bool trace_lineage;
	//! Type of the operator this lineage_op belongs to
	PhysicalOperatorType type;
	unordered_map<idx_t, shared_ptr<Log>> log_per_thread;
	//! ensures we add the rowid column just once during the first time we read it and no more
	idx_t intermediate_chunk_processed_counter = 0;
	//! if the join hash table uses perfect hash
	bool use_perfect_hash;
	idx_t cache_offset;
	idx_t cache_size;
	//! Name of the scanned table if a scan
	string table_name;
};

class Log {
public:
	Log() {}

	idx_t GetLogSize(idx_t stage_idx) {
		return 0;
	}

	idx_t GetLogSizeBytes();
  virtual idx_t GetLatestLSN() {
    return 0;
  }
	
  virtual idx_t  GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
      DataChunk &insert_chunk, idx_t data_idx,
      idx_t &cache_offset, idx_t &cache_size, bool &cache) {return 0;};

	virtual idx_t Size() {return 0;};
	virtual idx_t Count() {return 0;};
	virtual idx_t ChunksCount() {return 0;};
	virtual void BuildIndexes() {};

  vector<std::pair<idx_t, idx_t>> output_index;
  vector<std::pair<idx_t, idx_t>> cached_output_index;
};

// TableScanLog
// 
struct scan_artifact {
  buffer_ptr<SelectionData> sel;
  uint32_t count;
  idx_t start;
  idx_t vector_index;
};


class TableScanLog : public Log {
  public:
  TableScanLog() {}

  idx_t GetLatestLSN() override {
    return lineage.size();
  }

  idx_t  GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
      DataChunk &insert_chunk, idx_t data_idx,
      idx_t &cache_offset, idx_t &cache_size, bool &cache) override;
    
  idx_t Size() override;
  idx_t Count() override;
  idx_t ChunksCount() override;
  void BuildIndexes() override;

public:
  vector<scan_artifact> lineage;
};

// FilterLineage
//
struct filter_artifact {
  unique_ptr<sel_t[]> sel;
 // buffer_ptr<SelectionData> sel;
  uint32_t count;
  idx_t child_offset;
};

class FilterLog : public Log {
public:
	FilterLog() {}
  
  idx_t GetLatestLSN() override {
    return lineage.size();
  }

  idx_t  GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
      DataChunk &insert_chunk, idx_t data_idx,
      idx_t &cache_offset, idx_t &cache_size, bool &cache) override;
    
  idx_t Size() override;
  idx_t Count() override;
  idx_t ChunksCount() override;
  void BuildIndexes() override;


  vector<filter_artifact> lineage;
};

// Ordrer By
class OrderByLog : public Log {
  public:
    OrderByLog() {}
  
  idx_t  GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
      DataChunk &insert_chunk, idx_t data_idx,
      idx_t &cache_offset, idx_t &cache_size, bool &cache) override;
    
    
  idx_t Size() override;
  idx_t Count() override;
  idx_t ChunksCount() override;
  void BuildIndexes() override;

public:
  vector<vector<idx_t>> lineage;
};

// Limit
//
struct limit_artifact {
  uint32_t start;
  uint32_t end;
  idx_t child_offset;
};

class LimitLog : public Log {
  public:
  LimitLog() {}

  idx_t  GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
      DataChunk &insert_chunk, idx_t data_idx,
      idx_t &cache_offset, idx_t &cache_size, bool &cache) override;
    
  idx_t Size() override;
  idx_t Count() override;
  idx_t ChunksCount() override;
  void BuildIndexes() override;

public:
  vector<limit_artifact> lineage;
};

// Cross Product Log
//
struct cross_artifact {
  // returns if the left side is scanned as a constant vector
  idx_t branch_scan_lhs;
  idx_t position_in_chunk;
  idx_t scan_position;
  idx_t count;
  idx_t out_start;
};


class CrossLog : public Log {
  public:
  CrossLog() {}
  
  idx_t GetLatestLSN() override {
    return lineage.size();
  }
  
  idx_t  GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
      DataChunk &insert_chunk, idx_t data_idx,
      idx_t &cache_offset, idx_t &cache_size, bool &cache) override;
    

public:
  vector<cross_artifact> lineage;
};

// NLJ Log
//
struct nlj_artifact {
  buffer_ptr<SelectionData> left;
  buffer_ptr<SelectionData> right;
  uint32_t count;
  idx_t current_row_index;
  idx_t out_start;
};

class NLJLog : public Log {
  public:
  NLJLog() {}
  
  idx_t  GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
      DataChunk &insert_chunk, idx_t data_idx,
      idx_t &cache_offset, idx_t &cache_size, bool &cache) override;

public:
  vector<nlj_artifact> lineage;
};

// BNLJ Log
//
struct bnlj_artifact {
  bool branch_scanlhs;
  buffer_ptr<SelectionData>  sel;
  uint32_t scan_position;
  uint32_t inchunk;
  uint32_t count;
  idx_t out_start;
};

class BNLJLog : public Log {
  public:
  BNLJLog() {}
  
  idx_t  GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
      DataChunk &insert_chunk, idx_t data_idx,
      idx_t &cache_offset, idx_t &cache_size, bool &cache) override;

public:
  vector<bnlj_artifact> lineage;
};

// Merge Log
//
struct merge_artifact {
  buffer_ptr<SelectionData> left;
  buffer_ptr<SelectionData> right;
  uint32_t count;
  uint32_t right_chunk_index;
  idx_t out_start;
};

class MergeLog : public Log {
  public:
  MergeLog() {}
  
  idx_t  GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
      DataChunk &insert_chunk, idx_t data_idx,
      idx_t &cache_offset, idx_t &cache_size, bool &cache) override;
    
public:
  vector<merge_artifact> lineage;
};

// Perfect Hash Join
//
struct pha_scan_artifact {
  unique_ptr<uint32_t[]> gather;
  uint32_t count;
};

class PHALog : public Log {
  public:
    PHALog() {}

public:
  vector<vector<uint32_t>> build_lineage;
  vector<pha_scan_artifact> scan_lineage;
};

struct hg_artifact {
  unique_ptr<data_ptr_t[]> addchunk_lineage;
  uint32_t count;
};

struct flushmove_artifact {
  unique_ptr<data_ptr_t[]> src;
  unique_ptr<data_ptr_t[]> sink;
  uint32_t count;
};

struct sink_artifact {
  uint32_t branch;
  idx_t lsn;
};

struct partition_artifact {
  uint32_t partition;
  flushmove_artifact* la;
};

struct radix_artifact {
  uint32_t partition;
  SelectionVector sel;
  uint32_t sel_size;
  hg_artifact* scatter;
};

struct finalize_artifact {
  uint32_t partition;
  vector<flushmove_artifact*>* combine;
};

class HALog : public Log {
  public:
    HALog() {}

public:
  vector<hg_artifact> addchunk_log;
  vector<sink_artifact> sink_log;
  vector<flushmove_artifact> flushmove_log;
  vector<partition_artifact> partition_log;
  vector<vector<radix_artifact>> radix_log;
  vector<vector<flushmove_artifact*>> combine_log;
  vector<finalize_artifact> finalize_log;
  vector<hg_artifact> scan_log;

  // context for current scan
  // TODO: state should be passed and manageed by
  // lineage scan
  idx_t scan_log_index=0;
  idx_t current_key=0;
  idx_t offset_within_key=0;
};

// Hash Join Lineage
//
struct hj_probe_artifact {
  unique_ptr<sel_t[]> left;
  unique_ptr<data_ptr_t[]> right;
  uint32_t count;
  idx_t out_offset;
};

struct hj_build_artifact {
  buffer_ptr<SelectionData> sel;
  idx_t added_count;
  idx_t keys_size;
  unique_ptr<data_ptr_t[]> scatter;
  idx_t in_start;
};

struct hj_finalize_artifact {
  buffer_ptr<SelectionData> sel;
  idx_t added_count;
  unique_ptr<data_ptr_t[]> scatter;
};

class HashJoinLog : public Log {
public:
  HashJoinLog() {}
  
  idx_t  GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
      DataChunk &insert_chunk, idx_t data_idx,
      idx_t &cache_offset, idx_t &cache_size, bool &cache) override;
  
  idx_t Size() override;
  idx_t Count() override;
  idx_t ChunksCount() override;
  void BuildIndexes() override;

  idx_t GetBuildSideIndex(idx_t cur);


public:
  vector<hj_build_artifact> lineage_build;
  vector<hj_finalize_artifact> lineage_finalize;

  vector<hj_probe_artifact> lineage_binary;
  vector<unique_ptr<sel_t[]>> right_val_log;
	
  // Specialized Index
  vector<idx_t> hash_chunk_count;
	// hm_range: maintains the existing ranges in hash join build side
	std::vector<std::pair<idx_t, idx_t>> hm_range;
  // offset: difference between two consecutive values with a range
	uint64_t offset = 0;
	idx_t start_base = 0;
	idx_t last_base = 0;
};



} // namespace duckdb
#endif
