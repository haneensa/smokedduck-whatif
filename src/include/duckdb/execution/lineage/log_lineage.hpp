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
#include <iostream>
#include <utility>


namespace duckdb {
enum class PhysicalOperatorType : uint8_t;
class Log;
class LogIndex;

class LogIndex {
public:
	LogIndex() {}

	// PHA: Specialized Index
	unordered_map<uint32_t, vector<idx_t>> pha_hash_index;


	// HA: Specialized Index
	unordered_map<data_ptr_t, vector<idx_t>> ha_hash_index;
	unordered_map<data_ptr_t, vector<idx_t>> ha_distinct_hash_index;
	unordered_map<idx_t, idx_t> distinct_count;
	unordered_map<idx_t, idx_t> grouping_set_count;


	// HJ: Specialized Index
	unordered_map<data_ptr_t, idx_t> hj_hash_index;
	unordered_map<idx_t, data_ptr_t> perfect_hash_join_finalize;
	unordered_map<idx_t, vector<unique_ptr<sel_t[]>>> right_val_log;
	idx_t arraySize = 0;
	vector<idx_t> hj_array;
	vector<vector<std::pair<idx_t, data_ptr_t>>> index_hj;

	// Merge Join
	vector<idx_t> sort;
};

class Log {
public:
	Log(idx_t thid) : thid(thid), processed(false)  {}
  	virtual idx_t GetLatestLSN() { return 0; };
  	virtual idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                                idx_t& global_count, idx_t& local_count,
	                                idx_t& data_idx,
	                                idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                                shared_ptr<LogIndex> logIdx) {return 0;};

	virtual idx_t Size() { return output_index.size() * 2 * sizeof(idx_t); };
	virtual idx_t Count() { return 0; };
	virtual idx_t ChunksCount() { return output_index.size(); };
	virtual void BuildIndexes(shared_ptr<LogIndex> logIdx) {};
	virtual void PostProcess(shared_ptr<LogIndex> logIdx) {};

	virtual ~Log() {
		// Virtual destructor in the base class
	}
public:
	idx_t thid;
	vector<std::pair<idx_t, idx_t>> output_index;
	vector<std::pair<idx_t, idx_t>> cached_output_index;
  	bool processed;
};

// TableScanLog
// 
struct scan_artifact {
  buffer_ptr<SelectionData> sel;
  idx_t count;
  idx_t start;
  idx_t vector_index;
};

class TableScanLog : public Log {
  public:
  TableScanLog(idx_t thid) : Log(thid) {}

  idx_t GetLatestLSN() override {
    return lineage.size();
  }

  idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                      idx_t& global_count, idx_t& local_count,
	                      idx_t& data_idx,
	                      idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                      shared_ptr<LogIndex> logIdx) override;
    
  idx_t Size() override;
  idx_t Count() override;
  idx_t ChunksCount() override;
  void BuildIndexes(shared_ptr<LogIndex> logIdx) override;
  void PostProcess(shared_ptr<LogIndex> logIdx) override;

public:
  vector<scan_artifact> lineage;
};

// FilterLineage
//
struct filter_artifact {
  unique_ptr<sel_t[]> sel;
 // buffer_ptr<SelectionData> sel;
  idx_t count;
  idx_t child_offset;
};

class FilterLog : public Log {
public:
	FilterLog(idx_t thid) : Log(thid)  {}
  
  idx_t GetLatestLSN() override {
    return lineage.size();
  }

  idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                      idx_t& global_count, idx_t& local_count,
	                      idx_t& data_idx,
	                      idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                      shared_ptr<LogIndex> logIdx) override;
    
  idx_t Size() override;
  idx_t Count() override;
  idx_t ChunksCount() override;
  void BuildIndexes(shared_ptr<LogIndex> logIdx) override;
  void PostProcess(shared_ptr<LogIndex> logIdx) override;

  vector<filter_artifact> lineage;
};

// Ordrer By
class OrderByLog : public Log {
  public:
    OrderByLog(idx_t thid) : Log(thid)  {}
  
  idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                        idx_t& global_count, idx_t& local_count,
	                        idx_t& data_idx,
	                        idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                        shared_ptr<LogIndex> logIdx) override;
    
    
  idx_t Size() override;
  idx_t Count() override;
  idx_t ChunksCount() override;

public:
  vector<vector<idx_t>> lineage;
};

// Limit
//
struct limit_artifact {
  idx_t start;
  idx_t end;
  idx_t child_offset;
};

class LimitLog : public Log {
  public:
  LimitLog(idx_t thid) : Log(thid)  {}

  idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                      idx_t& global_count, idx_t& local_count,
	                      idx_t& data_idx,
	                      idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                      shared_ptr<LogIndex> logIdx) override;
    
  idx_t Size() override;
  idx_t Count() override;
  idx_t ChunksCount() override;
  void BuildIndexes(shared_ptr<LogIndex> logIdx) override;

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
  CrossLog(idx_t thid) : Log(thid)  {}
  
  idx_t GetLatestLSN() override {
    return lineage.size();
  }
  
  idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                      idx_t& global_count, idx_t& local_count,
	                      idx_t& data_idx,
	                      idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                      shared_ptr<LogIndex> logIdx) override;
    

public:
  vector<cross_artifact> lineage;
};


// NLJ Log
//
struct join_artifact {
  buffer_ptr<SelectionData> left;
  idx_t count;
};

class SharedJoinLog : public Log {
  public:
	  SharedJoinLog(idx_t thid) : Log(thid)  {}

  public:
  vector<join_artifact> shared_lineage;
};

// NLJ Log
//
struct nlj_artifact {
  buffer_ptr<SelectionData> left;
  buffer_ptr<SelectionData> right;
  idx_t count;
  idx_t current_row_index;
  idx_t out_start;
};

class NLJLog : public SharedJoinLog {
  public:
  NLJLog(idx_t thid) : SharedJoinLog(thid)  {}

  idx_t GetLatestLSN() override {
	return lineage.size();
  }

  idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                      idx_t& global_count, idx_t& local_count,
	                      idx_t& data_idx,
	                      idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                      shared_ptr<LogIndex> logIdx) override;
  void PostProcess(shared_ptr<LogIndex> logIdx) override;
public:
  vector<nlj_artifact> lineage;
};

// BNLJ Log
//
struct bnlj_artifact {
  bool branch_scanlhs;
  buffer_ptr<SelectionData>  sel;
  idx_t scan_position;
  idx_t inchunk;
  idx_t count;
  idx_t out_start;
  idx_t branch;
};

class BNLJLog : public SharedJoinLog {
  public:
  BNLJLog(idx_t thid) : SharedJoinLog(thid)  {}

  idx_t GetLatestLSN() override {
	return lineage.size();
  }

  idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                      idx_t& global_count, idx_t& local_count,
	                      idx_t& data_idx,
	                      idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                      shared_ptr<LogIndex> logIdx) override;

  void PostProcess(shared_ptr<LogIndex> logIdx) override;

public:
  vector<bnlj_artifact> lineage;
};

// Merge Log
//
struct merge_artifact {
  buffer_ptr<SelectionData> left;
  vector<vector<idx_t>> lhs_sort;
  buffer_ptr<SelectionData> right;
  idx_t count;
  idx_t right_chunk_index;
  idx_t out_start;
  idx_t branch;
};

class MergeLog : public SharedJoinLog {
  public:
  MergeLog(idx_t thid) : SharedJoinLog(thid)  {}

  idx_t GetLatestLSN() override {
	return lineage.size();
  }

  idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                      idx_t& global_count, idx_t& local_count,
	                      idx_t& data_idx,
	                      idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                      shared_ptr<LogIndex> logIdx) override;
  void BuildIndexes(shared_ptr<LogIndex> logIdx) override;
  void PostProcess(shared_ptr<LogIndex> logIdx) override;

public:
  vector<merge_artifact> lineage;
  vector<vector<idx_t>> combine;
};

// Perfect Hash Join
//
struct pha_scan_artifact {
  unique_ptr<uint32_t[]> gather;
  idx_t count;
};

class PHALog : public Log {
  public:
    PHALog(idx_t thid) : Log(thid)  {}

	idx_t GetLatestLSN() override {
		return scan_lineage.size();
	}

	idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                        idx_t& global_count, idx_t& local_count,
	                        idx_t& data_idx,
	                        idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                        shared_ptr<LogIndex> logIdx) override;

	idx_t Size() override;
	idx_t Count() override;
	idx_t ChunksCount() override;
	void BuildIndexes(shared_ptr<LogIndex> logIdx) override;

public:
  vector<vector<uint32_t>> build_lineage;
  vector<pha_scan_artifact> scan_lineage;

  idx_t scan_log_index=0;
  idx_t current_key=0;
  idx_t key_offset=0;
  idx_t offset_within_key=0;
};

struct hg_artifact {
  unique_ptr<data_ptr_t[]> addchunk_lineage;
  idx_t count;
};

struct flushmove_artifact {
  unique_ptr<data_ptr_t[]> src;
  unique_ptr<data_ptr_t[]> sink;
  idx_t count;
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
  idx_t sel_size;
  hg_artifact* scatter;
};

struct finalize_artifact {
  uint32_t partition;
  vector<flushmove_artifact*>* combine;
};

class HALog : public Log {
  public:
    HALog(idx_t thid) : Log(thid)  {}

	idx_t GetLatestLSN() override {
		return addchunk_log.size();
	}

	idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                        idx_t& global_count, idx_t& local_count,
	                        idx_t& data_idx,
	                        idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                        shared_ptr<LogIndex> logIdx) override;

	idx_t Size() override;
	idx_t Count() override;
	idx_t ChunksCount() override;
	void BuildIndexes(shared_ptr<LogIndex> logIdx) override;
	void PostProcess(shared_ptr<LogIndex> logIdx) override;

public:
  vector<hg_artifact> addchunk_log;
  vector<sink_artifact> sink_log;
  vector<flushmove_artifact> flushmove_log;
  vector<partition_artifact> partition_log;
  vector<vector<radix_artifact>> radix_log;
  vector<vector<flushmove_artifact*>> combine_log;
  vector<finalize_artifact> finalize_log;
  vector<hg_artifact> scan_log;

  unordered_map<idx_t, vector<idx_t>> distinct_index;
  unordered_map<idx_t, vector<idx_t>> distinct_scan;
  unordered_map<idx_t, vector<idx_t>> distinct_sink;

  unordered_map<idx_t, vector<idx_t>> grouping_set;


  // context for current scan
  // TODO: state should be passed and manageed by
  // lineage scan
  idx_t scan_log_index=0;
  idx_t current_key=0;
  idx_t offset_within_key=0;
  idx_t key_offset=0;
};

// Hash Join Lineage
//
struct hj_probe_artifact {
  unique_ptr<sel_t[]> left;
  unique_ptr<data_ptr_t[]> right;
  unique_ptr<sel_t[]> perfect_right;
  // 0: inner join
  // 1: perfect hash table
  // 2: semi/mark join
  idx_t branch;
  idx_t count;
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
  HashJoinLog(idx_t thid) : Log(thid)  {}


  idx_t GetLatestLSN() override {
	return lineage_binary.size();
  }

  idx_t  GetLineageAsChunk(DataChunk &insert_chunk,
	                      idx_t& global_count, idx_t& local_count,
	                      idx_t& data_idx,
	                      idx_t &cache_offset, idx_t &cache_size, bool &cache,
	                      shared_ptr<LogIndex> logIdx) override;
  
  idx_t Size() override;
  idx_t Count() override;
  idx_t ChunksCount() override;
  void BuildIndexes(shared_ptr<LogIndex> logIdx) override;
  void PostProcess(shared_ptr<LogIndex> logIdx) override;

public:
  vector<hj_build_artifact> lineage_build;
  vector<hj_finalize_artifact> lineage_finalize;

  vector<hj_probe_artifact> lineage_binary;
};

// Sampling Log
//
struct sample_artifact {
  buffer_ptr<SelectionData> sel;
  idx_t count;
  idx_t branch;
};

class SamplingLog : public Log {
  public:
	  SamplingLog(idx_t thid) : Log(thid)  {}

  public:
  vector<sample_artifact> lineage;
};
} // namespace duckdb
#endif
