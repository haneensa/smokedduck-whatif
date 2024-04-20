//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/fade.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once
#include "duckdb/execution/physical_operator.hpp"

#include <immintrin.h>
#include <random>
#include <queue>
#include <cmath>


namespace duckdb {
class PhysicalOperator;

// holds allocated data per node
// interventions, annotations, etc
struct FadeDataPerNode {
	int* annotations;
	idx_t n_interventions;
	std::set<int> del_set;
	idx_t n_masks;
	idx_t n_groups;
	// single_del_intervention.size() == input table size
	// bits: del or not
	__mmask16* del_interventions;
	__mmask16* base_target_matrix;
  int base_rows;
	int8_t* single_del_interventions;
	// this should be per attribute
	// unordered_map<string, vector<idx_t>>
	// vector<idx_t> single_scale_intervention;
  int opid;
	std::unordered_map<string, vector<void*>> alloc_vars;
	std::unordered_map<string, string> alloc_vars_types;
	std::unordered_map<string, int> alloc_vars_index;
	std::unordered_map<int, void*> input_data_map;
	int (*filter_fn)(int, int*, void*, void*, std::set<int>&, std::set<int>&);
	int (*join_fn)(int, int*, int*, void*, void*, void*, std::set<int>&,  std::set<int>&, std::set<int>&);
	int (*join_fn_forward)(int, std::unordered_map<int, std::vector<int>>&,
      std::unordered_map<int, std::vector<int>>&, void*, void*, void*, std::set<int>&,  std::set<int>&, std::set<int>&);
	int (*agg_duckdb_fn)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&, ChunkCollection&, std::set<int>&);
	int (*agg_fn)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,  std::unordered_map<int, void*>&, std::set<int>&);
	int (*agg_fn_nested)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,
	                     std::unordered_map<std::string, vector<void*>>&, std::set<int>&);
	bool has_agg_child;
	int child_agg_id;
};

enum InterventionType {
	DENSE_DELETE,
	SCALE_UNIFORM,
	SCALE_RANDOM,
	SEARCH
};

struct EvalConfig {
	int batch;
	int mask_size;
	bool is_scalar;
	bool use_duckdb;
	bool debug;
	bool prune;
	bool incremental;
	string columns_spec_str;
	InterventionType intervention_type;
	int n_intervention;
	int qid;
	int num_worker;
	float probability;
	int topk;
};

class Fade {
public:
	Fade() {};

	template<class T>
	static void allocate_agg_output(string typ, int t, int n_groups, int n_interventions, string out_var, PhysicalOperator* op,
	                         std::unordered_map<idx_t, FadeDataPerNode>& fade_data);

	static string PrepareLineage(PhysicalOperator *op, bool prune, bool forward_lineage);
  static std::vector<int> rank(PhysicalOperator* op, EvalConfig& config, std::unordered_map<idx_t, FadeDataPerNode>& fade_data);

	static string Whatif(PhysicalOperator* op, EvalConfig config);
	static string PredicateSearch(PhysicalOperator* op, EvalConfig config);

	template<class T1, class T2>
	static T2* GetInputVals(PhysicalOperator* op, shared_ptr<OperatorLineage> lop, idx_t col_idx);

	static string get_header(EvalConfig config);
	static string get_agg_alloc(int fid, string fn, string out_type);
	static string get_agg_finalize(EvalConfig config, FadeDataPerNode& node_data);
	static string group_partitions(EvalConfig config, FadeDataPerNode& node_data);

	static void* compile(std::string code, int id);

	static std::unordered_map<std::string, std::vector<std::string>> parseSpec(EvalConfig& config);

	template <class T>
	static void PrintOutput(FadeDataPerNode& info, T* data_ptr);

	static void GetLineage(PhysicalOperator* op);

	static void FillFilterBackwardLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop);
	static void FillJoinBackwardLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop);
	static void FillGBForwardLineage(shared_ptr<OperatorLineage> lop, int row_count);
	static void FillForwardLineage(PhysicalOperator* op, bool prune);

	static int PruneUtilization(PhysicalOperator* op, int M, int side);

	static void PruneLineage(PhysicalOperator* op, vector<int>& out_order);

	static void ReleaseFade(EvalConfig& config, void* handle, PhysicalOperator* op,
	                 std::unordered_map<idx_t, FadeDataPerNode>& fade_data);

	static void GroupByAlloc(EvalConfig& config, shared_ptr<OperatorLineage> lop,
	                         std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
	                         PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
	                         int keys, int n_groups);

	static int* random_unique(shared_ptr<OperatorLineage> lop, idx_t distinct);
	//static std::pair<int*, int> factorize(PhysicalOperator* op, shared_ptr<OperatorLineage> lop,
	//                                      std::unordered_map<std::string, std::vector<std::string>>& columns_spec);

	static void BindFunctions(EvalConfig& config, void* handle, PhysicalOperator* op,
	                   std::unordered_map<idx_t, FadeDataPerNode>& fade_data);
  static void* get_common_functions();
  static std::pair<int*, int> factorize(PhysicalOperator* op, shared_ptr<OperatorLineage> lop,
                                      std::unordered_map<std::string, std::vector<std::string>>& columns_spec);
};

} // namespace duckdb
#endif

