//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/fade.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once
#include "duckdb/execution/lineage/lineage_manager.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <immintrin.h>
#include <random>
#include <queue>
#include <cmath>
#include <condition_variable>
#include <mutex>

namespace duckdb {
class PhysicalOperator;
class FadeNode;
struct EvalConfig;

extern int (*fade_random_fn)(int, int, float, int, void*, int, std::vector<__mmask16>&);
extern unique_ptr<FadeNode> global_fade_node;


class FadeNode {
public:
	FadeNode(int opid, int n_interventions, int num_worker, int rows, bool debug)
	    : debug(debug), opid(opid), n_interventions(n_interventions),
	      num_worker(num_worker), rows(rows), n_groups(0), child_agg_id(-1), has_agg_child(false), counter(0) {};

	void GroupByAlloc(bool debug, PhysicalOperatorType typ, shared_ptr<OperatorLineage> lop,
	                  PhysicalOperator* op);

	void LocalGroupByAlloc(bool debug, shared_ptr<OperatorLineage> lop,
	                       PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
	                       int keys_size);

	void GroupByGetCachedData(EvalConfig& config, shared_ptr<OperatorLineage> lop,
	                                 PhysicalOperator* op, vector<unique_ptr<Expression>>& aggregates,
	                                 int keys);

	template<class T>
	void allocate_agg_output(string typ, int t, int n_interventions, string out_var) ;

	template <class T>
	void PrintOutput(T* data_ptr);

	virtual ~FadeNode() {
		if (!alloc_vars.empty())
			std::cout << "Print Output for " << n_interventions << " interventions and " << n_groups << " groups." << std::endl;
		for (auto &pair : alloc_vars) {
			if (!pair.second.empty()) {
				if (debug) {
					std::cout << "Print out results for " << pair.first << std::endl;
					if (alloc_vars_types[pair.first] == "int") {
						PrintOutput<int>((int *)pair.second[0]);
					} else if (alloc_vars_types[pair.first] == "float") {
						PrintOutput<float>((float *)pair.second[0]);
					}
				}
				for (int t = 0; t < pair.second.size(); t++) {
					free(pair.second[t]);
					pair.second[t] = nullptr;
				}
			}
		}
		for (auto &pair : input_data_map) {
			if (pair.second != nullptr) {
				free(pair.second);
				pair.second = nullptr;
			}
		}
	};

public:
	bool debug;
	int opid;
	int n_interventions;
	int num_worker;
	int rows;
	int n_groups;
	int child_agg_id;
	bool has_agg_child;

	int counter;
	std::mutex mtx;
	std::condition_variable cv;

	std::unordered_map<string, vector<void*>> alloc_vars;
	std::unordered_map<int, void*> input_data_map;
	std::unordered_map<string, string> alloc_vars_types;
	std::unordered_map<string, int> alloc_vars_index;
	std::unordered_map<string, string> alloc_vars_funcs;
};

class FadeCompile {
public:
	FadeCompile() : gen(false) {};
	virtual ~FadeCompile() = default; // Virtual destructor
public:
	bool gen;
	int (*filter_fn)(int, int*, void*, void*, const int, const int);
	int (*join_fn)(int, int*, int*, void*, void*, void*, const int, const int);
	int (*agg_duckdb_fn)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&, ChunkCollection&, const int, const int);
	int (*agg_fn)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,  std::unordered_map<int, void*>&, const int, const int);
	int (*agg_fn_bw)(int, std::vector<std::vector<int>>&, void*, std::unordered_map<std::string, vector<void*>>&,
	                 std::unordered_map<int, void*>&);
	int (*agg_fn_nested)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,
	                     std::unordered_map<std::string, vector<void*>>&, const int, const int);
};


class FadeNodeSingle: public FadeNode {
public:
	FadeNodeSingle(int opid, int n_interventions, int num_worker, int rows, bool debug)
	    : FadeNode(opid, n_interventions, num_worker, rows, debug) {};

	virtual ~FadeNodeSingle() = default; // Virtual destructor

public:
	int8_t* single_del_interventions;
	int8_t* base_single_del_interventions;
};

class FadeNodeDense: public FadeNode {
public:
	FadeNodeDense(int opid, int n_interventions, int num_worker, int rows, bool debug)
	    : FadeNode(opid, n_interventions, num_worker, rows, debug), n_masks(0) {};

	bool read_dense_from_file(bool debug, string col_spec, string table_name);
	void debug_dense_matrix();

	virtual ~FadeNodeDense() = default; // Virtual destructor

public:
	idx_t n_masks;
	__mmask16* del_interventions;
	__mmask16* base_target_matrix;
};


class FadeSparseNode : public FadeNode {
public:
	FadeSparseNode(int opid, int n_interventions, int num_worker, int rows, bool debug)
	    : FadeNode(opid, n_interventions, num_worker, rows, debug) {};

	bool read_annotations(int n_interventions, int rows, string& table_name, string& col_spec, bool debug);
public:
	unique_ptr<int[]> annotations;
	unique_ptr<int[]> base_annotations;
};

class FadeNodeSparseCompile : public FadeSparseNode, public FadeCompile {
public:
	FadeNodeSparseCompile(int opid, int n_interventions, int num_worker, int rows, bool debug)
	    : FadeSparseNode(opid, n_interventions, num_worker, rows, debug), FadeCompile() {};
};

class FadeNodeDenseCompile : public FadeNodeDense, public FadeCompile {
public:
	FadeNodeDenseCompile(int opid, int n_interventions, int num_worker, int rows, bool debug)
	    : FadeNodeDense(opid, n_interventions, num_worker, rows, debug), FadeCompile() {};
};

class FadeNodeSingleCompile : public FadeNodeSingle, public FadeCompile {
public:
	FadeNodeSingleCompile(int opid, int n_interventions, int num_worker, int rows, bool debug)
	    : FadeNodeSingle(opid, n_interventions, num_worker, rows, debug), FadeCompile() {};
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
	bool use_gb_backward_lineage;
	bool use_preprep_tm;
	int rand_count;
	std::vector<__mmask16> rand_base;
};

class Fade {
public:
	Fade() {};

  	static int LineageMemory(PhysicalOperator* op);
	static string PrepareLineage(PhysicalOperator *op, bool prune, bool forward_lineage, bool use_gb_backward_lineage);
  	static std::vector<int> rank(PhysicalOperator* op, EvalConfig& config, std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data);
	static string WhatifDense(PhysicalOperator* op, EvalConfig config);
	static string WhatifDenseCompile(PhysicalOperator* op, EvalConfig config);
	static string Whatif(PhysicalOperator* op, EvalConfig config);
	static string PredicateSearch(PhysicalOperator* op, EvalConfig config);
	static string WhatIfSparse(PhysicalOperator* op, EvalConfig config);

	static void Intervention2DEval(int thread_id, EvalConfig& config, PhysicalOperator* op,
	                        std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
	                        bool use_compiled);
	static void Clear(PhysicalOperator *op);

	template<class T1, class T2>
	static T2* GetInputVals(PhysicalOperator* op, idx_t col_idx);

	static string get_header(EvalConfig config);
	static string get_agg_alloc(EvalConfig& config, int fid, string fn, string out_type);
	static string get_agg_finalize(EvalConfig config, unique_ptr<FadeNode>& node_data);
	static string group_partitions(EvalConfig config, int n_groups,
	                               std::unordered_map<string, vector<void*>>& alloc_vars,
	                               std::unordered_map<string, int>& alloc_vars_index,
	                               std::unordered_map<string, string>& alloc_vars_types);

	static void* compile(std::string code, int id);

	static std::unordered_map<std::string, std::vector<std::string>> parseSpec(EvalConfig& config);

	static void GetLineage(PhysicalOperator* op, bool use_gb_backward_lineage);
	static pair<int, int> get_start_end(int row_count, int thread_id, int num_worker);
	static void FillFilterBackwardLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop);
	static void FillJoinBackwardLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop);
	static void FillGBForwardLineage(shared_ptr<OperatorLineage> lop, int row_count);
	static void FillForwardLineage(PhysicalOperator* op, bool prune);
	static void FillGBBackwardLineage(shared_ptr<OperatorLineage> lop, int row_count);

	static int PruneUtilization(PhysicalOperator* op, int M, int side);

	static void PruneLineage(PhysicalOperator* op, vector<int>& out_order);

	static void GetCachedData(EvalConfig& config, PhysicalOperator* op,
							  std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
							  std::unordered_map<std::string, std::vector<std::string>>& spec);
	static void GenRandomWhatifIntervention(int thread_id, EvalConfig& config, PhysicalOperator* op,
	                                       std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
	                                       std::unordered_map<std::string, std::vector<std::string>>& spec,
	                                       bool use_compiled);

	static void random_unique(int row_count, int* codes, idx_t distinct);

	static void BindFunctions(EvalConfig& config, void* handle, PhysicalOperator* op,
	                          std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data);

	static void AllocSingle(EvalConfig& config, PhysicalOperator* op,
	            std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
	            std::unordered_map<std::string, std::vector<std::string>>& spec);
	static void AllocDense(EvalConfig& config, PhysicalOperator* op,
	                std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
	                std::unordered_map<std::string, std::vector<std::string>>& spec);
	static void GenSparseAndAlloc(EvalConfig& config, PhysicalOperator* op,
	                              std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
	                              std::unordered_map<std::string, std::vector<std::string>>& columns_spec,
	                              bool compile);

	static void InterventionSparseEvalPredicate(int thread_id, EvalConfig& config, PhysicalOperator* op,
	                                            std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
	                                            bool use_compiled);
	static void HashAggregateIntervene2DEval(int thread_id, EvalConfig& config, shared_ptr<OperatorLineage> lop,
	                                         std::unordered_map<idx_t, unique_ptr<FadeNode>>& fade_data,
	                                         PhysicalOperator* op, void* var_0, bool use_compile);
};

} // namespace duckdb
#endif

