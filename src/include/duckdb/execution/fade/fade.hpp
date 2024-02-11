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

namespace duckdb {
class PhysicalOperator;

// holds allocated data per node
// interventions, annotations, etc
struct FadeDataPerNode {
	vector<int> annotations;
	idx_t n_interventions;
	idx_t n_masks;
	idx_t n_groups;
	// single_del_intervention.size() == input table size
	// bits: del or not
	__mmask16* del_interventions;
	int8_t* single_del_interventions;
	// this should be per attribute
	// unordered_map<string, vector<idx_t>>
	// vector<idx_t> single_scale_intervention;

	std::unordered_map<string, vector<void*>> alloc_vars;
	std::unordered_map<string, string> alloc_vars_types;
	std::unordered_map<string, int> alloc_vars_index;
	std::unordered_map<int, void*> input_data_map;
	vector<int> lineage[2];
	int (*filter_fn)(int, int*, void*, void*);
	int (*join_fn)(int, int*, int*, void*, void*, void*);
	int (*agg_duckdb_fn)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&, ChunkCollection&);
	int (*agg_fn)(int, int*, void*, std::unordered_map<std::string, vector<void*>>&,  std::unordered_map<int, void*>&);
};

enum InterventionType {
	DELETE,
	SCALE,
	SEARCH
};

struct EvalConfig {
	int batch;
	int mask_size;
	bool is_scalar;
	bool use_duckdb;
	bool debug;
	bool prune;
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

	static void Why(PhysicalOperator* op,  EvalConfig config);
	static string Whatif(PhysicalOperator* op, EvalConfig config);
	static string PredicateSearch(PhysicalOperator* op, EvalConfig config);
	static void Rexec(PhysicalOperator* op);

	template<class T1, class T2>
	static T2* GetInputVals(PhysicalOperator* op, shared_ptr<OperatorLineage> lop, idx_t col_idx);

	static string get_header(EvalConfig config);
	static string get_agg_alloc(int fid, string fn, string out_type);
	static string get_agg_finalize(EvalConfig config, FadeDataPerNode& node_data);

	static void* compile(std::string code, int id);

	static std::unordered_map<std::string, std::vector<std::string>> parseSpec(EvalConfig& config);

	template <class T>
	static void PrintOutput(FadeDataPerNode& info, T* data_ptr);

	static void GetLineage(EvalConfig& config, PhysicalOperator* op,
	                std::unordered_map<idx_t, FadeDataPerNode>& fade_data);

	static void FillFilterLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop,
	                              std::unordered_map<idx_t, FadeDataPerNode>& fade_data);

	static void FillJoinLineage(PhysicalOperator *op, shared_ptr<OperatorLineage> lop,
	                     std::unordered_map<idx_t,FadeDataPerNode>& fade_data);

	static std::vector<int> GetGBLineage(shared_ptr<OperatorLineage> lop, int row_count);

	static void PruneLineage(EvalConfig& config, PhysicalOperator* op,
	                  std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
	                  vector<int>& out_order);

	static void ReleaseFade(EvalConfig& config, void* handle, PhysicalOperator* op,
	                 std::unordered_map<idx_t, FadeDataPerNode>& fade_data);

	static void HashAggregateAllocate(EvalConfig& config, shared_ptr<OperatorLineage> lop,
	                                 std::unordered_map<idx_t, FadeDataPerNode>& fade_data,
	                                 PhysicalOperator* op);

	static vector<int> random_unique(shared_ptr<OperatorLineage> lop, idx_t distinct);
	static std::pair<vector<int>, int> factorize(PhysicalOperator* op, shared_ptr<OperatorLineage> lop,
	                                      std::unordered_map<std::string, std::vector<std::string>> columns_spec);

	static void BindFunctions(EvalConfig config, void* handle, PhysicalOperator* op,
	                   std::unordered_map<idx_t, FadeDataPerNode>& fade_data);
};

} // namespace duckdb
#endif
