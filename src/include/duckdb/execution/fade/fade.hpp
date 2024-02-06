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
	vector<idx_t> annotations;
	idx_t n_interventions;
	idx_t n_masks;
	// single_del_intervention.size() == input table size
	// bits: del or not
	__mmask16* del_interventions;
	// this should be per attribute
	// unordered_map<string, vector<idx_t>>
	// vector<idx_t> single_scale_intervention;

	std::unordered_map<string, vector<void*>> alloc_vars;
	std::unordered_map<int, void*> input_data_map;
	vector<int> lineage[2];
	int (*join_fn)(int, int*, int*, __mmask16*, __mmask16*, __mmask16*);
	int (*agg_duckdb_fn)(int, int*, __mmask16*, std::unordered_map<std::string, vector<void*>>&, ChunkCollection&);
	int (*agg_fn)(int, int*, __mmask16*, std::unordered_map<std::string, vector<void*>>&,  std::unordered_map<int, void*>&);
	    // Default constructor with default values
	FadeDataPerNode() : n_interventions(1), n_masks(0), del_interventions(nullptr) {}
};

enum InterventionType {
	DELETE,
	SCALE
};
struct EvalConfig {
	int batch;
	int mask_size;
	bool is_scalar;
	bool use_duckdb;
	string columns_spec_str;
	InterventionType intervention_type;
	int n_intervention;
	int qid;
	int num_worker;
};

class Fade {
public:
	Fade() {};

	static void Why(PhysicalOperator* op, int k, string columns_spec, int distinct);
	static void Whatif(PhysicalOperator* op, EvalConfig config);
	//static void WhatifCompile(PhysicalOperator* op, string intervention_type, string columns_spec, int n_intervention);
	static void Rexec(PhysicalOperator* op);

};

} // namespace duckdb
#endif

