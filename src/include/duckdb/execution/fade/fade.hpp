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

	// Default constructor with default values
	FadeDataPerNode() : n_interventions(1), n_masks(0), del_interventions(nullptr) {}
};

class Fade {
public:
	Fade() {};

	static void Why(PhysicalOperator* op, int k, string columns_spec, int distinct);
	static void Whatif(PhysicalOperator* op, string intervention_type, string columns_spec, int n_intervention);
	static void Rexec(PhysicalOperator* op);

};

} // namespace duckdb
#endif
