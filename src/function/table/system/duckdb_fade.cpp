#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/fade/fade.hpp"

namespace duckdb {

struct DuckDBFadeData : public GlobalTableFunctionState {
	DuckDBFadeData(string out_var, string fname) : offset(0), out_var(out_var), fname(fname) {
	}

	idx_t offset;
  string out_var;
  string fname;
};

static unique_ptr<FunctionData> DuckDBFadeBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {

	if (global_fade_node == nullptr) return nullptr;
  
  string out_var = "out_count";
	for (auto &pair : global_fade_node->alloc_vars_index) {
    out_var = pair.first;
    if (global_fade_node->aggid < 0) break;
    if (out_var == "out_sum_2") continue;
    if (global_fade_node->aggid == pair.second) {
      break;
    }
  }

  string name = global_fade_node->alloc_vars_funcs[out_var];
  names.emplace_back("pid");
	return_types.emplace_back(LogicalType::INTEGER);

  // restriction: single agg at a time
  auto logical_typ = LogicalType::FLOAT;
  if (name == "count") {
    logical_typ = LogicalType::INTEGER;
  }
  if (!global_config.groups.empty()) {
    for (int i=0; i < global_config.groups.size(); i++) {
      names.emplace_back("g" + to_string(global_config.groups[i]));
      return_types.emplace_back(logical_typ);
    }
  } else {
    for (int i=0; i < global_fade_node->n_groups; ++i) {
        names.emplace_back("g" + to_string(i));
        return_types.emplace_back(logical_typ);
    }
  }

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBFadeInit(ClientContext &context, TableFunctionInitInput &input) {
  string out_var = "out_count";
	for (auto &pair : global_fade_node->alloc_vars_index) {
    out_var = pair.first;
    if (global_fade_node->aggid < 0) break;
    if (out_var == "out_sum_2") continue;
    if (global_fade_node->aggid == pair.second) {
      break;
    }
  }

  string fname = global_fade_node->alloc_vars_funcs[out_var];
	auto result = make_uniq<DuckDBFadeData>(out_var, fname);
	return std::move(result);
}

//! Create table to store executed queries with their IDs
//! Table name: duckdb_fade()
//! Schema: (INTEGER gn)+
void DuckDBFadeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBFadeData>();

	if (global_fade_node == nullptr || global_fade_node->alloc_vars.empty()) return;

	auto n_groups = global_fade_node->n_groups;
	auto n_interventions = global_fade_node->n_interventions;
	if (data.offset >= n_interventions) {
		// finished returning values
		return;
	}

  string out_var = data.out_var;

  // start returning values
  // either fill up the chunk or return all the remaining columns
  if (data.offset >= n_interventions) {
    return;
  }
  idx_t col = 1;
	idx_t count = 0;
  int limit = n_interventions-data.offset;
  if (limit >= STANDARD_VECTOR_SIZE) {
    limit = STANDARD_VECTOR_SIZE;
  }
  for (int i=0; i < global_fade_node->n_groups; ++i) {
    if (data.fname == "count") {
      int count_val = global_config.groups_count[i];
      int* count_ptr = (int*)global_fade_node->alloc_vars["out_count"][0];
      int index = i * n_interventions + data.offset;
      output.data[col].Initialize(true, limit);
      int* col_data = (int*)output.data[col].GetData();
      for (int v = 0; v < limit; ++v) {
        col_data[v] = count_val-count_ptr[index + v];
      }
      col++;
      count = limit;
    } else if (data.fname  == "sum" || data.fname == "sum_no_overflow") {
      // TODO: check if data type is int
      float* data_ptr = (float*)global_fade_node->alloc_vars[out_var][0];
      float sum_val = global_config.groups_sum[i];
      int index = i * n_interventions + data.offset;
      output.data[col].Initialize(true, limit);
      float* col_data = (float*)output.data[col].GetData();
      for (int v = 0; v < limit; ++v) {
        col_data[v] = sum_val-data_ptr[index + v];
      }
      col++;
      count = limit;
    } else if (data.fname  == "avg") {
      float* data_ptr = (float*)global_fade_node->alloc_vars[out_var][0];
      int* count_ptr = (int*)global_fade_node->alloc_vars["out_count"][0];
      int index = i * n_interventions + data.offset;
      float sum_val = global_config.groups_sum[i];
      int count_val = global_config.groups_count[i];
      float* col_data = (float*)output.data[col].GetData();
      for (int v = 0; v < limit; ++v) {
        // TODO: make sure we don't divide by 0
        col_data[v] = (sum_val - data_ptr[index + v]) / (count_val - count_ptr[index + v]);
      }
      col++;
      count = limit;
    } else if (data.fname == "stddev") {
      //  sum(x^2)/n - sum(x)^2/n^2
      //  (sum(x^2) - sum(x_remove^2))/(n - n_remove) - (sum(x) - sum(x_remove))^2/(n - n_remove)^2
      float* sum_ptr = (float*)global_fade_node->alloc_vars[out_var][0];
      float* sum_2_ptr = (float*)global_fade_node->alloc_vars["out_sum_2"][0];
      int* count_ptr = (int*)global_fade_node->alloc_vars["out_count"][0];
      int index = i * n_interventions + data.offset;
      float sum_2 = global_config.groups_sum_2[i];
      float sum_val = global_config.groups_sum[i];
      int count_val = global_config.groups_count[i];
      
      float* col_data = (float*)output.data[col].GetData();
      for (int v = 0; v < limit; ++v) {
        // TODO: make sure we don't divide by 0
        float sum_base = sum_val - sum_ptr[index + v];
        int count_base = count_val - count_ptr[index + v];
        col_data[v] = std::sqrt( (sum_2 - sum_2_ptr[index + v]) / count_base - (sum_base*sum_base) / (count_base*count_base) );
      }
      col++;
      count = limit;
    }

  }
  
  output.data[0].Sequence(data.offset, 1, count);

  data.offset += count;
	output.SetCardinality(count);
}

void DuckDBFadeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_fade", {}, DuckDBFadeFunction, DuckDBFadeBind, DuckDBFadeInit));
}

} // namespace duckdb
