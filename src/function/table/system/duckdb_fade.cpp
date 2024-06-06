#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/fade/fade.hpp"

namespace duckdb {

struct DuckDBFadeData : public GlobalTableFunctionState {
	DuckDBFadeData(string out_var) : offset(0), out_var(out_var) {
	}

	idx_t offset;
  string out_var;
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

	auto result = make_uniq<DuckDBFadeData>(out_var);
	return std::move(result);
}

//! Create table to store executed queries with their IDs
//! Table name: queries_list
//! Schema: (INTEGER query_id, varchar query)
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
  string name = global_fade_node->alloc_vars_funcs[out_var];
  int limit = n_interventions-data.offset;
  if (limit >= STANDARD_VECTOR_SIZE) {
    limit = STANDARD_VECTOR_SIZE;
  }
  for (int i=0; i < global_fade_node->n_groups; ++i) {
    if (name == "count") {
      int* count_ptr = (int*)global_fade_node->alloc_vars["out_count"][0];
      int index = i * n_interventions + data.offset;
      // output.SetValue(col++, count, Value::INTEGER( (int)count_ptr[index] ));
      //count++;
      Vector in_index(LogicalType::INTEGER,(data_ptr_t)(count_ptr + index));
      output.data[col++].Reference(in_index);
      count = limit;
    } else if (name  == "sum" || name == "sum_no_overflow") {
      float* data_ptr = (float*)global_fade_node->alloc_vars[out_var][0];
      int index = i * n_interventions + data.offset;
      //output.SetValue(col++, count,Value::FLOAT( data_ptr[index] ));
      //count++;
      Vector in_index(LogicalType::FLOAT,(data_ptr_t)(data_ptr + index));
      output.data[col++].Reference(in_index);
      count = limit;
    } else if (name  == "avg") {
      // TODO: finalize aggregate and then have a pointer to the final result
      float* data_ptr = (float*)global_fade_node->alloc_vars[out_var][0];
      int* count_ptr = (int*)global_fade_node->alloc_vars["out_count"][0];
      int index = i * n_interventions + data.offset;
      if (count_ptr[index] > 0) {
        output.SetValue(col++, count,Value::FLOAT( data_ptr[index] / count_ptr[index] ));
      } else {
        output.SetValue(col++, count,Value::FLOAT( 0 ));
      }
      count++;
    } else if (name == "stddev") {
      //  sum(x^2)/n - sum(x)^2/n^2
      float* sum_ptr = (float*)global_fade_node->alloc_vars[out_var][0];
      float* sum_2_ptr = (float*)global_fade_node->alloc_vars["out_sum_2"][0];
      int* count_ptr = (int*)global_fade_node->alloc_vars["out_count"][0];
      int index = i * n_interventions + data.offset;
      if (count_ptr[index] > 1) {
        output.SetValue(col++, count,Value::FLOAT( std::sqrt(sum_2_ptr[index] / count_ptr[index]  - (sum_ptr[index]*sum_ptr[index])/(count_ptr[index]*count_ptr[index]) )));
      } else {
        output.SetValue(col++, count,Value::FLOAT( 0 ));
      }
      count++;
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

