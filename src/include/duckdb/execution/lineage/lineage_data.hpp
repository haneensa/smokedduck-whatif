//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage_data.hpp
// Wrapper specialized for lineage artifact data types
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "lineage_top.h"

#include <iostream>
#include <utility>

namespace duckdb {

template<typename T>
class LineageDataArray : public LineageData {
public:
	LineageDataArray(unique_ptr<T[]> data, idx_t count) : LineageData(count), data(move(data)) {
	}
	
  void Debug() override {
		std::cout << "LineageDataArray<" << typeid(T).name() << "> "  << " isProcessed: " << processed << std::endl;
		for (idx_t i = 0; i < count; i++) {
			std::cout << " (" << i << " -> " << data[i] << ") ";
		}
		std::cout << std::endl;
	}

	data_ptr_t Process(idx_t offset) override {
		if (processed == false) {
			for (idx_t i = 0; i < count; i++) {
				data[i] += offset;
			}
			processed = true;
		}
		return (data_ptr_t)data.get();
	}

	idx_t At(idx_t source) override {
		D_ASSERT(source < count);
		return (idx_t)data[source];
	}

	idx_t Size() override { return count * sizeof(T); }

private:
	unique_ptr<T[]> data;
};

class LineageSelVec : public LineageData {
public:
	LineageSelVec(const SelectionVector& vec_p, idx_t count, idx_t in_offset=0) : LineageData(count), vec(vec_p), in_offset(in_offset) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}
	void Debug() override;
	data_ptr_t Process(idx_t offset) override;
	idx_t Size() override {
		return count * sizeof(vec.get_index(0));
	}
	idx_t At(idx_t) override;

private:
	SelectionVector vec;
	idx_t in_offset;
};


// A Range of values where each successive number in the range indicates the lineage
// used to quickly capture Limits
class LineageRange : public LineageData {
public:
	LineageRange(idx_t start, idx_t end) : LineageData(end-start), start(start), end(end) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	virtual Vector GetVecRef(LogicalType t, idx_t offset) override {
		Vector vec(t, count);
		vec.Sequence(start+offset, 1, count);
		return vec;
	}

	void Debug() override {
		std::cout << "LineageRange - Start: " << start << " End: " << end << std::endl;
	}

	data_ptr_t Process(idx_t offset) override {
		throw std::logic_error("LineageRange shouldn't decompress its data");
	}

	idx_t Size() override {
		return 2 * sizeof(idx_t);
	}

	idx_t At(idx_t source) override {
		D_ASSERT(source >= start && source < end);
		return source;
	}

public:
	idx_t start;
	idx_t end;
};

// Constant Value
class LineageConstant : public LineageData {
public:
	LineageConstant(idx_t value, idx_t count) : LineageData(count), value(value) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	Vector GetVecRef(LogicalType t, idx_t offset) override {
		// adjust value based on type
		Vector vec(Value::Value::INTEGER(value + offset));
		return vec;
	}

	void Debug() override {
		std::cout << "LineageConstant - value: " << value << " Count: " << count << std::endl;
	}

	data_ptr_t Process(idx_t offset) override {
		throw std::logic_error("LineageConstant shouldn't decompress its data");
	}

	idx_t Size() override {
		return 1*sizeof(value);
	}

	idx_t At(idx_t) override {
		return value;
	}

private:
	idx_t value;
};

// Captures two lineage data of the same side - used for Joins
class LineageBinary : public LineageData {
public:
	LineageBinary(unique_ptr<LineageData> lhs, unique_ptr<LineageData> rhs) :
	      LineageData(0), left(move(lhs)), right(move(rhs)) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t offset) override;
	idx_t Size() override;
	idx_t At(idx_t) override {
		throw std::logic_error("Can't call backward directly on LineageBinary");
	}

	unique_ptr<LineageData> left;
	unique_ptr<LineageData> right;
private:
	bool switch_on_left = true;
};

} // namespace duckdb
#endif
