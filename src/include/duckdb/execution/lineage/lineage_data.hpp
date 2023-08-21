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
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "lineage_top.h"

#include <iostream>
#include <utility>

namespace duckdb {

struct LogRecord;


struct LogRecord {
	shared_ptr<LineageData> data;
	idx_t in_start;
	LogRecord(shared_ptr<LineageData> data, idx_t in_start) :
	      data(data), in_start(in_start) {
	}
};

class Log {
public:
	Log() {}

	void Append(shared_ptr<LogRecord> log_record, idx_t stage_idx) {
		D_ASSERT(stage_idx < 4);
		log[stage_idx].push_back(log_record);
	}

	void Append(shared_ptr<vector<shared_ptr<LogRecord>>> log_record_vec, idx_t stage_idx) {
		D_ASSERT(stage_idx < 4);
		if (!log_record_vec) return;
		log[stage_idx].insert(log[stage_idx].end(), log_record_vec->begin(), log_record_vec->end());
	}

	idx_t GetLogSize(idx_t stage_idx) {
		D_ASSERT(stage_idx < 4);
		return log[stage_idx].size();
	}

	shared_ptr<LogRecord> GetLogRecord(idx_t stage_idx, idx_t data_idx) {
		D_ASSERT(stage_idx < 4 && data_idx < log[stage_idx].size());
		return log[stage_idx][data_idx];
	}

	idx_t GetLogSizeBytes();

	std::vector<shared_ptr<LogRecord>> log[4];
};

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

public:
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
	LineageBinary(shared_ptr<LineageData> lhs, shared_ptr<LineageData> rhs) :
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

	shared_ptr<LineageData> left;
	shared_ptr<LineageData> right;
private:
	bool switch_on_left = true;
};


class CollectionLineage : public LineageData {
public:
	CollectionLineage(shared_ptr<vector<shared_ptr<LineageData>>> lineage_vec, idx_t count) : LineageData(count),
	      lineage_vec(lineage_vec) {
	}
	void Debug() override {
		if (lineage_vec == nullptr) return;
	    for (idx_t i = 0; i < lineage_vec->size(); i++) {
			if (lineage_vec->operator[](i))
				lineage_vec->operator[](i)->Debug();
		}
	};

	data_ptr_t Process(idx_t offset) override {
		throw std::logic_error("Can't call process on CollectionLineage");
	}

	idx_t Size() override {
		idx_t size = 0;
		if (lineage_vec == nullptr) return 0;
		for (idx_t i = 0; i < lineage_vec->size(); i++) {
			if (lineage_vec->operator[](i))
				size += lineage_vec->operator[](i)->Size();
		}

		return size;
	}

	idx_t At(idx_t) override {
		throw std::logic_error("Can't call At directly on LineageNested");
	}

public:
	shared_ptr<vector<shared_ptr<LineageData>>> lineage_vec;
};

class LineageIdentity : public LineageData {
public:
	LineageIdentity(idx_t count) : LineageData(count) {
	}

	Vector GetVecRef(LogicalType t, idx_t offset) override {
		Vector vec(t, count);
		vec.Sequence(offset, 1, count);
		return vec;
	}

	void Debug() override {
		std::cout << "LineageIdentity of size: " << count << std::endl;
	}

	data_ptr_t Process(idx_t offset) override {
		throw std::logic_error("Can't call process on LineageIdentity");
	}

	idx_t Size() override {
		return sizeof(idx_t);
	}

	idx_t At(idx_t source) override {
		if (source >= count) {
			throw std::logic_error("Value does not exist within this Identity");
		}
		return source;
	}
};


} // namespace duckdb
#endif
