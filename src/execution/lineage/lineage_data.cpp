#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_data.hpp"

namespace duckdb {

// LineageSelVec

void LineageSelVec::Debug() {
	std::cout << "LineageSelVec " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec.sel_data()->owned_data[i] + in_offset << ") ";
	}
	std::cout << std::endl;
}

data_ptr_t LineageSelVec::Process(idx_t offset) {
	if (processed == false) {
		for (idx_t i = 0; i < count; i++) {
			*(vec.data() + i) += offset + in_offset;
		}
		processed = true;
	}
	return (data_ptr_t)vec.data();
}

idx_t LineageSelVec::At(idx_t source) {
	D_ASSERT(source < count);
	return (idx_t)vec.sel_data()->owned_data[source] + in_offset;
}


idx_t LineageBinary::Count() {
	if (left) return left->Count();
	else return right->Count();
}

void LineageBinary::Debug() {
	if (left) left->Debug();
	if (right) right->Debug();
}

data_ptr_t LineageBinary::Process(idx_t offset) {
	if (switch_on_left && left) {
		switch_on_left = !switch_on_left;
		return left->Process(offset);
	} else if (right) {
		switch_on_left = !switch_on_left;
		return right->Process(offset);
	} else {
		return nullptr;
	}
}

idx_t LineageBinary::Size() {
	auto size = 0;
	if (left) size += left->Size();
	if (right) size += right->Size();
	return size;
}

} // namespace duckdb
#endif
