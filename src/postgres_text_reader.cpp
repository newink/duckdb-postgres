#include "postgres_text_reader.hpp"

namespace duckdb {

PostgresTextReader::PostgresTextReader(ClientContext &context, PostgresConnection &con_p,
                                       const vector<column_t> &column_ids, const PostgresBindData &bind_data)
    : PostgresResultReader(con_p, column_ids, bind_data), context(context) {
}

PostgresTextReader::~PostgresTextReader() {
	Reset();
}

void PostgresTextReader::BeginCopy(const string &sql) {
	result = con.Query(sql);
	row_offset = 0;
}

PostgresReadResult PostgresTextReader::Read(DataChunk &output) {
	if (!result) {
		return PostgresReadResult::FINISHED;
	}
	if (scan_chunk.data.empty()) {
		// initialize the scan chunk
		vector<LogicalType> types;
		for (idx_t i = 0; i < output.ColumnCount(); i++) {
			types.push_back(LogicalType::VARCHAR);
		}
		scan_chunk.Initialize(context, types);
	}
	for (; scan_chunk.size() < STANDARD_VECTOR_SIZE && row_offset < result->Count(); row_offset++) {
		idx_t output_offset = scan_chunk.size();
		for (idx_t output_idx = 0; output_idx < output.ColumnCount(); output_idx++) {
			auto col_idx = column_ids[output_idx];
			auto &out_vec = scan_chunk.data[output_idx];
			if (result->IsNull(row_offset, output_idx)) {
				FlatVector::SetNull(out_vec, output_offset, true);
				continue;
			}
			auto col_data = FlatVector::GetData<string_t>(out_vec);
			col_data[output_offset] =
			    StringVector::AddStringOrBlob(out_vec, result->GetStringRef(row_offset, output_idx));
		}
		scan_chunk.SetCardinality(scan_chunk.size() + 1);
	}
	for (idx_t c = 0; c < output.ColumnCount(); c++) {
		VectorOperations::Cast(context, scan_chunk.data[c], output.data[c], scan_chunk.size());
	}
	output.SetCardinality(scan_chunk.size());
	return row_offset < result->Count() ? PostgresReadResult::HAVE_MORE_TUPLES : PostgresReadResult::FINISHED;
}

void PostgresTextReader::Reset() {
	result.reset();
	row_offset = 0;
}

} // namespace duckdb
