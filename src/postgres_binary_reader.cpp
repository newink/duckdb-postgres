#include "postgres_binary_reader.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

PostgresBinaryReader::PostgresBinaryReader(PostgresConnection &con_p, const vector<column_t> &column_ids,
                                           const PostgresBindData &bind_data)
    : PostgresResultReader(con_p, column_ids, bind_data) {
}

PostgresBinaryReader::~PostgresBinaryReader() {
	Reset();
}

void PostgresBinaryReader::BeginCopy(const string &sql) {
	con.BeginCopyFrom(sql, PGRES_COPY_OUT);
	if (!Next()) {
		throw IOException("Failed to fetch header for COPY \"%s\"", sql);
	}
	CheckHeader();
}

PostgresReadResult PostgresBinaryReader::Read(DataChunk &output) {
	while (output.size() < STANDARD_VECTOR_SIZE) {
		while (!Ready()) {
			if (!Next()) {
				// finished this batch
				CheckResult();
				return PostgresReadResult::FINISHED;
			}
		}

		// read a row
		auto tuple_count = ReadInteger<int16_t>();
		if (tuple_count <= 0) { // done here, lets try to get more
			Reset();
			return PostgresReadResult::FINISHED;
		}

		D_ASSERT(tuple_count == column_ids.size());

		idx_t output_offset = output.size();
		for (idx_t output_idx = 0; output_idx < output.ColumnCount(); output_idx++) {
			auto col_idx = column_ids[output_idx];
			auto &out_vec = output.data[output_idx];
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				// row id
				// ctid in postgres are a composite type of (page_index, tuple_in_page)
				// the page index is a 4-byte integer, the tuple_in_page a 2-byte integer
				PostgresType ctid_type;
				ctid_type.info = PostgresTypeAnnotation::CTID;
				ReadValue(LogicalType::BIGINT, ctid_type, out_vec, output_offset);
			} else {
				ReadValue(bind_data.types[col_idx], bind_data.postgres_types[col_idx], out_vec, output_offset);
			}
		}
		Reset();
		output.SetCardinality(output_offset + 1);
	}
	// we filled a chunk
	return PostgresReadResult::HAVE_MORE_TUPLES;
}

} // namespace duckdb
