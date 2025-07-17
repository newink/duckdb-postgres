#include "postgres_connection.hpp"
#include "postgres_binary_reader.hpp"

namespace duckdb {

void PostgresConnection::BeginCopyFrom(const string &query, ExecStatusType expected_result) {
	auto result = PQExecute(query.c_str());
	if (!result || PQresultStatus(result) != expected_result) {
		PQclear(result);
		throw std::runtime_error("Failed to prepare COPY \"" + query + "\": " + string(PQresultErrorMessage(result)));
	}
	PQclear(result);
}

} // namespace duckdb
