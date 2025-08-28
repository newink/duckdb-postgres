#include "postgres_utils.hpp"
#include "storage/postgres_schema_entry.hpp"
#include "storage/postgres_transaction.hpp"
#include "postgres_type_oids.hpp"

#ifdef USE_DYNAMIC_LIBPQ
#include "../libpq_dynamic.h"
#else
#include "libpq-fe.h"
#endif

#ifdef HAVE_GSSAPI
#include <gssapi/gssapi.h>
#endif

namespace duckdb {

static void PGNoticeProcessor(void *arg, const char *message) {
}

PGconn *PostgresUtils::PGConnect(const string &dsn) {
#ifdef USE_DYNAMIC_LIBPQ
	// Initialize dynamic libpq loading
	static bool libpq_initialized = false;
	if (!libpq_initialized) {
		fprintf(stderr, "[DEBUG] PGConnect: Initializing dynamic libpq loading...\n");
		if (!libpq_dynamic_init()) {
			throw ConnectionException("Failed to load dynamic libpq library");
		}
		libpq_initialized = true;
	}
#endif
	
	// Debug: Log the connection string being used
	fprintf(stderr, "[DEBUG] PGConnect: Attempting connection with DSN: %s\n", dsn.c_str());
	fprintf(stderr, "[DEBUG] PGConnect: DSN length: %zu bytes\n", dsn.length());
	
	// Debug: Test connection string parsing before actual connection
	PQconninfoOption *connOptions = PQconninfoParse(dsn.c_str(), NULL);
	if (connOptions) {
		fprintf(stderr, "[DEBUG] PGConnect: Connection string parsing succeeded\n");
		for (PQconninfoOption *option = connOptions; option->keyword != NULL; option++) {
			if (option->val != NULL) {
				fprintf(stderr, "[DEBUG] PGConnect: Parsed param '%s' = '%s'\n", option->keyword, option->val);
			}
		}
		PQconninfoFree(connOptions);
	} else {
		fprintf(stderr, "[DEBUG] PGConnect: ERROR: Connection string parsing FAILED!\n");
	}
	
	// Debug: Check for Kerberos credential cache
	const char* krb5ccname = getenv("KRB5CCNAME");
	fprintf(stderr, "[DEBUG] PGConnect: KRB5CCNAME environment: %s\n", krb5ccname ? krb5ccname : "(not set)");
	
	// Check if we can access default credential cache
	system("klist -s 2>/dev/null && echo '[DEBUG] Kerberos: Valid tickets found' || echo '[DEBUG] Kerberos: No valid tickets or cache access failed'");
	
	// Debug: Check if libpq was compiled with GSSAPI support
	fprintf(stderr, "[DEBUG] PGConnect: Checking libpq GSSAPI support...\n");
	
#ifdef HAVE_GSSAPI
	fprintf(stderr, "[DEBUG] PGConnect: HAVE_GSSAPI is defined - extension compiled with GSSAPI\n");
#else
	fprintf(stderr, "[DEBUG] PGConnect: HAVE_GSSAPI NOT defined - extension compiled WITHOUT GSSAPI\n");
#endif

	// Debug: Check if GSSAPI symbols are available at runtime
	fprintf(stderr, "[DEBUG] PGConnect: Testing GSSAPI symbol availability...\n");
	
#ifdef HAVE_GSSAPI
	// Try a simple GSSAPI call to verify library linkage
	OM_uint32 major_status, minor_status;
	gss_buffer_desc name_buffer = GSS_C_EMPTY_BUFFER;
	gss_name_t gss_name = GSS_C_NO_NAME;
	
	name_buffer.value = (void*)"test@EXAMPLE.COM";
	name_buffer.length = strlen((char*)name_buffer.value);
	
	major_status = gss_import_name(&minor_status, &name_buffer, GSS_C_NT_USER_NAME, &gss_name);
	
	if (major_status == GSS_S_COMPLETE) {
		fprintf(stderr, "[DEBUG] PGConnect: GSSAPI symbols working - gss_import_name succeeded\n");
		gss_release_name(&minor_status, &gss_name);
	} else {
		fprintf(stderr, "[DEBUG] PGConnect: GSSAPI symbols FAILED - gss_import_name error: major=%u, minor=%u\n", 
		        major_status, minor_status);
	}
#else
	fprintf(stderr, "[DEBUG] PGConnect: GSSAPI test skipped - not compiled with GSSAPI support\n");
#endif
	
	PGconn *conn = PQconnectdb(dsn.c_str());

	// Debug: Log connection status and details
	if (conn) {
		fprintf(stderr, "[DEBUG] PGConnect: Connection status: %d (%s)\n", PQstatus(conn), 
		        PQstatus(conn) == CONNECTION_OK ? "OK" : "BAD");
		fprintf(stderr, "[DEBUG] PGConnect: Connection user: '%s'\n", PQuser(conn) ? PQuser(conn) : "(null)");
		fprintf(stderr, "[DEBUG] PGConnect: Connection host: '%s'\n", PQhost(conn) ? PQhost(conn) : "(null)");
		fprintf(stderr, "[DEBUG] PGConnect: Connection port: '%s'\n", PQport(conn) ? PQport(conn) : "(null)");
		fprintf(stderr, "[DEBUG] PGConnect: Connection db: '%s'\n", PQdb(conn) ? PQdb(conn) : "(null)");
		
		// Debug: Check GSSAPI-related connection options
		const char *gsslib = PQparameterStatus(conn, "gsslib");
		const char *gss_delegated_creds = PQparameterStatus(conn, "gss_delegated_creds");
		fprintf(stderr, "[DEBUG] PGConnect: GSSAPI lib: '%s'\n", gsslib ? gsslib : "(null)");
		fprintf(stderr, "[DEBUG] PGConnect: GSS delegated creds: '%s'\n", gss_delegated_creds ? gss_delegated_creds : "(null)");
	}

	// both PQStatus and PQerrorMessage check for nullptr
	if (PQstatus(conn) == CONNECTION_BAD) {
		// Debug: Log detailed error information
		fprintf(stderr, "[DEBUG] PGConnect: Connection failed with detailed error: %s\n", PQerrorMessage(conn));
		
		// Debug: Analyze error type
		string error_msg = string(PQerrorMessage(conn));
		if (error_msg.find("no PostgreSQL user name specified") != string::npos) {
			fprintf(stderr, "[DEBUG] PGConnect: ERROR ANALYSIS: Missing username in startup packet\n");
			fprintf(stderr, "[DEBUG] PGConnect: LIKELY CAUSE: GSSAPI not being used, falling back to regular auth\n");
			fprintf(stderr, "[DEBUG] PGConnect: SOLUTION: Ensure gssencmode=require and valid Kerberos ticket\n");
		} else if (error_msg.find("encryption required") != string::npos) {
			fprintf(stderr, "[DEBUG] PGConnect: ERROR ANALYSIS: GSSAPI encryption required but failed\n");
			fprintf(stderr, "[DEBUG] PGConnect: LIKELY CAUSE: No Kerberos credentials or server doesn't support GSSAPI\n");
			fprintf(stderr, "[DEBUG] PGConnect: SOLUTION: Run 'kinit user@REALM' to get Kerberos ticket\n");
		} else if (error_msg.find("GSSAPI") != string::npos || error_msg.find("GSS") != string::npos) {
			fprintf(stderr, "[DEBUG] PGConnect: ERROR ANALYSIS: GSSAPI authentication error\n");
			fprintf(stderr, "[DEBUG] PGConnect: Check Kerberos configuration and server setup\n");
		}
		
		throw IOException("Unable to connect to Postgres at %s: %s", dsn, string(PQerrorMessage(conn)));
	}
	
	fprintf(stderr, "[DEBUG] PGConnect: Connection successful\n");
	
	// Debug: Check server version for comparison
	int server_version = PQserverVersion(conn);
	fprintf(stderr, "[DEBUG] PGConnect: Server PostgreSQL version: %d\n", server_version);
	fprintf(stderr, "[DEBUG] PGConnect: Client libpq version: %d\n", PQlibVersion());
	
	PQsetNoticeProcessor(conn, PGNoticeProcessor, nullptr);
	return conn;
}

string PostgresUtils::TypeToString(const LogicalType &input) {
	if (input.HasAlias()) {
		if (StringUtil::CIEquals(input.GetAlias(), "wkb_blob")) {
			return "GEOMETRY";
		}
		return input.GetAlias();
	}
	switch (input.id()) {
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::DOUBLE:
		return "FLOAT";
	case LogicalTypeId::BLOB:
		return "BYTEA";
	case LogicalTypeId::LIST:
		return PostgresUtils::TypeToString(ListType::GetChildType(input)) + "[]";
	case LogicalTypeId::ENUM:
		throw NotImplementedException("Enums in Postgres must be named - unnamed enums are not supported. Use CREATE "
		                              "TYPE to create a named enum.");
	case LogicalTypeId::STRUCT:
		throw NotImplementedException("Composite types in Postgres must be named - unnamed composite types are not "
		                              "supported. Use CREATE TYPE to create a named composite type.");
	case LogicalTypeId::MAP:
		throw NotImplementedException("MAP type not supported in Postgres");
	case LogicalTypeId::UNION:
		throw NotImplementedException("UNION type not supported in Postgres");
	default:
		return input.ToString();
	}
}

LogicalType GetGeometryType() {
	auto blob_type = LogicalType(LogicalTypeId::BLOB);
	blob_type.SetAlias("WKB_BLOB");
	return blob_type;
}

LogicalType PostgresUtils::RemoveAlias(const LogicalType &type) {
	if (!type.HasAlias()) {
		return type;
	}
	if (StringUtil::CIEquals(type.GetAlias(), "json")) {
		return type;
	}
	if (StringUtil::CIEquals(type.GetAlias(), "geometry")) {
		return GetGeometryType();
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		auto child_types = StructType::GetChildTypes(type);
		return LogicalType::STRUCT(std::move(child_types));
	}
	case LogicalTypeId::ENUM: {
		auto &enum_vector = EnumType::GetValuesInsertOrder(type);
		Vector new_vector(LogicalType::VARCHAR);
		new_vector.Reference(enum_vector);
		return LogicalType::ENUM(new_vector, EnumType::GetSize(type));
	}
	default:
		throw InternalException("Unsupported logical type for RemoveAlias");
	}
}

LogicalType PostgresUtils::TypeToLogicalType(optional_ptr<PostgresTransaction> transaction,
                                             optional_ptr<PostgresSchemaEntry> schema,
                                             const PostgresTypeData &type_info, PostgresType &postgres_type) {
	auto &pgtypename = type_info.type_name;

	// postgres array types start with an _
	if (StringUtil::StartsWith(pgtypename, "_")) {
		if (transaction) {
			auto context = transaction->context.lock();
			if (!context) {
				throw InternalException("Context is destroyed!?");
			}
			Value array_as_varchar;
			if (context->TryGetCurrentSetting("pg_array_as_varchar", array_as_varchar)) {
				if (BooleanValue::Get(array_as_varchar)) {
					postgres_type.info = PostgresTypeAnnotation::CAST_TO_VARCHAR;
					return LogicalType::VARCHAR;
				}
			}
		}
		// get the array dimension information
		idx_t dimensions = type_info.array_dimensions;
		if (dimensions == 0) {
			dimensions = 1;
		}
		// fetch the child type of the array
		PostgresTypeData child_type_info;
		child_type_info.type_name = pgtypename.substr(1);
		child_type_info.type_modifier = type_info.type_modifier;
		PostgresType child_pg_type;
		auto child_type = PostgresUtils::TypeToLogicalType(transaction, schema, child_type_info, child_pg_type);
		// construct the child type based on the number of dimensions
		for (idx_t i = 1; i < dimensions; i++) {
			PostgresType new_pg_type;
			new_pg_type.children.push_back(std::move(child_pg_type));
			child_pg_type = std::move(new_pg_type);
			child_type = LogicalType::LIST(child_type);
		}
		auto result = LogicalType::LIST(child_type);
		postgres_type.children.push_back(std::move(child_pg_type));
		return result;
	}

	if (pgtypename == "bool") {
		return LogicalType::BOOLEAN;
	} else if (pgtypename == "int2") {
		return LogicalType::SMALLINT;
	} else if (pgtypename == "int4") {
		return LogicalType::INTEGER;
	} else if (pgtypename == "int8") {
		return LogicalType::BIGINT;
	} else if (pgtypename == "oid") { // "The oid type is currently implemented as an unsigned four-byte integer."
		return LogicalType::UINTEGER;
	} else if (pgtypename == "float4") {
		return LogicalType::FLOAT;
	} else if (pgtypename == "float8") {
		return LogicalType::DOUBLE;
	} else if (pgtypename == "numeric") {
		auto width = ((type_info.type_modifier - sizeof(int32_t)) >> 16) & 0xffff;
		auto scale = (((type_info.type_modifier - sizeof(int32_t)) & 0x7ff) ^ 1024) - 1024;
		if (type_info.type_modifier == -1 || width < 0 || scale < 0 || width > 38) {
			// fallback to double
			postgres_type.info = PostgresTypeAnnotation::NUMERIC_AS_DOUBLE;
			return LogicalType::DOUBLE;
		}
		return LogicalType::DECIMAL(width, scale);
	} else if (pgtypename == "char" || pgtypename == "bpchar") {
		postgres_type.info = PostgresTypeAnnotation::FIXED_LENGTH_CHAR;
		return LogicalType::VARCHAR;
	} else if (pgtypename == "varchar" || pgtypename == "text" || pgtypename == "json") {
		return LogicalType::VARCHAR;
	} else if (pgtypename == "jsonb") {
		postgres_type.info = PostgresTypeAnnotation::JSONB;
		return LogicalType::VARCHAR;
	} else if (pgtypename == "geometry") {
		return GetGeometryType();
	} else if (pgtypename == "date") {
		return LogicalType::DATE;
	} else if (pgtypename == "bytea") {
		return LogicalType::BLOB;
	} else if (pgtypename == "time") {
		return LogicalType::TIME;
	} else if (pgtypename == "timetz") {
		return LogicalType::TIME_TZ;
	} else if (pgtypename == "timestamp") {
		return LogicalType::TIMESTAMP;
	} else if (pgtypename == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	} else if (pgtypename == "interval") {
		return LogicalType::INTERVAL;
	} else if (pgtypename == "uuid") {
		return LogicalType::UUID;
	} else if (pgtypename == "point") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_POINT;
		child_list_t<LogicalType> point_struct;
		point_struct.emplace_back(make_pair("x", LogicalType::DOUBLE));
		point_struct.emplace_back(make_pair("y", LogicalType::DOUBLE));
		return LogicalType::STRUCT(point_struct);
	} else if (pgtypename == "line") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_LINE;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "lseg") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_LINE_SEGMENT;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "box") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_BOX;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "path") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_PATH;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "polygon") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_POLYGON;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else if (pgtypename == "circle") {
		postgres_type.info = PostgresTypeAnnotation::GEOM_CIRCLE;
		return LogicalType::LIST(LogicalType::DOUBLE);
	} else {
		if (!transaction) {
			// unsupported so fallback to varchar
			postgres_type.info = PostgresTypeAnnotation::CAST_TO_VARCHAR;
			return LogicalType::VARCHAR;
		}
		auto context = transaction->context.lock();
		if (!context) {
			throw InternalException("Context is destroyed!?");
		}
		auto entry = schema->GetEntry(CatalogTransaction(schema->ParentCatalog(), *context), CatalogType::TYPE_ENTRY,
		                              pgtypename);
		if (!entry) {
			// unsupported so fallback to varchar
			postgres_type.info = PostgresTypeAnnotation::CAST_TO_VARCHAR;
			return LogicalType::VARCHAR;
		}
		// custom type (e.g. composite or enum)
		auto &type_entry = entry->Cast<PostgresTypeEntry>();
		auto result_type = RemoveAlias(type_entry.user_type);
		postgres_type = type_entry.postgres_type;
		return result_type;
	}
}

LogicalType PostgresUtils::ToPostgresType(const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::UUID:
	case LogicalTypeId::VARCHAR:
		return input;
	case LogicalTypeId::LIST:
		return LogicalType::LIST(ToPostgresType(ListType::GetChildType(input)));
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> new_types;
		for (idx_t c = 0; c < StructType::GetChildCount(input); c++) {
			auto &name = StructType::GetChildName(input, c);
			auto &type = StructType::GetChildType(input, c);
			new_types.push_back(make_pair(name, ToPostgresType(type)));
		}
		auto result = LogicalType::STRUCT(std::move(new_types));
		result.SetAlias(input.GetAlias());
		return result;
	}
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return LogicalType::TIMESTAMP;
	case LogicalTypeId::TINYINT:
		return LogicalType::SMALLINT;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return LogicalType::BIGINT;
	case LogicalTypeId::UBIGINT:
		return LogicalType::DECIMAL(20, 0);
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	default:
		return LogicalType::VARCHAR;
	}
}

PostgresType PostgresUtils::CreateEmptyPostgresType(const LogicalType &type) {
	PostgresType result;
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		for (auto &child_type : StructType::GetChildTypes(type)) {
			result.children.push_back(CreateEmptyPostgresType(child_type.second));
		}
		break;
	case LogicalTypeId::LIST:
		result.children.push_back(CreateEmptyPostgresType(ListType::GetChildType(type)));
		break;
	default:
		break;
	}
	return result;
}

bool PostgresUtils::SupportedPostgresOid(const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::BIT:
	case LogicalTypeId::UUID:
		return true;
	default:
		return false;
	}
}

string PostgresUtils::PostgresOidToName(uint32_t oid) {
	switch (oid) {
	case BOOLOID:
		return "bool";
	case INT2OID:
		return "int2";
	case INT4OID:
		return "int4";
	case INT8OID:
		return "int8";
	case FLOAT4OID:
		return "float4";
	case FLOAT8OID:
		return "float8";
	case CHAROID:
	case BPCHAROID:
		return "char";
	case TEXTOID:
	case VARCHAROID:
		return "varchar";
	case JSONOID:
		return "json";
	case BYTEAOID:
		return "bytea";
	case DATEOID:
		return "date";
	case TIMEOID:
		return "time";
	case TIMESTAMPOID:
		return "timestamp";
	case INTERVALOID:
		return "interval";
	case TIMETZOID:
		return "timetz";
	case TIMESTAMPTZOID:
		return "timestamptz";
	case BITOID:
		return "bit";
	case UUIDOID:
		return "uuid";
	case NUMERICOID:
		return "numeric";
	case JSONBOID:
		return "jsonb";
	case BOOLARRAYOID:
		return "_bool";
	case CHARARRAYOID:
	case BPCHARARRAYOID:
		return "_char";
	case INT8ARRAYOID:
		return "_int8";
	case INT2ARRAYOID:
		return "_int2";
	case INT4ARRAYOID:
		return "_int4";
	case FLOAT4ARRAYOID:
		return "_float4";
	case FLOAT8ARRAYOID:
		return "_float8";
	case TEXTARRAYOID:
	case VARCHARARRAYOID:
		return "_varchar";
	case JSONARRAYOID:
		return "_json";
	case JSONBARRAYOID:
		return "_jsonb";
	case NUMERICARRAYOID:
		return "_numeric";
	case UUIDARRAYOID:
		return "_uuid";
	case DATEARRAYOID:
		return "_date";
	case TIMEARRAYOID:
		return "_time";
	case TIMESTAMPARRAYOID:
		return "_timestamp";
	case TIMESTAMPTZARRAYOID:
		return "_timestamptz";
	case INTERVALARRAYOID:
		return "_interval";
	case TIMETZARRAYOID:
		return "_timetz";
	case BITARRAYOID:
		return "_bit";
	default:
		return "unsupported_type";
	}
}

uint32_t PostgresUtils::ToPostgresOid(const LogicalType &input) {
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
		return BOOLOID;
	case LogicalTypeId::SMALLINT:
		return INT2OID;
	case LogicalTypeId::INTEGER:
		return INT4OID;
	case LogicalTypeId::BIGINT:
		return INT8OID;
	case LogicalTypeId::FLOAT:
		return FLOAT4OID;
	case LogicalTypeId::DOUBLE:
		return FLOAT8OID;
	case LogicalTypeId::VARCHAR:
		return VARCHAROID;
	case LogicalTypeId::BLOB:
		return BYTEAOID;
	case LogicalTypeId::DATE:
		return DATEOID;
	case LogicalTypeId::TIME:
		return TIMEOID;
	case LogicalTypeId::TIMESTAMP:
		return TIMESTAMPOID;
	case LogicalTypeId::INTERVAL:
		return INTERVALOID;
	case LogicalTypeId::TIME_TZ:
		return TIMETZOID;
	case LogicalTypeId::TIMESTAMP_TZ:
		return TIMESTAMPTZOID;
	case LogicalTypeId::BIT:
		return BITOID;
	case LogicalTypeId::UUID:
		return UUIDOID;
	case LogicalTypeId::LIST:
		return PostgresUtils::ToPostgresOid(ListType::GetChildType(input));
	default:
		throw NotImplementedException("Unsupported type for Postgres array copy: %s", input.ToString());
	}
}

PostgresVersion PostgresUtils::ExtractPostgresVersion(const string &version_str) {
	PostgresVersion result;
	idx_t pos = 0;
	if (!StringUtil::Contains(version_str, "PostgreSQL")) {
		result.type_v = PostgresInstanceType::UNKNOWN;
	}
	// scan for the first digit
	while (pos < version_str.size() && !StringUtil::CharacterIsDigit(version_str[pos])) {
		pos++;
	}
	for (idx_t version_idx = 0; version_idx < 3; version_idx++) {
		idx_t digit_start = pos;
		while (pos < version_str.size() && StringUtil::CharacterIsDigit(version_str[pos])) {
			pos++;
		}
		if (digit_start == pos) {
			// no digits
			break;
		}
		// our version is at [digit_start..pos)
		auto digit_str = version_str.substr(digit_start, pos - digit_start);
		auto digit = std::strtoll(digit_str.c_str(), 0, 10);
		switch (version_idx) {
		case 0:
			result.major_v = digit;
			break;
		case 1:
			result.minor_v = digit;
			break;
		default:
			result.patch_v = digit;
			break;
		}

		// check if the next character is a dot, if not we stop
		if (pos >= version_str.size() || version_str[pos] != '.') {
			break;
		}
		pos++;
	}
	return result;
}

string PostgresUtils::QuotePostgresIdentifier(const string &text) {
	return KeywordHelper::WriteOptionallyQuoted(text, '"', false);
}

} // namespace duckdb
