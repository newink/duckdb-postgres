#include "storage/postgres_catalog.hpp"
#include "storage/postgres_index.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"

namespace duckdb {

PostgresCreateIndex::PostgresCreateIndex(unique_ptr<CreateIndexInfo> info, TableCatalogEntry &table)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), info(std::move(info)), table(table) {
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PostgresCreateIndex::GetData(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &catalog = table.catalog;
	auto &schema = table.schema;
	auto transaction = catalog.GetCatalogTransaction(context.client);
	auto existing = schema.GetEntry(transaction, CatalogType::INDEX_ENTRY, info->index_name);
	if (existing) {
		switch (info->on_conflict) {
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return SourceResultType::FINISHED;
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw BinderException("Index with name \"%s\" already exists in schema \"%s\"", info->index_name,
			                      table.schema.name);
		case OnCreateConflict::REPLACE_ON_CONFLICT: {
			DropInfo drop_info;
			drop_info.type = CatalogType::INDEX_ENTRY;
			drop_info.schema = info->schema;
			drop_info.name = info->index_name;
			schema.DropEntry(context.client, drop_info);
			break;
		}
		default:
			throw InternalException("Unsupported on create conflict");
		}
	}
	schema.CreateIndex(transaction, *info, table);

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Logical Operator
//===--------------------------------------------------------------------===//
class LogicalPostgresCreateIndex : public LogicalExtensionOperator {
public:
	LogicalPostgresCreateIndex(unique_ptr<CreateIndexInfo> info_p, TableCatalogEntry &table)
	    : info(std::move(info_p)), table(table) {
	}

	unique_ptr<CreateIndexInfo> info;
	TableCatalogEntry &table;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		return planner.Make<PostgresCreateIndex>(std::move(info), table);
	}

	void Serialize(Serializer &serializer) const override {
		throw NotImplementedException("Cannot serialize Postgres Create index");
	}

	void ResolveTypes() override {
		types = {LogicalType::BIGINT};
	}
};

unique_ptr<LogicalOperator> PostgresCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                             TableCatalogEntry &table,
                                                             unique_ptr<LogicalOperator> plan) {
	// FIXME: this is a work-around for the CreateIndexInfo we are getting here not being fully bound
	// this needs to be fixed upstream (eventually)
	auto result = Catalog::BindCreateIndex(binder, stmt, table, std::move(plan));
	auto &index_create = result->Cast<LogicalCreateIndex>();
	return make_uniq<LogicalPostgresCreateIndex>(std::move(index_create.info), table);
}

} // namespace duckdb
