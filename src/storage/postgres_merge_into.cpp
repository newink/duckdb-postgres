// TODO: Commented out for DuckDB v1.3.2 compatibility - LogicalMergeInto not available
/*
#include "storage/postgres_catalog.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "storage/postgres_update.hpp"
#include "storage/postgres_delete.hpp"
#include "storage/postgres_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Plan Merge Into
//===--------------------------------------------------------------------===//
unique_ptr<MergeIntoOperator> PostgresPlanMergeIntoAction(PostgresCatalog &catalog, ClientContext &context,
                                                          LogicalMergeInto &op, PhysicalPlanGenerator &planner,
                                                          BoundMergeIntoAction &action, PhysicalOperator &child_plan) {
	auto result = make_uniq<MergeIntoOperator>();

	result->action_type = action.action_type;
	result->condition = std::move(action.condition);
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	for (auto &constraint : op.bound_constraints) {
		bound_constraints.push_back(constraint->Copy());
	}
	auto return_types = op.types;

	switch (action.action_type) {
	case MergeActionType::MERGE_UPDATE: {
		if (action.columns.empty()) {
			// not updating any columns
			result->action_type = MergeActionType::MERGE_DO_NOTHING;
			break;
		}
		LogicalUpdate update(op.table);
		for (auto &def : op.bound_defaults) {
			update.bound_defaults.push_back(def->Copy());
		}
		update.bound_constraints = std::move(bound_constraints);
		update.expressions = std::move(action.expressions);
		update.columns = std::move(action.columns);
		update.update_is_del_and_insert = action.update_is_del_and_insert;
		result->op = catalog.PlanUpdate(context, planner, update, child_plan);
		auto &pg_update = result->op->Cast<PostgresUpdate>();
		pg_update.keep_copy_alive = false;
		break;
	}
	case MergeActionType::MERGE_DELETE: {
		LogicalDelete delete_op(op.table, 0);
		auto ref = make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, op.row_id_start);
		delete_op.expressions.push_back(std::move(ref));
		delete_op.bound_constraints = std::move(bound_constraints);
		result->op = catalog.PlanDelete(context, planner, delete_op, child_plan);
		break;
	}
	case MergeActionType::MERGE_INSERT: {
		LogicalInsert insert_op(op.table, 0);
		insert_op.bound_constraints = std::move(bound_constraints);
		for (auto &def : op.bound_defaults) {
			insert_op.bound_defaults.push_back(def->Copy());
		}
		// transform expressions if required
		if (!action.column_index_map.empty()) {
			vector<unique_ptr<Expression>> new_expressions;
			for (auto &col : op.table.GetColumns().Physical()) {
				auto storage_idx = col.StorageOid();
				auto mapped_index = action.column_index_map[col.Physical()];
				if (mapped_index == DConstants::INVALID_INDEX) {
					// push default value
					new_expressions.push_back(op.bound_defaults[storage_idx]->Copy());
				} else {
					// push reference
					new_expressions.push_back(std::move(action.expressions[mapped_index]));
				}
			}
			action.expressions = std::move(new_expressions);
		}
		result->expressions = std::move(action.expressions);
		result->op = catalog.PlanInsert(context, planner, insert_op, child_plan);
		// MERGE cannot keep the copy alive because we can interleave with other operations
		auto &pg_insert = result->op->Cast<PostgresInsert>();
		pg_insert.keep_copy_alive = false;
		break;
	}
	case MergeActionType::MERGE_ERROR:
		result->expressions = std::move(action.expressions);
		break;
	case MergeActionType::MERGE_DO_NOTHING:
		break;
	default:
		throw InternalException("Unsupported merge action");
	}
	return result;
}

PhysicalOperator &PostgresCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalMergeInto &op, PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw NotImplementedException("RETURNING is not implemented for Postgres yet");
	}
	map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions;

	// plan the merge into clauses
	for (auto &entry : op.actions) {
		vector<unique_ptr<MergeIntoOperator>> planned_actions;
		for (auto &action : entry.second) {
			planned_actions.push_back(PostgresPlanMergeIntoAction(*this, context, op, planner, *action, plan));
		}
		actions.emplace(entry.first, std::move(planned_actions));
	}

	auto &result = planner.Make<PhysicalMergeInto>(op.types, std::move(actions), op.row_id_start, op.source_marker,
	                                               false, op.return_chunk);
	result.children.push_back(plan);
	return result;
}

} // namespace duckdb
*/
