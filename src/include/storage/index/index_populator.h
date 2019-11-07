#pragma  once

#include "parser/expression_defs.h"
#include "catalog/catalog.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "storage/sql_table.h"
#include "storage/index/index_defs.h"
#include "storage/index/index_builder.h"
#include "storage/index/index.h"
#include "catalog/index_schema.h"
#include "catalog/catalog_accessor.h"

namespace terrier::storage::index {

class IndexPopulator {
 public:

  static void EvalTo(ProjectedRow * pr, size_t i, const parser::AbstractExpression &expr) {}

  /* Populates an index from a SQL table to an index. */
  static Index *PopulateIndex(
      catalog::CatalogAccessor *catalog,
      transaction::TransactionContext *txn,
      catalog::db_oid_t db_oid,
      bool is_unique,
      bool concurrent,
      catalog::table_oid_t  table_oid,
      IndexType index_type,
      std::vector<const parser::AbstractExpression &> &index_key_exprs
//      const parser::AbstractExpression &predicate /* partial index */
      ) {
    const auto table_ptr = catalog->GetTable(table_oid);
    TERRIER_ASSERT(table_ptr != nullptr, "table does not exist");

    // prep table projected row
    const auto &table_schema = catalog->GetSchema(table_oid);
    std::vector<catalog::col_oid_t> table_col_oids;
    table_col_oids.reserve(table_schema.GetColumns().size());
    for (const auto &col : table_schema.GetColumns()) {
      table_col_oids.push_back(col.Oid());
    }
    auto table_pr_initializer = table_ptr->InitializerForProjectedRow(table_col_oids);
    auto *table_pr_buf = common::AllocationUtil::AllocateAligned(table_pr_initializer.ProjectedRowSize());
    auto *table_pr = table_pr_initializer.InitializeRow(table_pr_buf);

    // prep index projected row
    std::vector<catalog::IndexSchema::Column> index_key_cols;
    index_key_cols.reserve(index_key_exprs.size());
    uint32_t oid_counter = 0; // TODO should be handed over from the catalog
    for (auto &attr : index_key_exprs) {
      index_key_cols.emplace_back("", attr.GetReturnValueType(), true, attr);
      index_key_cols.back().SetOid(static_cast<catalog::indexkeycol_oid_t>(++oid_counter));
    }
    catalog::IndexSchema index_schema(index_key_cols, index_type, is_unique, false, false, false);
    IndexBuilder index_builder;
    index_builder.SetKeySchema(index_schema);
    Index *index = index_builder.Build();
    TERRIER_ASSERT(index != nullptr, "index can not be built");
    auto &index_pr_initializer = index->GetProjectedRowInitializer();
    auto *index_pr_buf = common::AllocationUtil::AllocateAligned(index_pr_initializer.ProjectedRowSize());
    auto *index_pr = index_pr_initializer.InitializeRow(index_pr_buf);

    for (auto slot : *table_ptr) {
      if (table_ptr->Select(txn, slot, table_pr)) {
        for (size_t i = 0; i < index_key_cols.size(); ++i) {
          EvalTo(index_pr, i, index_key_exprs[i]);
        }
        if (is_unique) {
          index->InsertUnique(txn, *index_pr, slot);
        } else {
          index->Insert(txn, *index_pr, slot);
        }
      }
    }

    delete table_pr_buf;
    delete index_pr_buf;

    return index;
  }

};

}