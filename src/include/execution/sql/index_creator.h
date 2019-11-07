#pragma once

#include <string>
#include <variant>
#include "parser/expression_defs.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "catalog/catalog_defs.h"
#include "storage/sql_table.h"
#include "storage/index/index_defs.h"
#include "storage/index/index.h"

namespace terrier::execution::sql {

class IndexCreator {
 private:
  common::ManagedPointer<catalog::Catalog> catalog_;
//  transaction::TransactionManager *txn_manager_;

  common::ManagedPointer<catalog::DatabaseCatalog> GetDatabaseCatalog(
      transaction::TransactionContext *txn,
      catalog::db_oid_t db_oid);

  common::ManagedPointer<storage::SqlTable> GetSqlTable(
      transaction::TransactionContext *txn,
      catalog::db_oid_t db_oid,
      catalog::table_oid_t table_oid);

 public:
  bool CreateIndex(
      transaction::TransactionContext *txn,
      catalog::db_oid_t db_oid,
      catalog::namespace_oid_t ns_oid,
      const std::string table_name,
      catalog::index_oid_t index_oid,
      catalog::table_oid_t table_oid,
      bool is_unique,
      storage::index::IndexType method,
      std::variant<catalog::col_oid_t, parser::AbstractExpression> expr,
      bool asc,
      bool nulls_first,
      /* table space */
      parser::AbstractExpression &predicate /* partial index */
    );
};

}