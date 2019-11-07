#include <string>
#include <vector>
#include <variant>
#include <storage/index/index_builder.h>

#include "parser/expression_defs.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "execution/sql/index_creator.h"
#include "catalog/catalog.h"
#include "catalog/catalog_defs.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/postgres/pg_database.h"
#include "catalog/postgres/pg_index.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_type.h"
#include "storage/sql_table.h"
#include "storage/index/index_defs.h"
#include "storage/index/index_builder.h"
#include "storage/index/index.h"
#include "catalog/index_schema.h"

namespace terrier::execution::sql {


common::ManagedPointer<catalog::DatabaseCatalog> IndexCreator::GetDatabaseCatalog(
    transaction::TransactionContext *txn,
    catalog::db_oid_t db_oid) {
  auto db_catalog_ptr = catalog_->GetDatabaseCatalog(txn, db_oid);
  TERRIER_ASSERT(db_catalog_ptr != nullptr, "No catalog for given database oid");
  return db_catalog_ptr;
}

common::ManagedPointer<storage::SqlTable> IndexCreator::GetSqlTable(
    transaction::TransactionContext *txn,
    catalog::db_oid_t db_oid,
    catalog::table_oid_t table_oid) {

  auto db_catalog_ptr = GetDatabaseCatalog(txn, db_oid);
  common::ManagedPointer<storage::SqlTable> table_ptr = nullptr;

  /* Let's ignore catalog tables for now. */
  switch (!table_oid) {
    case (!catalog::postgres::DATABASE_TABLE_OID): {
      TERRIER_ASSERT(false, "unsupported");
      break;
    }
    case (!catalog::postgres::CLASS_TABLE_OID): {
      TERRIER_ASSERT(false, "unsupported");
      break;
    }
    case (!catalog::postgres::NAMESPACE_TABLE_OID): {
      TERRIER_ASSERT(false, "unsupported");
      break;
    }
    case (!catalog::postgres::COLUMN_TABLE_OID): {
      TERRIER_ASSERT(false, "unsupported");
      break;
    }
    case (!catalog::postgres::CONSTRAINT_TABLE_OID): {
      TERRIER_ASSERT(false, "unsupported");
      break;
    }
    case (!catalog::postgres::INDEX_TABLE_OID): {
      TERRIER_ASSERT(false, "unsupported");
      break;
    }
    case (!catalog::postgres::TYPE_TABLE_OID): {
      TERRIER_ASSERT(false, "unsupported");
      break;
    }
    default:
      table_ptr = db_catalog_ptr->GetTable(txn, table_oid);
  }

  TERRIER_ASSERT(table_ptr != nullptr, "table does not exist");
  return table_ptr;
}

//bool IndexCreator::CreateIndex(
//    transaction::TransactionContext *txn,
//    catalog::db_oid_t db_oid,
//    bool is_unique,
//    bool concurrently,
//    const std::string table_name,
//    catalog::index_oid_t index_oid,
//    catalog::table_oid_t table_oid,
//    std::variant<catalog::col_oid_t, parser::AbstractExpression &> expr,
//    storage::index::IndexType method,
//    /* collation */
//    bool asc,
//    bool nulls_first,
//    /* table space */
//    parser::AbstractExpression &predicate /* partial index */
//) {
//  // currently expressions are not supported
//  if (std::holds_alternative<parser::AbstractExpression &>(expr)) {
//    TERRIER_ASSERT(false, "expressions are not supported");
//  }
//  // database object
//  auto db_catalog_ptr = GetDatabaseCatalog(txn, db_oid);
//  if (db_catalog_ptr->GetIndex(txn, index_oid)) {
//    TERRIER_ASSERT(false, "index with index_oid already exists");
//  }
//
//  // prep table for index population
//  const auto sql_table = GetSqlTable(txn, db_oid, table_oid);
//  const auto &table_schema = db_catalog_ptr->GetSchema(txn, table_oid);
//  std::vector<catalog::col_oid_t> table_col_oids;
//  for (const auto &col : table_schema.GetColumns()) {
//    table_col_oids.push_back(col.Oid());
//  }
//  auto table_pr_init = sql_table->InitializerForProjectedRow(table_col_oids);
//  auto *table_pr_buf = common::AllocationUtil::AllocateAligned(table_pr_init.ProjectedRowSize());
//  auto table_pr = table_pr_init.InitializeRow(table_pr_buf);
//
//  // index schema
//  auto indexed_col_oid = std::get<catalog::col_oid_t>(expr);
//  auto indexed_columns = {table_schema.GetColumn(indexed_col_oid)};
//  auto index_schema = catalog::IndexSchema(
//      indexed_columns,
//      method,
//      is_unique,
//      false,
//      false, /* ??? */
//      false
//      );
//  auto index_builder = storage::index::IndexBuilder().SetKeySchema(index_schema);
//  auto index = index_builder.Build();
//  index->GetProjectedRowInitializer().ProjectedRowSize();
//  auto index_pr = index->GetProjectedRowInitializer();
//
//  for (auto it : *sql_table) {
//    if (sql_table->Select(txn, it, table_pr)) {
//
//    }
//  }
////    for (auto table_iter = sql_table->begin();
////      table_iter != sql_table->end();
////      sql_table->Select(txn, &table_iter, table_pr)) {
////
////    }
//
//  delete[] table_pr_buf;
//  return false;
//}


}