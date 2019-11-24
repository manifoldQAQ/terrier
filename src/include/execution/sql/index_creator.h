#pragma once

#include <map>
#include <memory>
#include <vector>
#include <planner/plannodes/create_index_plan_node.h>
#include <execution/sema/sema.h>
#include "execution/vm/module.h"
#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "storage/storage_defs.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/translator_factory.h"
#include "execution/compiler/storage/pr_filler.h"
#include "loggers/execution_logger.h"
#include "execution/ast/ast_dump.h"
#include "execution/vm/bytecode_generator.h"


namespace terrier::execution::sql {

/**
 * Allows insertion from TPL.
 */
class EXPORT IndexCreator {
 public:
  static std::function<void(sql::ProjectedRowWrapper*, sql::ProjectedRowWrapper*)>
  CompileBuildKeyFunction(terrier::execution::exec::ExecutionContext *exec_ctx,
                                        terrier::catalog::table_oid_t table_oid,
                                        terrier::catalog::index_oid_t index_oid);

 public:
  /**
   * Constructor
   * @param exec_ctx The execution context
   * @param table_oid The oid of the table to insert into
   */
  explicit IndexCreator(exec::ExecutionContext *exec_ctx,
      catalog::table_oid_t table_oid,
      catalog::index_oid_t index_oid, bool unique);
  /**
   * Destructor
   */
  ~IndexCreator();

  /**
   * @return The projected row of the index.
   */
  storage::ProjectedRow *GetIndexPR();

  sql::ProjectedRowWrapper GetIndexPRWrapper();

  storage::ProjectedRow *GetTablePR();

  sql::ProjectedRowWrapper GetTablePRWrapper();

  std::function<void(sql::ProjectedRowWrapper*, sql::ProjectedRowWrapper*)> GetBuildKeyFn();

  /**
   * Insert into the index
   * @return Whether insertion was successful.
   */
  bool IndexInsert(storage::ProjectedRow *index_pr, storage::TupleSlot ts);

 private:
  exec::ExecutionContext *exec_ctx_;
  bool unique_;

  std::function<void(sql::ProjectedRowWrapper*, sql::ProjectedRowWrapper*)> build_key_fn_;

  common::ManagedPointer<terrier::storage::SqlTable> table_;
  void *table_pr_buffer_;
  uint32_t table_pr_size_;
  storage::ProjectedRow *table_pr_;
  sql::ProjectedRowWrapper table_pr_wrapper_;

  common::ManagedPointer<terrier::storage::index::Index> index_;
  void *index_pr_buffer_;
  uint32_t index_pr_size_;
  storage::ProjectedRow *index_pr_;
  sql::ProjectedRowWrapper index_pr_wrapper_;
};

}  // namespace terrier::execution::sql