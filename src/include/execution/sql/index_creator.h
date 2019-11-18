#pragma once

#include <map>
#include <memory>
#include <vector>
#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "storage/storage_defs.h"

namespace terrier::execution::sql {

/**
 * Allows insertion from TPL.
 */
class EXPORT IndexCreator {
 public:
  /**
   * Constructor
   * @param exec_ctx The execution context
   * @param table_oid The oid of the table to insert into
   */
  explicit IndexCreator(exec::ExecutionContext *exec_ctx, catalog::index_oid_t index_oid, bool unique);
  /**
   * Destructor
   */
  ~IndexCreator();

  /**
   * @return The projected row of the index.
   */
  storage::ProjectedRow *GetIndexPR();

  /**
   * Insert into the index
   * @return Whether insertion was successful.
   */
  bool IndexInsert(storage::ProjectedRow *index_pr, storage::TupleSlot ts);

 private:
  exec::ExecutionContext *exec_ctx_;
  bool unique_;
  common::ManagedPointer<terrier::storage::index::Index> index_;

  void *index_pr_buffer_;
  uint32_t index_pr_size_;
  storage::ProjectedRow *index_pr_;
};

}  // namespace terrier::execution::sql
