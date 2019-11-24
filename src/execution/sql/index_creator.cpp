#include "execution/sql/index_creator.h"
#include <memory>
#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "storage/storage_defs.h"

namespace terrier::execution::sql {

IndexCreator::IndexCreator(exec::ExecutionContext *exec_ctx, catalog::index_oid_t index_oid, bool unique)
    : exec_ctx_{exec_ctx}, unique_{unique} {
  index_ = exec_ctx->GetAccessor()->GetIndex(index_oid);
  auto index_pri = index_->GetProjectedRowInitializer();
  index_pr_size_ = index_pri.ProjectedRowSize();
  index_pr_buffer_ = exec_ctx->GetMemoryPool()->AllocateAligned(index_pr_size_, sizeof(uint64_t), true);
  index_pr_ = index_pri.InitializeRow(index_pr_buffer_);
}

/**
 * Destructor
 */
IndexCreator::~IndexCreator() { exec_ctx_->GetMemoryPool()->Deallocate(index_pr_buffer_, index_pr_size_); }

/**
 * @return The projected row of the index.
 */
storage::ProjectedRow *IndexCreator::GetIndexPR() { return index_pr_; }

/**
 * Insert into the index
 * @return Whether insertion was successful.
 */
bool IndexCreator::IndexInsert(storage::ProjectedRow *index_pr, storage::TupleSlot ts) {
  if (unique_) {
    return index_->InsertUnique(exec_ctx_->GetTxn(), *index_pr, ts);
  } else {
    return index_->Insert(exec_ctx_->GetTxn(), *index_pr, ts);
  }
}

}  // namespace terrier::execution::sql