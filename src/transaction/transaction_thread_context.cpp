#include "transaction/transaction_thread_context.h"
#include "transaction/transaction_context.h"
#include <iostream>

namespace terrier::transaction {

void TransactionThreadContext::BeginTransaction(timestamp_t t) {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  curr_running_txns_.insert(t);
}

void TransactionThreadContext::Commit(TransactionContext *const txn) {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  // TODO do we need to check this?
  const timestamp_t start_time = txn->StartTime();
  const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(start_time);
  TERRIER_ASSERT(ret == 1, "Committed transaction did not exist in global transactions table");
  // It is not necessary to have to GC process read-only transactions, but it's probably faster to call free off
  // the critical path there anyway
  // Also note here that GC will figure out what varlen entries to GC, as opposed to in the abort case.
  if (gc_enabled_) completed_txns_.push_front(txn);
}

void TransactionThreadContext::Abort(TransactionContext *const txn) {
  // In a critical section, remove this transaction from the table of running transactions
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  const timestamp_t start_time = txn->StartTime();
  const size_t ret UNUSED_ATTRIBUTE = curr_running_txns_.erase(start_time);
  TERRIER_ASSERT(ret == 1, "Aborted transaction did not exist in global transactions table");
  if (gc_enabled_) completed_txns_.push_front(txn);
}

std::optional<timestamp_t> TransactionThreadContext::OldestTransactionStartTime() const {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  if (curr_running_txns_.empty()) return std::optional<timestamp_t>();
  timestamp_t oldest_txn = *curr_running_txns_.begin();
  return std::optional<timestamp_t>(oldest_txn);
}

TransactionQueue TransactionThreadContext::HandCompletedTransactions() {
  common::SpinLatch::ScopedSpinLatch guard(&latch_);
  TransactionQueue tmp = std::move(completed_txns_);
  TERRIER_ASSERT(completed_txns_.empty(), "TransactionManager's queue should now be empty.");
  return tmp;
}

}  // namespace terrier::transaction
