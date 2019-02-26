#pragma once
#include <set>
#include "common/spin_latch.h"
#include "transaction/transaction_defs.h"

namespace terrier::transaction {
/**
 * A TransactionThreadContext encapsulates information about the thread on which the transaction is started
 * (and presumably will finish). While this is not essential to our concurrency control algorithm, having
 * this information tagged with each transaction helps with various performance optimizations.
 */
class TransactionThreadContext {
 public:
  /**
   * Constructs a new TransactionThreadContext with the given worker_id
   * @param worker_id the worker_id of the thread
   */
  explicit TransactionThreadContext(worker_id_t worker_id) : worker_id_(worker_id) {}

  TransactionThreadContext(worker_id_t worker_id, bool gc_enabled) : worker_id_(worker_id), gc_enabled_(gc_enabled) {}

  /**
   * @return worker id of the thread
   */
  worker_id_t GetWorkerId() const { return worker_id_; }

  /**
   * Adds a running transaction to the running transaction set
   * @param t timestamp of the transaction
   * @return true if the timestamp is already in the running transaction queue
   */
  void BeginTransaction(timestamp_t t);

  void Commit(TransactionContext *txn);

  void Abort(TransactionContext *txn);

  std::optional<timestamp_t> OldestTransactionStartTime() const;

  TransactionQueue HandCompletedTransactions();

 private:
  // id of the worker thread on which the transaction start and finish.
  worker_id_t worker_id_;
  bool gc_enabled_;
  std::set<timestamp_t> curr_running_txns_;
  TransactionQueue completed_txns_;

  mutable common::SpinLatch latch_;
};

}  // namespace terrier::transaction
