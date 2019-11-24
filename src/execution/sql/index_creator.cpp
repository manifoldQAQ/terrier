#include "execution/sql/index_creator.h"
#include <memory>
#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "storage/storage_defs.h"
#include "storage/sql_table.h"

namespace terrier::execution::sql {

std::function<void(sql::ProjectedRowWrapper*, sql::ProjectedRowWrapper*)>
    IndexCreator::CompileBuildKeyFunction(terrier::execution::exec::ExecutionContext *exec_ctx,
                                           terrier::catalog::table_oid_t table_oid,
                                           terrier::catalog::index_oid_t index_oid
                                           ) {
  auto accessor = exec_ctx->GetAccessor();
  auto table = accessor->GetTable(table_oid);
  auto table_schema = accessor->GetSchema(table_oid);
  std::vector<catalog::col_oid_t> col_oids;
  for (const auto &col : table_schema.GetColumns()) {
    col_oids.emplace_back(col.Oid());
  }
  storage::ProjectionMap table_pm(table->ProjectionMapForOids(col_oids));

  // Create pr filler
  terrier::execution::compiler::CodeGen codegen(accessor);
  terrier::execution::compiler::PRFiller filler(&codegen, table_schema, table_pm);

  // Get the index
  auto index = accessor->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  auto index_schema = accessor->GetIndexSchema(index_oid);

  // Compile the function
  auto[root, fn_name]= filler.GenFiller(index_pm, index_schema);

  // Create the query object, whose region must outlive all the processing.
  // Compile and check for errors
  EXECUTION_LOG_INFO("Generated File");
  sema::Sema type_checker{codegen.Context()};
  type_checker.Run(root);
  if (codegen.Reporter()->HasErrors()) {
    EXECUTION_LOG_ERROR("Type-checking error! \n {}", codegen.Reporter()->SerializeErrors());
  }

  EXECUTION_LOG_INFO("Converted: \n {}", execution::ast::AstDump::Dump(root));


  // Convert to bytecode

  auto bytecode_module = vm::BytecodeGenerator::Compile(root, exec_ctx, "tmp-tpl");
  bytecode_module->PrettyPrint(&std::cout);
  auto module = std::make_unique<vm::Module>(std::move(bytecode_module));

  // Now get the compiled function
  std::function<void(sql::ProjectedRowWrapper *, sql::ProjectedRowWrapper *)> filler_fn;
  TERRIER_ASSERT(module->GetFunction(fn_name, vm::ExecutionMode::Compiled, &filler_fn), "");
  return filler_fn;
}

IndexCreator::IndexCreator(exec::ExecutionContext *exec_ctx,
    catalog::table_oid_t table_oid,
    catalog::index_oid_t index_oid, bool unique)
    : exec_ctx_{exec_ctx}, unique_{unique},
      table_pr_wrapper_{sql::ProjectedRowWrapper(nullptr)},
      index_pr_wrapper_{sql::ProjectedRowWrapper(nullptr)}
{
//  build_key_fn_ = CompileBuildKeyFunction(exec_ctx, table_oid, index_oid);

  index_ = exec_ctx->GetAccessor()->GetIndex(index_oid);
  auto index_pri = index_->GetProjectedRowInitializer();
  index_pr_size_ = index_pri.ProjectedRowSize();
  index_pr_buffer_ = exec_ctx->GetMemoryPool()->AllocateAligned(index_pr_size_, sizeof(uint64_t), true);
  index_pr_ = index_pri.InitializeRow(index_pr_buffer_);
  index_pr_wrapper_ = sql::ProjectedRowWrapper(index_pr_);

  table_ = exec_ctx->GetAccessor()->GetTable(table_oid);
  std::vector<catalog::col_oid_t> col_oids;
  auto table_schema = exec_ctx->GetAccessor()->GetSchema(table_oid);
  for (const auto & col : table_schema.GetColumns()) {
    col_oids.emplace_back(col.Oid());
  }
  auto table_pri = table_->InitializerForProjectedRow(col_oids);
  table_pr_size_ = table_pri.ProjectedRowSize();
  table_pr_buffer_ = exec_ctx->GetMemoryPool()->AllocateAligned(table_pr_size_, sizeof(uint64_t), true);
  table_pr_ = table_pri.InitializeRow(table_pr_buffer_);
  table_pr_wrapper_ = sql::ProjectedRowWrapper(table_pr_);
}

/**
 * Destructor
 */
IndexCreator::~IndexCreator() {
  exec_ctx_->GetMemoryPool()->Deallocate(table_pr_buffer_, table_pr_size_);
  exec_ctx_->GetMemoryPool()->Deallocate(index_pr_buffer_, index_pr_size_); }

/**
 * @return The projected row of the index.
 */
storage::ProjectedRow *IndexCreator::GetIndexPR() { return index_pr_; }

sql::ProjectedRowWrapper IndexCreator::GetIndexPRWrapper() { return index_pr_wrapper_; }

/**
 * @return The projected row of the index.
 */
storage::ProjectedRow *IndexCreator::GetTablePR() { return table_pr_; }

sql::ProjectedRowWrapper IndexCreator::GetTablePRWrapper() { return table_pr_wrapper_; }


std::function<void(sql::ProjectedRowWrapper*, sql::ProjectedRowWrapper*)> IndexCreator::GetBuildKeyFn() {
  return build_key_fn_;
}

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