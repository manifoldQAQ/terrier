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
  static void CompileBuildKeyFunction(
      exec::ExecutionContext *exec_ctx,
      catalog::namespace_oid_t ns_oid,
      catalog::table_oid_t table_oid,
      catalog::index_oid_t index_oid
      ) {
    catalog::CatalogAccessor *accessor = exec_ctx->GetAccessor();

    auto table = accessor->GetTable(table_oid);
    auto table_schema = accessor->GetSchema(table_oid);
    std::vector<catalog::col_oid_t> col_oids;
    storage::ProjectionMap table_pm(table->ProjectionMapForOids(col_oids));

    auto index = accessor->GetIndex(index_oid);
    auto index_schema = accessor->GetIndexSchema(index_oid);
    const auto &index_pm = index->GetKeyOidToOffsetMap();

    execution::compiler::CodeGen codegen(accessor);
    execution::compiler::PRFiller filler(&codegen, table_schema, table_pm);

    auto[root, fn_name] = filler.GenFiller(index_pm, index_schema);
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
    std::function<void(sql::ProjectedRowWrapper*, sql::ProjectedRowWrapper*)> filler_fn;
    module->GetFunction(fn_name, vm::ExecutionMode::Compiled, &filler_fn);

  }

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