#include <execution/sql/table_vector_iterator.h>
#include <array>
#include <memory>
#include <vector>

#include "execution/compiler/storage/pr_filler.h"
#include "catalog/catalog_defs.h"
#include "execution/sql/index_creator.h"
#include "execution/sql/inserter.h"
#include "execution/sql_test.h"
#include "execution/compiler/codegen.h"
#include "execution/util/timer.h"
#include "storage/sql_table.h"
#include "execution/sql/index_iterator.h"

namespace terrier::execution::sql::test {

class IndexCreatorTest : public SqlBasedTest {
  void SetUp() override {
    InitTPL();
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }
 public:
  static std::unique_ptr<vm::Module> MakeModule(execution::compiler::CodeGen *codegen, ast::File* root, exec::ExecutionContext *exec_ctx) {
    // Create the query object, whose region must outlive all the processing.
    // Compile and check for errors
    EXECUTION_LOG_INFO("Generated File");
    sema::Sema type_checker{codegen->Context()};
    type_checker.Run(root);
    if (codegen->Reporter()->HasErrors()) {
      EXECUTION_LOG_ERROR("Type-checking error! \n {}", codegen->Reporter()->SerializeErrors());
    }

    EXECUTION_LOG_INFO("Converted: \n {}", execution::ast::AstDump::Dump(root));


    // Convert to bytecode

    auto bytecode_module = vm::BytecodeGenerator::Compile(root, exec_ctx, "tmp-tpl");
    bytecode_module->PrettyPrint(&std::cout);
    return std::make_unique<vm::Module>(std::move(bytecode_module));
  }
  /**
 * Initialize all TPL subsystems
 */
  static void InitTPL() {
    execution::CpuInfo::Instance();
    execution::vm::LLVMEngine::Initialize();
  }

  /**
   * Shutdown all TPL subsystems
   */
  static void ShutdownTPL() {
    terrier::execution::vm::LLVMEngine::Shutdown();
    terrier::LoggersUtil::ShutDown();
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};


std::function<void(sql::ProjectedRowWrapper*, sql::ProjectedRowWrapper*)>
testcompile(terrier::execution::exec::ExecutionContext *exec_ctx,
                                      terrier::catalog::table_oid_t table_oid,
                                      terrier::catalog::index_oid_t index_oid
) {
  auto accessor = exec_ctx->GetAccessor();
  auto table = accessor->GetTable(table_oid);
  auto table_schema = accessor->GetSchema(table_oid);
  std::vector<catalog::col_oid_t> col_oids;
  for (const auto & col : table_schema.GetColumns()) {
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
  auto [root, fn_name]= filler.GenFiller(index_pm, index_schema);

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
  std::function<void(sql::ProjectedRowWrapper*, sql::ProjectedRowWrapper*)> filler_fn;
  TERRIER_ASSERT(module->GetFunction(fn_name, vm::ExecutionMode::Compiled, &filler_fn), "");
  return filler_fn;
}


//// NOLINTNEXTLINE
//TEST_F(IndexCreatorTest, CompileTest) {
//  // Get Table Info
//  auto accessor = exec_ctx_->GetAccessor();
//  auto table_oid = accessor->GetTableOid("test_1");
//  auto index_oid = accessor->GetIndexOid("index_1");
//
//  IndexCreator index_creator(exec_ctx_.get(), table_oid, index_oid, false);
//
//  auto table_pr = index_creator.GetTablePRWrapper();
//  auto index_pr = index_creator.GetIndexPRWrapper();
//
//  auto filler_fn = testcompile(exec_ctx_.get(), table_oid, index_oid);
//
//  table_pr.Set<int32_t, false>(0, 500, false);
//  filler_fn(&table_pr, &index_pr);
//  auto val = index_pr.Get<int32_t, false>(0, nullptr);
//  ASSERT_EQ(*val, 500);
//
//  table_pr.Set<int32_t, false>(0, 651, false);
//  filler_fn(&table_pr, &index_pr);
//  val = index_pr.Get<int32_t, false>(0, nullptr);
//  ASSERT_EQ(*val, 651);
//
//}


// NOLINTNEXTLINE
TEST_F(IndexCreatorTest, CompileTest2) {
  // Get Table Info
  auto accessor = exec_ctx_->GetAccessor();
  auto table_oid = accessor->GetTableOid("test_1");
  auto index_oid = accessor->GetIndexOid("index_1");

  IndexCreator index_creator(exec_ctx_.get(), table_oid, index_oid, false);

  auto table_pr = index_creator.GetTablePRWrapper();
  auto index_pr = index_creator.GetIndexPRWrapper();


  auto table = accessor->GetTable(table_oid);
  auto table_schema = accessor->GetSchema(table_oid);
  std::vector<catalog::col_oid_t> col_oids;
  for (const auto & col : table_schema.GetColumns()) {
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
  auto [root, fn_name]= filler.GenFiller(index_pm, index_schema);

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

  auto bytecode_module = vm::BytecodeGenerator::Compile(root, exec_ctx_.get(), "tmp-tpl");
  bytecode_module->PrettyPrint(&std::cout);
  auto module = std::make_unique<vm::Module>(std::move(bytecode_module));

  // Now get the compiled function
  std::function<void(sql::ProjectedRowWrapper*, sql::ProjectedRowWrapper*)> filler_fn;
  ASSERT_TRUE(module->GetFunction(fn_name, vm::ExecutionMode::Compiled, &filler_fn));

  table_pr.Set<int32_t, false>(0, 500, false);
  filler_fn(&table_pr, &index_pr);
  auto val = index_pr.Get<int32_t, false>(0, nullptr);
  ASSERT_EQ(*val, 500);

  table_pr.Set<int32_t, false>(0, 651, false);
  filler_fn(&table_pr, &index_pr);
  val = index_pr.Get<int32_t, false>(0, nullptr);
  ASSERT_EQ(*val, 651);

}


//// NOLINTNEXTLINE
//TEST_F(IndexCreatorTest, CompileTest) {
////  auto txn = exec_ctx_->GetTxn();
//  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
//  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
////  auto table = exec_ctx_->GetAccessor()->GetTable(table_oid);
//
//  IndexCreator index_creator(exec_ctx_.get(), table_oid, index_oid, false);
//
////  auto table_pr = index_creator.GetTablePR();
//  auto table_pr_wrapper = index_creator.GetTablePRWrapper();
////  auto index_pr = index_creator.GetIndexPR();
////  auto index_pr_wrapper = index_creator.GetIndexPRWrapper();
//  auto filler_fn = index_creator.GetBuildKeyFn();
//
//  table_pr_wrapper.Set<int32_t, false>(0, 500, false);
////  filler_fn(&table_pr_wrapper, &index_pr_wrapper);
//
////  for (auto it = table->begin(); it != table->end(); it++) {
////    if (table->Select(txn, *it, table_pr)) {
////      filler_fn(&table_pr_wrapper, &index_pr_wrapper);
////      index_creator.IndexInsert(index_pr, *it);
////    }
////  }
//}

// NOLINTNEXTLINE
TEST_F(IndexCreatorTest, SimpleTest) {
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "empty_index_1");
  std::vector<uint32_t> table_col_oids = {1, 2, 3, 4};
//  auto index = exec_ctx_->GetAccessor()->GetIndex(index_oid);

  IndexCreator index_creator(exec_ctx_.get(), table_oid, index_oid, false);
  auto *index_pr = index_creator.GetIndexPR();
  auto cols = exec_ctx_->GetAccessor()->GetSchema(table_oid).GetColumns();

  {
    TableVectorIterator iter(exec_ctx_.get(), !table_oid, table_col_oids.data(),
                             static_cast<uint32_t>(table_col_oids.size()));
    iter.Init();
    while (iter.Advance()) {
      auto pci = iter.GetProjectedColumnsIterator();
      for (; pci->HasNext(); pci->Advance()) {
        const int *key_ptr = pci->Get<int32_t, false>(0, nullptr);
        *reinterpret_cast<int32_t *>(index_pr->AccessForceNotNull(0)) = *key_ptr;
        auto tuple_slot = pci->GetTupleSlot();
        index_creator.IndexInsert(index_pr, tuple_slot);
      }
      pci->Reset();
    }
  }
//  {
//    TableVectorIterator iter(exec_ctx_.get(), !table_oid, table_col_oids.data(),
//                             static_cast<uint32_t>(table_col_oids.size()));
//    iter.Init();
//    while (iter.Advance()) {
//      auto pci = iter.GetProjectedColumnsIterator();
//      for (; pci->HasNext(); pci->Advance()) {
//        const int *key_ptr = pci->Get<int32_t, false>(0, nullptr);
//        *reinterpret_cast<int32_t *>(index_pr->AccessForceNotNull(0)) = *key_ptr;
//        std::vector<storage::TupleSlot> tuple_slots;
//        index->ScanKey(*exec_ctx_->GetTxn(), *index_pr, &tuple_slots);
//        TERRIER_ASSERT(tuple_slots.size() == 1, "");
//      }
//      pci->Reset();
//    }
//  }
}

// NOLINTNEXTLINE
TEST_F(IndexCreatorTest, test2) {
  //
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  Inserter inserter(exec_ctx_.get(), table_oid);
  auto table_pr = inserter.GetTablePR();
  auto schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);

  *reinterpret_cast<int16_t *>(table_pr->AccessForceNotNull(3)) = 15;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(1)) = 721;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(2)) = 4256;
  *reinterpret_cast<int64_t *>(table_pr->AccessForceNotNull(0)) = 445;

  inserter.TableInsert();

  auto table = exec_ctx_->GetAccessor()->GetTable(table_oid);

  size_t count = 0;
  for (auto iter = table->begin(); iter != table->end(); iter++) {
    count++;
  }
  EXPECT_EQ(TEST1_SIZE + 1, count);

  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_1");
  auto index_pr = inserter.GetIndexPR(index_oid);
  auto index = exec_ctx_->GetAccessor()->GetIndex(index_oid);
  *reinterpret_cast<int32_t *>(index_pr->AccessForceNotNull(0)) = 15;
  std::vector<storage::TupleSlot> results1;
  index->ScanKey(*exec_ctx_->GetTxn(), *index_pr, &results1);
  EXPECT_TRUE(inserter.IndexInsert(index_oid));
  std::vector<storage::TupleSlot> results2;
  index->ScanKey(*exec_ctx_->GetTxn(), *index_pr, &results2);
  EXPECT_EQ(results1.size() + 1, results2.size());
}

// NOLINTNEXTLINE
TEST_F(IndexCreatorTest, MultiIndexTest) {
  //
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_2");
  Inserter inserter(exec_ctx_.get(), table_oid);
  auto table_pr = inserter.GetTablePR();
  auto schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);

  *reinterpret_cast<int16_t *>(table_pr->AccessForceNotNull(3)) = 15;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(1)) = 721;
  *reinterpret_cast<int32_t *>(table_pr->AccessForceNotNull(2)) = 4256;
  *reinterpret_cast<int64_t *>(table_pr->AccessForceNotNull(0)) = 445;

  inserter.TableInsert();

  auto table = exec_ctx_->GetAccessor()->GetTable(table_oid);

  size_t count = 0;
  for (auto iter = table->begin(); iter != table->end(); iter++) {
    count++;
  }
  EXPECT_EQ(TEST2_SIZE + 1, count);

  auto index_oid = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_2");
  auto index_pr = inserter.GetIndexPR(index_oid);
  auto index = exec_ctx_->GetAccessor()->GetIndex(index_oid);
  *reinterpret_cast<int16_t *>(index_pr->AccessForceNotNull(0)) = 15;
  std::vector<storage::TupleSlot> results1;
  index->ScanKey(*exec_ctx_->GetTxn(), *index_pr, &results1);
  EXPECT_TRUE(inserter.IndexInsert(index_oid));
  {
    std::vector<storage::TupleSlot> results2;
    index->ScanKey(*exec_ctx_->GetTxn(), *index_pr, &results2);
    EXPECT_EQ(results1.size() + 1, results2.size());
  }

  auto index_oid2 = exec_ctx_->GetAccessor()->GetIndexOid(NSOid(), "index_2_multi");
  auto index_pr2 = inserter.GetIndexPR(index_oid2);
  auto index2 = exec_ctx_->GetAccessor()->GetIndex(index_oid2);
  *reinterpret_cast<int16_t *>(index_pr2->AccessForceNotNull(1)) = 15;
  *reinterpret_cast<int32_t *>(index_pr2->AccessForceNotNull(0)) = 721;
  std::vector<storage::TupleSlot> results3;
  index2->ScanKey(*exec_ctx_->GetTxn(), *index_pr2, &results3);
  EXPECT_TRUE(inserter.IndexInsert(index_oid2));
  std::vector<storage::TupleSlot> results4;
  index2->ScanKey(*exec_ctx_->GetTxn(), *index_pr2, &results4);
  EXPECT_EQ(results3.size() + 1, results4.size());
  {
    std::array<uint32_t, 2> col_oids{1, 2};

    IndexIterator index_iter{exec_ctx_.get(), !table_oid, !index_oid2, col_oids.data(),
                             static_cast<uint32_t>(col_oids.size())};
    index_iter.Init();

    auto pr = index_iter.PR();

    *reinterpret_cast<int16_t *>(pr->AccessForceNotNull(1)) = 15;
    *reinterpret_cast<int32_t *>(pr->AccessForceNotNull(0)) = 721;
    index_iter.ScanKey();
    size_t final_multi_count = 0;
    while (index_iter.Advance()) {
      final_multi_count++;
    }

    EXPECT_EQ(final_multi_count, results4.size());
  }
}

}  // namespace terrier::execution::sql::test
