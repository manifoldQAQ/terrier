#include <memory>

#include "execution/sql_test.h"  // NOLINT

#include "catalog/catalog_defs.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/util/timer.h"

namespace tpl::sql::test {

class TableVectorIteratorTest : public SqlBasedTest {
  void SetUp() override {
    // Create the test tables
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();
    GenerateTestTables(exec_ctx_.get());
  }

 protected:
  /**
   * Execution context to use for the test
   */
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
};

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, EmptyIteratorTest) {
  //
  // Check to see that iteration doesn't begin without an input block
  //
  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "empty_table");
  TableVectorIterator iter(!table_oid, exec_ctx_.get());
  iter.Init();
  ASSERT_FALSE(iter.Advance());
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, SimpleIteratorTest) {
  //
  // Simple test to ensure we iterate over the whole table
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_1");
  TableVectorIterator iter(!table_oid, exec_ctx_.get());
  iter.Init();
  ProjectedColumnsIterator *pci = iter.projected_columns_iterator();

  u32 num_tuples = 0;
  i32 prev_val{0};
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      auto* val = pci->Get<i32, false>(0, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::test1_size, num_tuples);
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, NullableTypesIteratorTest) {
  //
  // Ensure we iterate over the whole table even the types of the columns are
  // different
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_2");
  TableVectorIterator iter(!table_oid, exec_ctx_.get());
  iter.Init();
  ProjectedColumnsIterator *pci = iter.projected_columns_iterator();

  u32 num_tuples = 0;
  i16 prev_val{0};
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      // The serial column is the smallest one (SmallInt type), so it ends up at the last index in the storage layer.
      auto* val = pci->Get<i16, false>(3, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::test2_size, num_tuples);
}

// NOLINTNEXTLINE
TEST_F(TableVectorIteratorTest, IteratorAddColTest) {
  //
  // Ensure we only iterate over specified columns
  //

  auto table_oid = exec_ctx_->GetAccessor()->GetTableOid(NSOid(), "test_2");
  TableVectorIterator iter(!table_oid, exec_ctx_.get());
  const auto & schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  iter.AddCol(!schema.GetColumn("col1").Oid());
  iter.Init();
  ProjectedColumnsIterator *pci = iter.projected_columns_iterator();

  u32 num_tuples = 0;
  i16 prev_val{0};
  while (iter.Advance()) {
    for (; pci->HasNext(); pci->Advance()) {
      // Because we only specified one column, its index is 0 instead of three
      auto* val = pci->Get<i16, false>(0, nullptr);
      if (num_tuples > 0) {
        ASSERT_EQ(*val, prev_val + 1);
      }
      prev_val = *val;
      num_tuples++;
    }
    pci->Reset();
  }
  EXPECT_EQ(sql::test2_size, num_tuples);
}


}  // namespace tpl::sql::test