fun main(exec_ctx: *ExecutionContext) -> int64 {
  // initialize table
  var tvi: TableVectorIterator
  var oids: [4]uint32
  oids[0] = 1
  oids[1] = 2
  oids[2] = 3
  oids[3] = 4
  // index creator
  var index_creator : IndexCreator
  @indexCreatorInitBind(&index_creator, exec_ctx, "empty_index_1", false)
  var index_pr : *ProjectedRow = @indexCreatorGetIndexPR(&index_creator)

  var count = 0
  var flag = 0
  // iterate through table and populate index
  @tableIterInitBind(&tvi, exec_ctx, "test_1", oids)
  for (@tableIterAdvance(&tvi)) {
    var pci = @tableIterGetPCI(&tvi)
    for (; @pciHasNext(pci); @pciAdvance(pci)) {
      count = count + 1
      var cola = @pciGetInt(pci, 0)
      @prSetInt(index_pr, 0, @intToSql(count))
      var ts: TupleSlot = @pciGetTupleSlot(pci)
      var ok = @indexCreatorIndexInsert(&index_creator, index_pr, &ts)
    }
    @pciReset(pci)
  }
  @tableIterClose(&tvi)
  @indexCreatorFree(&index_creator)

  return count
}