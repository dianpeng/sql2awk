package plan

// The following documentation is used to describe how the query is been
// mapped from SQL to AWK script.
//
// After the plan been generated, it will contain 7 phases, which will be
// executed sequentially, notes sort phase is not really executed by the
// awk but instead by sort command line
//
// 1) TableScan
//    For each TableScan object, a AWK routine will be setup to collect
//    the input/column from file and generate an array inside of the AWK
//
// 2) Join
//    The join phase will take *each* array, result from table scan, and
//    craft a *nested loop*, one table one nest of loop, and then place
//    the join filter, can be picutured as expression *after* the where,
//    though optimization will kick in which make the expression simpler
//    most of the cases.
//
//    Example as following:
//
//    for (const r0 in tab0) {
//      for (const r1 in tab1) {
//        ...
//          for (const rn in tabn) {
//            if (!filter(...)) continue;
//               group_by_next(r0, r1, r2, ...., rn)
//          }
//      }
//    }
//    group_by_flush()
//
// 3) GroupBy
//    The group by phase, if applicable, will perform a simple hash based
//    group by. It will setup 1 arrays, which holds index into all the
//    entry. Basically, a group_by array is setup, for column tuple needs to
//    be served as group by key, the value will be evaluated, denoted as V
//    And for all the column from the tab0...tabN, which result in the same
//    V, each table's column # will be recorded togther, to be one entry of
//    group_by. Let's assume for certain V, there're 3 tuples have the same
//    value V, in group_by, we have 4 entries related to this V.
//
//    1) group_by[V] = 3, indicates for value V, we have 3 entries
//    2) group_by[V, 0] = (r0, r1, r2, ..., rn), all the column index for that tuple
//    3) ....
//    4) ....
//
// 4) Agg
//    The aggregation phase, if applicable, will be used to perform aggregation
//    The aggregation result will be stored inside of agg table, which has index
//    -1 during code generation. All reference to aggregation value will become
//    reference to agg table, which has -1 index. This phase just does agg
//    calculation, and store the result into the agg table. It will recursively
//    call next phase after it finish its job.
//
// 5) Having
//    This phase will just perform a simple filter, since all the aggregation
//    operation is done. And it will call next phase handler
//
// 6) Output
//    This phase tries to generate output based on projection. The distinct will
//    help to perform dedup *after* the output been generated otherwise, we can
//    not do the job. Notes the output is a *fused* phase since it will take
//    care of several things, one is distinct, if user specify distinct, then
//    the output will setup a distinct table to make sure that only distincted
//    value will be output; the other is limitation, which is the same, if user
//    just want few entries to be displayed, the output phase will take care of
//    it as well.
//
// 7) Sort
//    This phase is not really correct, but we have no way to do sorting in AWK
//    unless using GAWK. To address this issue, we let the sort command line
//    tool to do the trick for us
