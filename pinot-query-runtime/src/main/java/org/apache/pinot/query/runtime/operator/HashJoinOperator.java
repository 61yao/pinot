/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.operator;

import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.operands.FilterOperand;


/**
 * This basic {@code BroadcastJoinOperator} implement a basic broadcast join algorithm.
 *
 * <p>It takes the right table as the broadcast side and materialize a hash table. Then for each of the left table row,
 * it looks up for the corresponding row(s) from the hash table and create a joint row.
 *
 * <p>For each of the data block received from the left table, it will generate a joint data block.
 *
 * We currently support left join, inner join and semi join.
 * The output is in the format of [left_row, right_row]
 */
// TODO: Move inequi out of hashjoin. (https://github.com/apache/pinot/issues/9728)
public class HashJoinOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "HASH_JOIN";
  private static final Set<JoinRelType> SUPPORTED_JOIN_TYPES =
      ImmutableSet.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT);
  private final HashMap<Key, List<Object[]>> _broadcastHashTable;
  private final Operator<TransferableBlock> _leftTableOperator;
  private final Operator<TransferableBlock> _rightTableOperator;
  private final JoinRelType _joinType;
  private final DataSchema _resultSchema;
  private final int _resultRowSize;
  private final List<FilterOperand> _joinClauseEvaluators;
  private boolean _isHashTableBuilt;
  private TransferableBlock _upstreamErrorBlock;
  private KeySelector<Object[], Object[]> _leftKeySelector;
  private KeySelector<Object[], Object[]> _rightKeySelector;

  public HashJoinOperator(Operator<TransferableBlock> leftTableOperator, Operator<TransferableBlock> rightTableOperator,
      DataSchema outputSchema, JoinNode.JoinKeys joinKeys, List<RexExpression> joinClauses, JoinRelType joinType) {
    Preconditions.checkState(SUPPORTED_JOIN_TYPES.contains(joinType),
        "Join type: " + joinType + " is not supported!");
    _leftKeySelector = joinKeys.getLeftJoinKeySelector();
    _rightKeySelector = joinKeys.getRightJoinKeySelector();
    Preconditions.checkState(_leftKeySelector != null, "LeftKeySelector for join cannot be null");
    Preconditions.checkState(_rightKeySelector != null, "RightKeySelector for join cannot be null");
    _leftTableOperator = leftTableOperator;
    _rightTableOperator = rightTableOperator;
    _resultSchema = outputSchema;
    _joinClauseEvaluators = new ArrayList<>(joinClauses.size());
    for (RexExpression joinClause : joinClauses) {
      _joinClauseEvaluators.add(FilterOperand.toFilterOperand(joinClause, _resultSchema));
    }
    _joinType = joinType;
    _resultRowSize = _resultSchema.size();
    _isHashTableBuilt = false;
    _broadcastHashTable = new HashMap<>();
    _upstreamErrorBlock = null;
  }

  @Override
  public List<Operator> getChildOperators() {
    // WorkerExecutor doesn't use getChildOperators, returns null here.
    return null;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    try {
      if (!_isHashTableBuilt) {
        // Build JOIN hash table
        buildBroadcastHashTable();
      }
      if (_upstreamErrorBlock != null) {
        return _upstreamErrorBlock;
      } else if (!_isHashTableBuilt) {
        return TransferableBlockUtils.getNoOpTransferableBlock();
      }
      // JOIN probe.
      return buildJoinedDataBlock(getProbeOperator().nextBlock());
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private Operator<TransferableBlock> getProbeOperator(){
    switch (_joinType){
      case LEFT:
        // Intentional fall through
      case INNER:
        return _leftTableOperator;
      case RIGHT:
        return _rightTableOperator;
    }
    Preconditions.checkState(false, "Join type shouldn't be supported:" + _joinType);
    return null;
  }

  private KeySelector<Object[], Object[]>  getProbeKeySelector() {
    switch (_joinType) {
      case LEFT:
        // Intentional fall through
      case INNER:
        return _leftKeySelector;
      case RIGHT:
        return _rightKeySelector;
    }
    Preconditions.checkState(false);
    return null;
  }
  private Operator<TransferableBlock> getBroadcastOperator(){
    switch (_joinType) {
      case LEFT:
        // Intentional fall through
      case INNER:
        return _rightTableOperator;
      case RIGHT:
        return _leftTableOperator;
    }
    Preconditions.checkState(false, "Join type shouldn't be supported:" + _joinType);
    return null;
  }

  private KeySelector<Object[], Object[]>  getBroadcastKeySelector(){
    switch (_joinType) {
      case LEFT:
        // Intentional fall through
      case INNER:
        return _rightKeySelector;
      case RIGHT:
        return _leftKeySelector;
    }
    Preconditions.checkState(false);
    return null;
  }

  private List<Object[]> getLeftAndRightRow(Object[] probeRow, Object[] broadcastRow){
    switch (_joinType) {
      case LEFT:
        // Intentional fall through
      case INNER:
        return ImmutableList.of(probeRow, broadcastRow);
      case RIGHT:
        return ImmutableList.of(broadcastRow, probeRow);
    }
    Preconditions.checkState(false);
    return null;
  }

  private void buildBroadcastHashTable() {
    Operator<TransferableBlock> broadcastOperator = getBroadcastOperator();
    KeySelector<Object[], Object[]> broadcastKeySelector = getBroadcastKeySelector();

    TransferableBlock broadcastBlock = broadcastOperator.nextBlock();
    if (broadcastBlock.isErrorBlock()) {
      _upstreamErrorBlock = broadcastBlock;
      return;
    }

    if (TransferableBlockUtils.isEndOfStream(broadcastBlock)) {
      _isHashTableBuilt = true;
      return;
    } else if (TransferableBlockUtils.isNoOpBlock(broadcastBlock)) {
      return;
    }
    List<Object[]> container = broadcastBlock.getContainer();
    System.out.println(container.size());
    // put all the rows into corresponding hash collections keyed by the key selector function.
    for (Object[] row : container) {
      List<Object[]> hashCollection =
          _broadcastHashTable.computeIfAbsent(new Key(broadcastKeySelector.getKey(row)), k -> new ArrayList<>());
      hashCollection.add(row);
    }
    System.out.println(_broadcastHashTable.size());
  }

  private TransferableBlock buildJoinedDataBlock(TransferableBlock probeBlock)
      throws Exception {
    if (probeBlock.isErrorBlock()) {
      _upstreamErrorBlock = probeBlock;
      return _upstreamErrorBlock;
    } else if (TransferableBlockUtils.isNoOpBlock(probeBlock) || TransferableBlockUtils.isEndOfStream(probeBlock)) {
      return probeBlock;
    }
    List<Object[]> rows = new ArrayList<>();
    List<Object[]> container = probeBlock.isEndOfStreamBlock() ? new ArrayList<>() : probeBlock.getContainer();
    System.out.println(container.size());
    for (Object[] probeRow : container) {
      // NOTE: Empty key selector will always give same hash code.
      List<Object[]> hashCollection =
          _broadcastHashTable.getOrDefault(new Key(getProbeKeySelector().getKey(probeRow)), Collections.emptyList());
      // If it is a left join and right table is empty, we return left rows.
      if (hashCollection.isEmpty() && (_joinType == JoinRelType.LEFT || _joinType == JoinRelType.RIGHT)) {
        rows.add(joinRow(probeRow, null));
      } else {
        // If it is other type of join.
        for (Object[] broadCastRow : hashCollection) {
          // TODO: Optimize this to avoid unnecessary object copy.
          Object[] resultRow = joinRow(probeRow, broadCastRow);
          if (_joinClauseEvaluators.isEmpty() || _joinClauseEvaluators.stream()
              .allMatch(evaluator -> evaluator.apply(resultRow))) {
            rows.add(resultRow);
          }
        }
      }
    }
    return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
  }

  private Object[] joinRow(Object[] probeRow, @Nullable Object[] broadcastRow) {
    List<Object[]> leftAndRightRows = getLeftAndRightRow(probeRow, broadcastRow);
    Object[] resultRow = new Object[_resultRowSize];
    int idx = 0;
    if (leftAndRightRows.get(0) != null) {
      for (Object obj : leftAndRightRows.get(0)) {
        resultRow[idx++] = obj;
      }
    }
    if (leftAndRightRows.get(1) != null) {
      for (Object obj : leftAndRightRows.get(1)) {
        resultRow[idx++] = obj;
      }
    }
    return resultRow;
  }
}
