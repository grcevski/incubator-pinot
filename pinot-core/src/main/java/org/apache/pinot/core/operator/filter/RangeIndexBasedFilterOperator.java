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
package org.apache.pinot.core.operator.filter;

import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory.OfflineDictionaryBasedRangePredicateEvaluator;
import org.apache.pinot.core.segment.index.readers.RangeIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class RangeIndexBasedFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "RangeFilterOperator";

  // NOTE: Range index can only apply to dictionary-encoded columns for now
  // TODO: Support raw index columns
  private final OfflineDictionaryBasedRangePredicateEvaluator _rangePredicateEvaluator;
  private final DataSource _dataSource;
  private final int _numDocs;

  public RangeIndexBasedFilterOperator(OfflineDictionaryBasedRangePredicateEvaluator rangePredicateEvaluator,
      DataSource dataSource, int numDocs) {
    _rangePredicateEvaluator = rangePredicateEvaluator;
    _dataSource = dataSource;
    _numDocs = numDocs;
  }

  @Override
  protected FilterBlock getNextBlock() {
    RangeIndexReader rangeIndexReader = (RangeIndexReader) _dataSource.getRangeIndex();
    assert rangeIndexReader != null;
    int firstRangeId = rangeIndexReader.findRangeId(_rangePredicateEvaluator.getStartDictId());
    // NOTE: End dictionary id is exclusive in OfflineDictionaryBasedRangePredicateEvaluator.
    int lastRangeId = rangeIndexReader.findRangeId(_rangePredicateEvaluator.getEndDictId() - 1);

    // Need to scan the first and last range as they might be partially matched
    // TODO: Detect fully matched first and last range
    ImmutableRoaringBitmap docIdsToScan;
    if (firstRangeId == lastRangeId) {
      docIdsToScan = rangeIndexReader.getDocIds(firstRangeId);
    } else {
      docIdsToScan =
          ImmutableRoaringBitmap.or(rangeIndexReader.getDocIds(firstRangeId), rangeIndexReader.getDocIds(lastRangeId));
    }
    ScanBasedFilterOperator scanBasedFilterOperator =
        new ScanBasedFilterOperator(_rangePredicateEvaluator, _dataSource, _numDocs);
    FilterBlockDocIdSet scanBasedDocIdSet = scanBasedFilterOperator.getNextBlock().getBlockDocIdSet();
    MutableRoaringBitmap docIds = ((ScanBasedDocIdIterator) scanBasedDocIdSet.iterator()).applyAnd(docIdsToScan);

    // Ranges in the middle of first and last range are fully matched
    for (int rangeId = firstRangeId + 1; rangeId < lastRangeId; rangeId++) {
      docIds.or(rangeIndexReader.getDocIds(rangeId));
    }
    return new FilterBlock(new BitmapDocIdSet(docIds, _numDocs) {

      // Override this method to reflect the entries scanned
      @Override
      public long getNumEntriesScannedInFilter() {
        return scanBasedDocIdSet.getNumEntriesScannedInFilter();
      }
    });
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
