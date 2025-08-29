/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.deletionvectors.velox

import io.github.zhztheplayer.velox4j.Velox4j
import io.github.zhztheplayer.velox4j.`type`.{BigIntType, BooleanType}
import io.github.zhztheplayer.velox4j.arrow.Arrow
import io.github.zhztheplayer.velox4j.config.{Config, ConnectorConfig}
import io.github.zhztheplayer.velox4j.eval.{Evaluation, Evaluator}
import io.github.zhztheplayer.velox4j.expression.{CallTypedExpr, ConstantTypedExpr, FieldAccessTypedExpr}
import io.github.zhztheplayer.velox4j.memory.BytesAllocationListener
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, BitVector, FieldVector, VarBinaryVector, VectorSchemaRoot}
import org.apache.spark.sql.delta.RowIndexFilter
import org.apache.spark.sql.delta.deletionvectors.{DropAllRowsFilter, KeepAllRowsFilter, RoaringBitmapArray, RoaringBitmapArrayFormat, RowIndexMarkingFiltersBuilder}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.execution.velox.VeloxInitializer
import org.apache.spark.sql.vectorized.ColumnVector

import scala.collection.JavaConverters._

object VeloxRowIndexMarkingFilters {
  object DropMarkedRowsFilter extends RowIndexMarkingFiltersBuilder {
    VeloxInitializer.ensureInitialized()

    override def getFilterForEmptyDeletionVector(): RowIndexFilter = KeepAllRowsFilter
    override def getFilterForNonEmptyDeletionVector(bitmap: RoaringBitmapArray): RowIndexFilter = {
      new DropMarkedRowsFilter(bitmap)
    }
  }

  object KeepMarkedRowsFilter extends RowIndexMarkingFiltersBuilder {
    VeloxInitializer.ensureInitialized()

    override def getFilterForEmptyDeletionVector(): RowIndexFilter = DropAllRowsFilter
    override def getFilterForNonEmptyDeletionVector(bitmap: RoaringBitmapArray): RowIndexFilter = {
      new KeepMarkedRowsFilter(bitmap)
    }
  }

  abstract sealed class RowIndexMarkingFilters(bitmap: RoaringBitmapArray) extends RowIndexFilter {
    val valueWhenContained: Byte
    val valueWhenNotContained: Byte

    private val arrowAlloc = new RootAllocator()
    private val veloxAllocListener = new BytesAllocationListener()
    private val memoryManager = Velox4j.newMemoryManager(veloxAllocListener)
    private val session = Velox4j.newSession(memoryManager)

    private val roaringBitmapArrayEvaluator: Evaluator = {
      val arrowBitmap = new VarBinaryVector("bitmap", arrowAlloc)
      arrowBitmap.setValueCount(1)
      arrowBitmap.setSafe(0, bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable))

      val veloxBitmapConstant = session.arrowOps()
        .fromArrowVector(arrowAlloc, arrowBitmap)
        .wrapInConstant(1, 0)

      val evaluation = new Evaluation(
        new CallTypedExpr(new BooleanType(), Seq(
          ConstantTypedExpr.create(
            veloxBitmapConstant
          ),
          FieldAccessTypedExpr.create(new BigIntType(), "value"),
        ).asJava, "roaring_bitmap_array_contains"),
        Config.empty(),
        ConnectorConfig.empty()
      )

      session.evaluationOps().createEvaluator(evaluation)
    }

    override def materializeIntoVector(
      start: Long, end: Long, batch: WritableColumnVector): Unit = {
      val arrowRowIndexVector = new BigIntVector("value", arrowAlloc)
      val rowIdCount = (end - start).toInt
      arrowRowIndexVector.setValueCount(rowIdCount)
      for (rowId <- 0 until rowIdCount) {
        arrowRowIndexVector.set(rowId, start + rowId)
      }
      materializeIntoVectorWithArrowRowIndex(rowIdCount, arrowRowIndexVector, batch)
    }

    override def materializeIntoVectorWithRowIndex(
      batchSize: Int,
      rowIndexColumn: ColumnVector,
      batch: WritableColumnVector): Unit = {
      val arrowRowIndexVector = new BigIntVector("value", arrowAlloc)
      arrowRowIndexVector.setValueCount(batchSize)
      for (offset <- 0 until batchSize) {
        val rowId = rowIndexColumn.getLong(offset)
        arrowRowIndexVector.set(offset, rowId)
      }
      materializeIntoVectorWithArrowRowIndex(batchSize, arrowRowIndexVector, batch)
    }

    private def materializeIntoVectorWithArrowRowIndex(
      rowIdCount: Int, arrowRowIndexVector: BigIntVector, batch: WritableColumnVector): Unit = {
      assert(arrowRowIndexVector.getValueCount == rowIdCount)
      val arrowRowIndexVsr = new VectorSchemaRoot(Seq[FieldVector](arrowRowIndexVector).asJava)
      val veloxRowIndexVector = session.arrowOps().fromArrowVectorSchemaRoot(
        arrowAlloc, arrowRowIndexVsr)
      arrowRowIndexVsr.close()
      val sv = session.selectivityVectorOps().create(rowIdCount)
      val veloxOut = roaringBitmapArrayEvaluator.eval(sv, veloxRowIndexVector)
      sv.close()
      veloxRowIndexVector.close()
      val arrowOut = Arrow.toArrowVector(arrowAlloc, veloxOut).asInstanceOf[BitVector]
      veloxOut.close()
      assert(arrowOut.getValueCount == rowIdCount)
      for (rowId <- 0 until rowIdCount) {
        val value = if (arrowOut.get(rowId) == 1) {
          valueWhenContained
        } else {
          valueWhenNotContained
        }
        batch.putByte(rowId, value)
      }
      arrowOut.close()
    }

    override def materializeSingleRowWithRowIndex(
      rowIndex: Long,
      batch: WritableColumnVector): Unit = {
      // Fallback
      val isContained = isContainedInBitmap(rowIndex)
      // Assumes the batch has only one element.
      batch.putByte(0, isContained)
    }

    private def isContainedInBitmap(rowIndex: Long): Byte = {
      val isContained = bitmap.contains(rowIndex)
      if (isContained) {
        valueWhenContained
      } else {
        valueWhenNotContained
      }
    }
  }

  final class DropMarkedRowsFilter(bitmap: RoaringBitmapArray)
    extends RowIndexMarkingFilters(bitmap) {
    override val valueWhenContained: Byte = RowIndexFilter.DROP_ROW_VALUE
    override val valueWhenNotContained: Byte = RowIndexFilter.KEEP_ROW_VALUE
  }

  final class KeepMarkedRowsFilter(bitmap: RoaringBitmapArray)
    extends RowIndexMarkingFilters(bitmap) {
    override val valueWhenContained: Byte = RowIndexFilter.KEEP_ROW_VALUE
    override val valueWhenNotContained: Byte = RowIndexFilter.DROP_ROW_VALUE
  }
}
