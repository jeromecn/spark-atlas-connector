/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.atlas.sql

import com.hortonworks.spark.atlas.AtlasUtils.logDebug

import java.util.UUID
import java.util.concurrent.TimeUnit
import com.hortonworks.spark.atlas.sql.CommandsHarvester.WriteToDataSourceV2Harvester
import com.hortonworks.spark.atlas.sql.SparkExecutionPlanProcessor.SinkDataSourceWriter
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite
import org.apache.spark.sql.hive.execution._
import com.hortonworks.spark.atlas._
import com.hortonworks.spark.atlas.types.metadata
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}
import org.apache.atlas.model.instance.AtlasObjectId
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.PersistedView
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.connector.write.{DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.streaming.SinkProgress
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent

import scala.collection.immutable.Stream.Empty
import scala.collection.mutable

case class ColumnLineage(
    db: String = "",
    table: String = "",
    name: String = "",
    nameIndex: Long = 0L,
    owner: String = "",
    child: Set[ColumnLineage] = Set.empty[ColumnLineage])

object ColumnLineage extends Logging {

  def findAggregateColumn(expressions: Seq[Expression]): Seq[ColumnLineage] = {
    logDebug(s"[ColumnLineage] findAggregateColumn, expressions: ${expressions.size}")

    var columns: Seq[ColumnLineage] = Seq.empty

    expressions.foreach(ep => ep match {
      case ch: AttributeReference =>
        columns = columns.++(Some(ColumnLineage(name = ch.name, nameIndex = ch.exprId.id)))
      case e =>
        if (!e.children.isEmpty) {
          columns = columns.++(findAggregateColumn(e.children))
        }
    })
    logDebug(s"[ColumnLineage] findAggregateColumn, expressions: ${expressions.size}, " +
      s"columns: ${columns.size}, col: ${columns}")
    columns
  }
  def findColumns(plan: Seq[LogicalPlan],
                  parentColumn: String,
                  parentColumnIndex: Long): Seq[ColumnLineage] = {

    var columns: Seq[ColumnLineage] = Seq.empty

    plan.foreach(p => p match {
      case c: HiveTableRelation =>
        logDebug(s"[ColumnLineage] findColumns, HiveTableRelation, " +
          s"dataCols: ${c.dataCols}, " +
          s"json: ${c.toJSON}")
        if (!c.dataCols.isEmpty) {
          findAggregateColumn(c.dataCols).foreach(cd =>
            if (cd.name.equals(parentColumn) && cd.nameIndex.equals(parentColumnIndex)) {
              columns = columns.++(Option(ColumnLineage(
                db = SparkUtils.getDatabaseName(c.tableMeta.identifier),
                table = SparkUtils.getTableName(c.tableMeta.identifier),
                name = cd.name,
                nameIndex = cd.nameIndex,
                owner = c.tableMeta.owner
              )))
            }
          )
        }
      case c: Aggregate =>
        logDebug(s"[ColumnLineage] findColumns, Aggregate, " +
          s"aggregateExpressions: ${c.aggregateExpressions.length}, " +
          s"expressions: ${c.expressions.length}, " +
          s"children: ${c.children.length}, ")

        var alias: Seq[ColumnLineage] = Seq.empty

        c.aggregateExpressions.foreach(ag => ag match {
          case ch: org.apache.spark.sql.catalyst.expressions.Alias =>
            logDebug(s"[ColumnLineage] findColumns, Aggregate, child, alias: ${ch.name}")
            var attrs: Seq[ColumnLineage] = findAggregateColumn(ch.children)
            val aliasColumn = ColumnLineage(name = ch.name,
              nameIndex = ch.exprId.id,
              child = attrs.toSet)
            alias = alias.++(Some(aliasColumn))

            logDebug(s"[ColumnLineage] findColumns, Aggregate, child, " +
              s"alias: ${aliasColumn.name}, " +
              s"aliasindex: ${aliasColumn.nameIndex}, " +
              s"attrs: ${aliasColumn.child.size}, ${aliasColumn.child.toString()}")

            if (aliasColumn.name.eq(parentColumn)
              && aliasColumn.nameIndex.equals(parentColumnIndex)) {
              attrs.foreach(sub =>
                columns = columns.++(findColumns(c.children, sub.name, sub.nameIndex))
              )
            }

          case e =>
            logDebug("[ColumnLineage] findColumns, Aggregate, aggregateExpressions, child, " +
              s"case e: ${e}")
        })

      case c: Project =>
        logDebug(s"[ColumnLineage] findColumns, Project, " +
          s"projectList: ${c.projectList}, " +
          s"json: ${c.toJSON}")
        for (p <- c.projectList) {
          if (p.name.equals(parentColumn) && p.exprId.id.equals(parentColumnIndex)) {
            val subColumns: Seq[ColumnLineage] = findAggregateColumn(p.children)
            subColumns.foreach(sub =>
              columns = columns.++(findColumns(c.children, sub.name, sub.nameIndex))
            )
          }
        }
      case e =>
        logDebug(s"[ColumnLineage] findColumns, Other, " +
          s"e: ${e}, " +
          s"json: ${e.toJSON}")
        if (!e.children.isEmpty) {
          columns = columns.++(findColumns(e.children, parentColumn, parentColumnIndex))
        }
    })

    logDebug(s"[ColumnLineage] findColumns, result, parentName: ${parentColumn}, " +
      s"parentNameIndex: ${parentColumnIndex}, " +
      s"subColumnsSize: ${columns.size}, " +
      s"subColumns: ${columns}")
    columns
  }
}
case class QueryDetail(
    qe: QueryExecution,
    executionId: Long,
    query: Option[String] = None,
    sink: Option[SinkProgress] = None,
    queryId: Option[UUID] = None)

object QueryDetail extends Logging {
  def fromQueryExecutionListener(qe: QueryExecution, durationNs: Long): QueryDetail = {
    QueryDetail(qe, AtlasUtils.issueExecutionId(), Option(SQLQuery.get()))
  }

  def fromStreamingQueryListener(qe: StreamExecution, event: QueryProgressEvent): QueryDetail = {
    QueryDetail(qe.lastExecution, AtlasUtils.issueExecutionId(), None,
      Some(event.progress.sink), Some(qe.id))
  }
}

class SparkExecutionPlanProcessor(
    private[atlas] val atlasClient: AtlasClient,
    val conf: AtlasClientConf)
  extends AbstractEventProcessor[QueryDetail] with Logging {

  val createReqHelper = new AtlasEntityCreationRequestHelper(atlasClient)

  // TODO: We should handle OVERWRITE to remove the old lineage.
  // TODO: We should consider LLAPRelation later
  override protected def process(qd: QueryDetail): Unit = {
    logDebug(s"[SparkExecutionPlanProcessor] process, qd: ${qd.toString}")
    var outNodes: Seq[SparkPlan] = qd.qe.sparkPlan.collect {
      case p: UnionExec => p.children
      case p: DataWritingCommandExec => Seq(p)
      case p: WriteToDataSourceV2Exec => Seq(p)
      case p: LeafExecNode => Seq(p)
    }.flatten

    if (qd.sink.isDefined && !outNodes.exists(_.isInstanceOf[WriteToDataSourceV2Exec])) {
      val sink = qd.sink.get

      outNodes ++= Seq(
        WriteToDataSourceV2Exec(
          new MicroBatchWrite(0,
            new SinkDataSourceWriter(sink)), qd.qe.sparkPlan))
    }

    val entities = outNodes.flatMap {
      case r: ExecutedCommandExec =>
        r.cmd match {
          case c: LoadDataCommand =>
            // Case 1. LOAD DATA LOCAL INPATH (from local)
            // Case 2. LOAD DATA INPATH (from HDFS)
            logDebug(s"LOAD DATA [LOCAL] INPATH (${c.path}) ${c.table}")
            CommandsHarvester.LoadDataHarvester.harvest(c, qd)

          case c: CreateViewCommand =>
            c.viewType match {
              case PersistedView =>
                logDebug(s"CREATE VIEW AS SELECT query: ${qd.qe}")
                CommandsHarvester.CreateViewHarvester.harvest(c, qd)
              case _ => Seq.empty
            }

          case c: SaveIntoDataSourceCommand =>
            logDebug(s"DATA FRAME SAVE INTO DATA SOURCE: ${qd.qe}")
            CommandsHarvester.SaveIntoDataSourceHarvester.harvest(c, qd)

          case c: CreateTableCommand =>
            logDebug(s"CREATE TABLE USING external source - hive")
            CommandsHarvester.CreateTableHarvester.harvest(c, qd)

          case c: CreateDataSourceTableCommand =>
            logDebug(s"CREATE TABLE USING external source")
            CommandsHarvester.CreateDataSourceTableHarvester.harvest(c, qd)

          case _ =>
            Seq.empty
        }

      case r: DataWritingCommandExec =>
        r.cmd match {
          case c: InsertIntoHiveTable =>
            logDebug(s"INSERT INTO HIVE TABLE query ${qd.qe}")
            CommandsHarvester.InsertIntoHiveTableHarvester.harvest(c, qd)

          case c: InsertIntoHadoopFsRelationCommand =>
            logDebug(s"INSERT INTO SPARK TABLE query ${qd.qe}")
            CommandsHarvester.InsertIntoHadoopFsRelationHarvester.harvest(c, qd)

          case c: CreateHiveTableAsSelectCommand =>
            logDebug(s"CREATE TABLE AS SELECT query: ${qd.qe}")
            CommandsHarvester.CreateHiveTableAsSelectHarvester.harvest(c, qd)

          case c: CreateDataSourceTableAsSelectCommand =>
            logDebug(s"CREATE TABLE USING xx AS SELECT query: ${qd.qe}")
            CommandsHarvester.CreateDataSourceTableAsSelectHarvester.harvest(c, qd)

          case _ =>
            Seq.empty
        }

      case r: WriteToDataSourceV2Exec =>
        WriteToDataSourceV2Harvester.harvest(r, qd)

      case _ =>
        Seq.empty

        // Case 5. FROM ... INSERT (OVERWRITE) INTO t2 INSERT INTO t3
        // CASE LLAP:
        //    case r: RowDataSourceScanExec
        //        if (r.relation.getClass.getCanonicalName.endsWith("dd")) =>
        //      println("close hive connection via " + r.relation.getClass.getCanonicalName)

    } ++ {
      qd.qe.sparkPlan match {
        case d: DataWritingCommandExec if d.cmd.isInstanceOf[InsertIntoHiveDirCommand] =>
          CommandsHarvester.InsertIntoHiveDirHarvester.harvest(
            d.cmd.asInstanceOf[InsertIntoHiveDirCommand], qd)

        case _ =>
          Seq.empty
      }
    }

    createReqHelper.requestCreation(entities, qd.queryId)
  }
}

object SparkExecutionPlanProcessor {

  class SinkDataSourceWriter(val sinkProgress: SinkProgress) extends StreamingWrite {

    override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory =
      throw new UnsupportedOperationException("should not reach here!")

    override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit =
      throw new UnsupportedOperationException("should not reach here!")

    override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit =
      throw new UnsupportedOperationException("should not reach here!")
  }

}
