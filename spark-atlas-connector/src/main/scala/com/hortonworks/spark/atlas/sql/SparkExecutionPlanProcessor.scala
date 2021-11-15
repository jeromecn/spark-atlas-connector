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
import com.hortonworks.spark.atlas.utils.Logging
import org.apache.atlas.model.instance.AtlasObjectId
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.PersistedView
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
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
    child: Seq[ColumnLineage] = Seq.empty[ColumnLineage])

object ColumnLineage extends Logging {
//  def fromQueryExecutionListener(Seq[LogicalPlan], cols: Seq[String]): Seq[ColumnLineage] = {
//    val columns = Seq.empty[ColumnLineage]
//
//    for (c <- qe.optimizedPlan.children) {
//      val column = Some(ColumnLineage(name = ???, child = ???))
//      findColumns(c.children, column, "")
//      columns.++(column)
//    }
//
//    for (c <- cols) {
//
//    }
//
//    columns
//  }

  def findColumns(plan: Seq[LogicalPlan],
                          column: Option[ColumnLineage],
                          parentColumn: String): Unit = {

    plan.foreach(p => p match {
      case c: HiveTableRelation =>
        logDebug(s"[ColumnLineage] findColumns, HiveTableRelation, dataCols: ${c.dataCols}")
        if (!c.dataCols.isEmpty) {
          c.dataCols.foreach(dc => if (parentColumn.equals(dc.name)) {
            // todo
            column.get.child.++(Some(ColumnLineage(
              db = dc.qualifier.head,
              table = dc.qualifier.last,
              name = dc.name
            )))
          })
        }
      case c: Aggregate =>
        logDebug(s"[ColumnLineage] findColumns, Aggregate, " +
          s"aggregateExpressions: ${c.aggregateExpressions.toString()}")
        if (!c.aggregateExpressions.isEmpty) {
          for ( ag <- c.aggregateExpressions) {
            logDebug(s"[ColumnLineage] findColumns, Aggregate, " +
              s"name: ${ag.name}, " +
              s"qualifiedName: ${ag.qualifiedName}")
            val ags = ag.name.split(" AS ")
            if (ags.length == 2) {
              val reg = "([A-z])+#(\\d)+".r
//              column.get.child.++(Some(ColumnLineage(
//                db = "output", table = "output", name = ags.last.split("#").head
//              )))
              reg.findAllMatchIn(ags.head).foreach(oriCol =>
                findColumns(c.children, column, oriCol.group(1)))
            }
          }
        }
      case c: Project =>
        logDebug(s"[ColumnLineage] findColumns, Project, " +
          s"projectList: ${c.projectList}")
        for (p <- c.projectList) {
          val ags = p.name.split(" AS ")
          if (ags.length == 2) {
            findColumns(c.children, column, ags.head)
          }
        }
      case e =>
        logDebug(s"[ColumnLineage] findColumns, Other, " +
          s"e: ${e}")
        if (!e.children.isEmpty) {
          findColumns(e.children, column, parentColumn)
        }
    })
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
