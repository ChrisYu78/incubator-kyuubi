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
package org.apache.kyuubi.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable, OptimizedCreateHiveTableAsSelectCommand}

import org.apache.kyuubi.sql.KyuubiSQLConf._

case class SecurityCheck(session: SparkSession) extends (LogicalPlan => Unit)
  with SQLConfHelper {
  override def apply(plan: LogicalPlan): Unit = {
    if (conf.getConf(SECURITY_CHECK)) {
      plan match {
        case i @ InsertIntoHiveTable(_, _, _, _, _, _) =>
          checkDatabaseName(Option(i.table.database), "InsertIntoHiveTable")
        case i @ InsertIntoHadoopFsRelationCommand(_, _, _, _, _, _, _, _, _, _, _, _) =>
          checkDatabaseName(
            Option(i.catalogTable.get.database),
            "InsertIntoHadoopFsRelationCommand")
        case i @ CreateDatabaseCommand(_, _, _, _, _) =>
          checkDatabaseName(Option(i.databaseName), "CreateDatabaseCommand")
        case i @ DropDatabaseCommand(_, _, _) =>
          checkDatabaseName(Option(i.databaseName), "DropDatabaseCommand")
        case i @ CreateTableCommand(_, _) =>
          checkDatabaseName(Option(i.table.database), "CreateTableCommand")
        case i @ CreateTableLikeCommand(_, _, _, _, _, _) =>
          checkDatabaseName(i.targetTable.database, "CreateTableLikeCommand")
        case i @ CreateHiveTableAsSelectCommand(_, _, _, _) =>
          checkDatabaseName(Option(i.tableDesc.database), "CreateHiveTableAsSelectCommand")
        case i @ OptimizedCreateHiveTableAsSelectCommand(_, _, _, _) =>
          checkDatabaseName(Option(i.tableDesc.database), "OptimizedCreateHiveTableAsSelectCommand")
        case i @ CreateDataSourceTableAsSelectCommand(_, _, _, _) =>
          checkDatabaseName(Option(i.table.database), "CreateDataSourceTableAsSelectCommand")
        case i @ DropTableCommand(_, _, _, _) =>
          checkDatabaseName(i.tableName.database, "DropTableCommand")
        case i @ AlterDatabaseSetLocationCommand(_, _) =>
          checkDatabaseName(Option(i.databaseName), "AlterDatabaseSetLocationCommand")
        case i @ AlterDatabasePropertiesCommand(_, _) =>
          checkDatabaseName(Option(i.databaseName), "AlterDatabasePropertiesCommand")
        case i @ AlterTableSetPropertiesCommand(_, _, _) =>
          checkDatabaseName(i.tableName.database, "AlterTableSetPropertiesCommand")
        case i @ AlterTableUnsetPropertiesCommand(_, _, _, _) =>
          checkDatabaseName(i.tableName.database, "AlterTableUnsetPropertiesCommand")
        case i @ AlterTableChangeColumnCommand(_, _, _) =>
          checkDatabaseName(i.tableName.database, "AlterTableChangeColumnCommand")
        case i @ AlterTableSerDePropertiesCommand(_, _, _, _) =>
          checkDatabaseName(i.tableName.database, "AlterTableSerDePropertiesCommand")
        case i @ AlterTableAddPartitionCommand(_, _, _) =>
          checkDatabaseName(i.tableName.database, "AlterTableAddPartitionCommand")
        case i @ AlterTableRenamePartitionCommand(_, _, _) =>
          checkDatabaseName(i.tableName.database, "AlterTableRenamePartitionCommand")
        case i @ AlterTableRenameCommand(_, _, _) =>
          checkDatabaseName(i.newName.database, "AlterTableRenameCommand")
        case i @ AlterTableAddColumnsCommand(_, _) =>
          checkDatabaseName(i.table.database, "AlterTableAddColumnsCommand")
        case i @ AlterTableDropPartitionCommand(_, _, _, _, _) =>
          checkDatabaseName(i.tableName.database, "AlterTableDropPartitionCommand")
        case i @ RepairTableCommand(_, _, _, _) =>
          checkDatabaseName(i.tableName.database, "RepairTableCommand")
        case i @ AlterTableSetLocationCommand(_, _, _) =>
          checkDatabaseName(i.tableName.database, "AlterTableSetLocationCommand")
        case i @ DropFunctionCommand(_, _, _, _) =>
          checkDatabaseName(i.databaseName, "DropFunctionCommand")
        case i @ TruncateTableCommand(_, _) =>
          checkDatabaseName(i.tableName.database, "TruncateTableCommand")
        case i @ LoadDataCommand(_, _, _, _, _) =>
          checkDatabaseName(i.table.database, "LoadDataCommand")
        case _ => Unit
      }
    }
  }

  private def checkDatabaseName(
      databaseName: Option[String],
      commandName: String) = {
    val specifiedDatabasePrefix = conf.getConf(SPECIFIED_DATABASE_PREFIX)
    val currentDatabase = session.sessionState.catalog.getCurrentDatabase
    val dbName = databaseName.getOrElse(currentDatabase)
    if (!dbName.startsWith(specifiedDatabasePrefix)) {
      throw new IllegalAccessException(
        s"The command[$commandName] is not allowed execution " +
          s"in database[$dbName] with prefixes other than `$specifiedDatabasePrefix`")
    }
  }
}
