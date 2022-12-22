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
package org.apache.spark.sql

import org.apache.kyuubi.sql.KyuubiSQLConf

class SecurityCheckSuite extends KyuubiSparkSQLExtensionTest {

  test("Check Production Operations") {
    sql("CREATE TABLE IF NOT EXISTS test2(i int) PARTITIONED BY (p int)").collect()
    withSQLConf(KyuubiSQLConf.SECURITY_CHECK.key -> "true") {
      // drop nonexistent database
      //      val df10 = sql("Create DATABASE tmp_yuqi").collect()
      //      val df1 = sql("DROP DATABASE  IF EXISTS tmp_yuqi")
      //      val analyzed = df1.queryExecution.analyzed
      //      val command = analyzed.asInstanceOf[DropDatabaseCommand]
      //      assert(command.databaseName == "tmp_yuqi")

      // drop nonexistent PARTITION

      intercept[IllegalAccessException](
        sql("CREATE TABLE test3 like test2").collect())
      sql("Create DATABASE tmp_yuqi").collect()
      sql("use tmp_yuqi")
      sql("CREATE TABLE test6 like default.test2")
      sql("show databases").collect()
      withTable("test") {
        sql("CREATE TABLE IF NOT EXISTS tmp_yuqi.test(i int) PARTITIONED BY (p int)")
        sql("show tables in default").collect()
        sql("show tables in tmp_yuqi").collect()
        // val df22 = sql("DROP TABLE tmp_yuqi.test")
        intercept[IllegalAccessException](sql("DROP TABLE default.test2"))
        sql("insert into tmp_yuqi.test partition(p = 1) select 999 ").collect()
        sql("create table tmp_yuqi.test999 as select * from tmp_yuqi.test ").collect()
        sql("select * from tmp_yuqi.test999").collect()
        intercept[IllegalAccessException](
          sql("insert into default.test2 partition(p = 1) select 999 "))
      }
    }
  }

  test("create or drop databases") {
    withSQLConf(KyuubiSQLConf.SECURITY_CHECK.key -> "true") {
      sql("Create DATABASE tmp_yuqi999")
      sql("Create DATABASE tmp_yuqi2")
      sql("Create DATABASE tmp_yuqi3")
      print(sql("show databases").collect().length + "\n")
      intercept[IllegalAccessException](
        sql("Create DATABASE hhahah"))
      sql("DROP DATABASE  IF EXISTS tmp_yuqi")
      sql("drop DATABASE tmp_yuqi2")
      print(sql("show databases").collect().length + "\n")
    }
  }
}
