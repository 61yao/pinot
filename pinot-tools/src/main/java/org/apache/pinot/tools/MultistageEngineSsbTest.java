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
package org.apache.pinot.tools;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class MultistageEngineSsbTest extends Quickstart {

  protected Connection _h2Connection;

  @Override
  public List<String> types() {
    return Collections.singletonList("MULTI_STAGE_SSB");
  }

  protected void setUpH2Connection()
      throws Exception {
    Class.forName("org.h2.Driver");
    _h2Connection = DriverManager.getConnection("jdbc:h2:mem:");
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    Map<String, String> queryOptions = Collections.singletonMap("queryOptions",
        CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE + "=true");

    setUpH2Connection();
    List<String> testTables = Arrays.asList("dates", "customer", "lineorder", "part", "supplier");
    Map<String, String> createTableSqlMap = new HashMap<>();
    createTableSqlMap.put("dates",
        "CREATE TABLE dates(D_DATEKEY INT, D_DATE VARCHAR(18), D_DAYOFWEEK VARCHAR(9), D_MONTH VARCHAR(9), D_YEAR "
            + "INT, D_YEARMONTHNUM INT, D_YEARMONTH VARCHAR(7), D_DAYNUMINWEEK INT, D_DAYNUMINMONTH INT, "
            + "D_DAYNUMINYEAR INT, D_MONTHNUMINYEAR INT, D_WEEKNUMINYEAR INT, D_SELLINGSEASON VARCHAR(12), "
            + "D_LASTDAYINWEEKFL INT, D_LASTDAYINMONTHFL INT, D_HOLIDAYFL INT, D_WEEKDAYFL INT)");
    createTableSqlMap.put("customer",
        "CREATE TABLE customer(C_CUSTKEY INT, C_NAME VARCHAR(25), C_ADDRESS VARCHAR(25), C_CITY VARCHAR(10) ,C_NATION VARCHAR(15), C_REGION VARCHAR(12), C_PHONE VARCHAR(15), C_MKTSEGMENT VARCHAR(10))");
    createTableSqlMap.put("lineorder",
        "CREATE TABLE lineorder(LO_ORDERKEY INT, LO_LINENUMBER INT, LO_CUSTKEY INT, LO_PARTKEY INT,LO_SUPPKEY INT,LO_ORDERDATE INT, LO_ORDERPRIORITY VARCHAR(15),"
            + "LO_SHIPPRIORITY VARCHAR(1), LO_QUANTITY INT, LO_EXTENDEDPRICE INT, LO_ORDTOTALPRICE INT, LO_DISCOUNT INT, LO_REVENUE INT, LO_SUPPLYCOST INT, LO_TAX INT,"
            + "LO_COMMITDATE INT, LO_SHIPMODE VARCHAR(10))");
    createTableSqlMap.put("part",
        "CREATE TABLE part(P_PARTKEY INT, P_NAME VARCHAR(22), P_MFGR VARCHAR(6), P_CATEGORY VARCHAR(7), P_BRAND1 VARCHAR(9), P_COLOR VARCHAR(11), P_TYPE VARCHAR(25), P_SIZE INT, P_CONTAINER VARCHAR(10))");
    createTableSqlMap.put("supplier",
        "CREATE TABLE supplier(S_SUPPKEY INT,S_NAME VARCHAR(25), S_ADDRESS VARCHAR(25), S_CITY VARCHAR(10), S_NATION VARCHAR(15), S_REGION VARCHAR(12), S_PHONE VARCHAR(15))");
    for(String table: testTables){
      File baseDir = new File(getTmpDir(), table);
      File dataDir = new File(baseDir, "rawdata");
      System.out.println("Creating h2 table:" + table);
      String createTableSql =
          createTableSqlMap.get(table) + " AS SELECT *  FROM CSVREAD('" + dataDir.getAbsolutePath() + "/" + table
              + ".csv', null, 'fieldSeparator=|');";
      QuickStartUtils.setUpH2Table(table, createTableSql, _h2Connection);
    }
    List<String> queries = new ArrayList<>();
//    queries.add("select sum(CAST(LO_EXTENDEDPRICE AS DOUBLE) * LO_DISCOUNT) as revenue from lineorder, dates where LO_ORDERDATE = D_DATEKEY and D_YEAR = 1993 and LO_DISCOUNT between 1 and 3 and LO_QUANTITY < 25;");
//    queries.add("select sum(CAST(LO_EXTENDEDPRICE AS DOUBLE) * LO_DISCOUNT) as revenue from lineorder, dates where LO_ORDERDATE = D_DATEKEY and D_YEARMONTHNUM = 199401 and LO_DISCOUNT between 4 and 6 and LO_QUANTITY between 26 and 35;");
//    queries.add("select sum(CAST(LO_EXTENDEDPRICE AS DOUBLE) * LO_DISCOUNT) as revenue from lineorder, dates where "
//        + "LO_ORDERDATE = D_DATEKEY and D_WEEKNUMINYEAR = 6 and D_YEAR = 1994 and LO_DISCOUNT between 5 and 7 and "
//        + "LO_QUANTITY between 26 and 35;");
    queries.add("select sum(CAST(LO_REVENUE AS DOUBLE)), D_YEAR, P_BRAND1 from lineorder, dates, part, supplier where"
        + " LO_ORDERDATE = D_DATEKEY and LO_PARTKEY = P_PARTKEY and LO_SUPPKEY = S_SUPPKEY and P_CATEGORY = 'MFGR#12'"
        + " and S_REGION = 'AMERICA' group by D_YEAR, P_BRAND1 order by D_YEAR, P_BRAND1;");
//    queries.add("select sum(CAST(LO_REVENUE AS DOUBLE)), D_YEAR, P_BRAND1 from lineorder, dates, part, supplier where"
//        + " LO_ORDERDATE = D_DATEKEY and LO_PARTKEY = P_PARTKEY and LO_SUPPKEY = S_SUPPKEY and P_BRAND1 between "
//        + "'MFGR#2221' and 'MFGR#2228' and S_REGION = 'ASIA' group by D_YEAR, P_BRAND1 order by D_YEAR, P_BRAND1;");
//    queries.add("select sum(CAST(LO_REVENUE AS DOUBLE)), D_YEAR, P_BRAND1 from lineorder, dates, part, supplier where"
//        + " LO_ORDERDATE = D_DATEKEY and LO_PARTKEY = P_PARTKEY and LO_SUPPKEY = S_SUPPKEY and P_BRAND1 = 'MFGR#2221'"
//        + " and S_REGION = 'EUROPE' group by D_YEAR, P_BRAND1 order by D_YEAR, P_BRAND1;");
    queries.add("select C_NATION, S_NATION, D_YEAR, sum(LO_REVENUE) as revenue from customer, lineorder, supplier, "
        + "dates where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY and LO_ORDERDATE = D_DATEKEY and C_REGION = "
        + "'ASIA' and S_REGION = 'ASIA' and D_YEAR >= 1992 and D_YEAR <= 1997 group by C_NATION, S_NATION, D_YEAR "
        + "order by D_YEAR asc, revenue desc;");
//    queries.add("select C_CITY, S_CITY, D_YEAR, sum(LO_REVENUE) as revenue from customer, lineorder, supplier, dates "
//        + "where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY and LO_ORDERDATE = D_DATEKEY and C_NATION = "
//        + "'UNITED STATES' and S_NATION = 'UNITED STATES' and D_YEAR >= 1992 and D_YEAR <= 1997 group by C_CITY, "
//        + "S_CITY, D_YEAR order by D_YEAR asc, revenue desc;");
//    queries.add("select C_CITY, S_CITY, D_YEAR, sum(LO_REVENUE) as revenue from customer, lineorder, supplier, dates "
//        + "where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY and LO_ORDERDATE = D_DATEKEY and (C_CITY='UNITED "
//        + "KI1' or C_CITY='UNITED KI5') and (S_CITY='UNITED KI1' or S_CITY='UNITED KI5') and D_YEAR >= 1992 and "
//        + "D_YEAR <= 1997 group by C_CITY, S_CITY, D_YEAR order by D_YEAR asc, revenue desc;");
//    queries.add("select C_CITY, S_CITY, D_YEAR, sum(LO_REVENUE) as revenue from customer, lineorder, supplier, dates "
//        + "where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY and LO_ORDERDATE = D_DATEKEY and (C_CITY='UNITED "
//        + "KI1' or C_CITY='UNITED KI5') and (S_CITY='UNITED KI1' or S_CITY='UNITED KI5') and D_YEARMONTH = 'Dec1997' "
//        + "group by C_CITY, S_CITY, D_YEAR order by D_YEAR asc, revenue desc;");
    queries.add("select D_YEAR, C_NATION, sum(LO_REVENUE - LO_SUPPLYCOST) as profit from  lineorder, customer, "
        + "supplier, part,  dates where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY and LO_PARTKEY = P_PARTKEY "
        + "and LO_ORDERDATE = D_DATEKEY and C_REGION = 'AMERICA' and S_REGION = 'AMERICA' and (P_MFGR = 'MFGR#1' or "
        + "P_MFGR = 'MFGR#2') group by D_YEAR, C_NATION order by D_YEAR, C_NATION;");
//    queries.add("select D_YEAR, S_NATION, P_CATEGORY, sum(LO_REVENUE - LO_SUPPLYCOST) as profit from lineorder, "
//        + "dates, customer, supplier, part where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY and LO_PARTKEY = "
//        + "P_PARTKEY and LO_ORDERDATE = D_DATEKEY and C_REGION = 'AMERICA' and S_REGION = 'AMERICA' and (D_YEAR = "
//        + "1997 or D_YEAR = 1998) and (P_MFGR = 'MFGR#1' or P_MFGR = 'MFGR#2') group by D_YEAR, S_NATION, P_CATEGORY "
//        + "order by D_YEAR, S_NATION, P_CATEGORY;");
//    queries.add("select D_YEAR, S_CITY, P_BRAND1, sum(LO_REVENUE - LO_SUPPLYCOST) as profit from lineorder, dates, "
//        + "customer, supplier, part where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY and LO_PARTKEY = "
//        + "P_PARTKEY and LO_ORDERDATE = D_DATEKEY and C_REGION = 'AMERICA' and S_NATION = 'UNITED STATES' and (D_YEAR"
//        + " = 1997 or D_YEAR = 1998) and P_CATEGORY = 'MFGR#14' group by D_YEAR, S_CITY, P_BRAND1 order by D_YEAR, "
//        + "S_CITY, P_BRAND1;");
    for(String query: queries) {
      //try{
      System.out.println("====================testing query ===================");
      System.out.println(query);
      QuickStartUtils.testQuery(runner, query, _h2Connection, queryOptions);
      System.out.println("-----------------------------------------------------");
//      } catch (Exception e){
//        printStatus(Color.YELLOW, e.getMessage());
//      }
    }



    printStatus(Color.GREEN, "********************TEST PASSED *****************************");
  }

  @Override
  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> overrides = new HashMap<>(super.getConfigOverrides());
    overrides.put("pinot.multistage.engine.enabled", "true");
    overrides.put("pinot.server.instance.currentDataTableVersion", 4);
    return overrides;
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "MULTI_STAGE_SSB"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
