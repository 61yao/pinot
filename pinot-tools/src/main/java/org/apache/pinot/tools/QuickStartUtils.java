package org.apache.pinot.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.map.HashedMap;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class QuickStartUtils {
  /**
   * Set up an H2 table with records from the given Avro files inserted.
   *
   * @param tableName Name of the table to be created
   * @param h2Connection H2 connection
   * @throws Exception
   */
  @SuppressWarnings("SqlNoDataSourceInspection")
  public static void setUpH2Table(String tableName, String createTableSql, Connection h2Connection)
      throws Exception {
    h2Connection.prepareCall("DROP TABLE IF EXISTS " + tableName).execute();
    h2Connection.prepareCall(createTableSql).execute();
  }

  static void testQuery(QuickstartRunner runner,
      String query, Connection h2Connection, Map<String, String> queryOptions)
      throws Exception {
    JsonNode pinotResponse = runner.runQuery(query, queryOptions);
    if (!pinotResponse.get("exceptions").isEmpty()) {
      throw new RuntimeException("Got Exceptions from Query Response: " + pinotResponse + " query:" + query);
    }
    List<List<String>> pinotResultList = new ArrayList<>();
    List<String> columnsNames = new ArrayList<>();
    int numColumns = 0;
    int numRows = 0;
    Map<String, Integer> pinotNameToIdx = new HashedMap();
    if (pinotResponse.has("resultTable")) {
      JsonNode columns = pinotResponse.get("resultTable").get("dataSchema").get("columnNames");
      numColumns = 0;
      for(int i = 0; i < columns.size(); i++){
//        System.out.println("liuyao: column" + i + " " + columns.get(i).asText());
        if(columns.get(i).asText().equals("$hostName") || columns.get(i).asText().equals("$docId") || columns.get(i).asText().equals("$segmentName")) {
          continue;
        }
        ++numColumns;
        columnsNames.add(columns.get(i).asText());
        pinotNameToIdx.put(columns.get(i).asText(), i);
      }
      JsonNode rows = pinotResponse.get("resultTable").get("rows");
      numRows = rows.size();
      for (int i = 0; i < numRows; i++) {
        JsonNode row = rows.get(i);
        List<String> rowArr = new ArrayList<>();
//        System.out.println("============row============" + i + "==============");
        for(String column: columnsNames){
//          System.out.println(row.get(pinotNameToIdx.get(column)).asText());
          rowArr.add(row.get(pinotNameToIdx.get(column)).asText());
        }
        pinotResultList.add(rowArr);
      }
    }
    System.out.println("get pinot result");

    // h2 response
    Statement h2statement = h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    h2statement.execute(query);
    ResultSet h2ResultSet = h2statement.getResultSet();
    ResultSetMetaData h2MetaData = h2ResultSet.getMetaData();
    System.out.println("get h2 result");

    int h2NumColumns = h2MetaData.getColumnCount();
    if(h2NumColumns != numColumns){
      throw new RuntimeException("num columns is different: h2NumColumns:" + h2NumColumns + " numColumns:" + numColumns + " query:" + query);
    }
    Map<String, Integer> nameToIdx = new HashedMap();
    for(int i = 1; i <= h2MetaData.getColumnCount(); i++){
      nameToIdx.put(h2MetaData.getColumnName(i), i);
    }
    int h2NumRows = 0;
    List<List<String>> h2ResultList = new ArrayList<>();
    while (h2ResultSet.next()){
      ++h2NumRows;
      ArrayList<String> row = new ArrayList<>();
      for(String column: columnsNames){
        System.out.println("coulunName:" + column);
        row.add(h2ResultSet.getString(nameToIdx.get(column)));
      }
      h2ResultList.add(row);
    }
    if(h2NumRows != numRows){
      throw new RuntimeException("num rows is different h2 num rows:" + h2NumRows + " pinot rows" + numRows + " query:" + query);
    }

    // compare unordered results
    Set<List<String>> pinotSet = new HashSet<List<String>>(pinotResultList);
    Set<List<String>> h2Set = new HashSet<List<String>>(h2ResultList);
    if(!pinotSet.equals(h2Set)){
      throw new RuntimeException("result is different query:" + query);
    }
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    Set<String> orderByColumns = new HashSet<>();
    if (queryContext.getOrderByExpressions() != null) {
      for (OrderByExpressionContext orderByExpression : queryContext.getOrderByExpressions()) {
        orderByExpression.getColumns(orderByColumns);
      }
    }
    // TODO: handle order by
    if(!orderByColumns.isEmpty() && !pinotResultList.equals(h2ResultList)){
      throw new RuntimeException("result order is different");
    }
  }
}