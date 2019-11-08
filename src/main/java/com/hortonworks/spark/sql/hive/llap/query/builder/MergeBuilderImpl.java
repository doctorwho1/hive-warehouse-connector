package com.hortonworks.spark.sql.hive.llap.query.builder;

import com.hortonworks.hwc.MergeBuilder;
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession;
import org.apache.commons.lang3.StringUtils;

import static java.lang.String.join;

public class MergeBuilderImpl implements MergeBuilder {

  private final HiveWarehouseSession hive;
  private final String database;

  private String targetTable;
  private String targetAlias;
  private String sourceTableOrExpr;
  private String sourceAlias;
  private String onExpr;
  private String updateExpr;
  private String[] updateNameValuePairs;
  private boolean deleteRequired;
  private String deleteExpr;
  private String[] insertValues;
  private String insertExpr;

  public MergeBuilderImpl(HiveWarehouseSession hive, String database) {
    this.hive = hive;
    this.database = database;
  }

  @Override
  public MergeBuilder mergeInto(String targetTable, String alias) {
    this.targetTable = targetTable;
    this.targetAlias = alias;
    return this;
  }

  @Override
  public MergeBuilder using(String sourceTableOrExpr, String alias) {
    this.sourceTableOrExpr = sourceTableOrExpr;
    this.sourceAlias = alias;
    return this;
  }

  @Override
  public MergeBuilder on(String expr) {
    this.onExpr = expr;
    return this;
  }

  @Override
  public MergeBuilder whenMatchedThenUpdate(String matchExpr, String... nameValuePairs) {
    this.updateExpr = matchExpr;
    this.updateNameValuePairs = nameValuePairs;
    return this;
  }

  @Override
  public MergeBuilder whenMatchedThenDelete(String matchExpr) {
    this.deleteRequired = true;
    this.deleteExpr = matchExpr;
    return this;
  }

  @Override
  public MergeBuilder whenNotMatchedInsert(String matchExpr, String... values) {
    this.insertExpr = matchExpr;
    this.insertValues = values;
    return this;
  }

  @Override
  public void merge() {
    hive.executeUpdate(this.toString(), true);
  }

  @Override
  public String toString() {
    StringBuilder mergeQueryBuilder = new StringBuilder();

    //MERGE INTO db.targetTable AS targetAlias USING sourceTableOrExpr AS sourceAlias ON onExpr
    mergeQueryBuilder.append(String.format("MERGE INTO %s.%s AS %s USING %s AS %s ON %s", database, targetTable, targetAlias,
        sourceTableOrExpr, sourceAlias, onExpr));

    // WHEN MATCHED AND (matchExpr) THEN UPDATE SET updateNameValuePairs[0], updateNameValuePairs[1]
    if (updateNameValuePairs != null && updateNameValuePairs.length > 0) {
      mergeQueryBuilder.append(whenMatchedAndExpr(false, updateExpr));
      mergeQueryBuilder.append(" THEN UPDATE SET ").append(join(",", updateNameValuePairs));
    }

    // WHEN MATCHED AND (matchExpr) THEN DELETE
    if (deleteRequired) {
      mergeQueryBuilder.append(whenMatchedAndExpr(false, deleteExpr));
      mergeQueryBuilder.append(" THEN DELETE");
    }

    // WHEN NOT MATCHED AND (matchExpr) THEN INSERT VALUES (insertValues[0], insertValues[1]...);
    if (insertValues != null && insertValues.length > 0) {
      mergeQueryBuilder.append(whenMatchedAndExpr(true, insertExpr));
      mergeQueryBuilder.append(" THEN INSERT VALUES (").append(join(",", insertValues)).append(")");
    }

    return mergeQueryBuilder.toString();
  }


  private String whenMatchedAndExpr(boolean not, String matchExpr) {
    String matchString = " WHEN MATCHED ";
    if (not) {
      matchString = " WHEN NOT MATCHED ";
    }
    if (StringUtils.isNotBlank(matchExpr)) {
      matchString = matchString + "AND (" + matchExpr + ") ";
    }
    return matchString;
  }

}
