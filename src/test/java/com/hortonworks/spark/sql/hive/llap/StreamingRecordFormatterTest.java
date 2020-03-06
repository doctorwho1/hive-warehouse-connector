package com.hortonworks.spark.sql.hive.llap;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.hortonworks.spark.sql.hive.llap.record.formatter.DummyStreamingDataSource;
import com.hortonworks.spark.sql.hive.llap.writers.streaming.StreamingRecordFormatter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Test;
import static com.hortonworks.spark.sql.hive.llap.writers.streaming.StreamingRecordFormatter.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamingRecordFormatterTest extends SessionTestBase {

  private static final String RESOURCES_BASE_PATH = "src/test/resources/record_formatter/";
  public static final String DUMMY_STREAMING_DATA_SOURCE = "com.hortonworks.spark.sql.hive.llap.record.formatter.DummyStreamingDataSource";

  @Test
  public void testSimpleTypes() {
    StructType schema = new StructType()
        .add("bytetype", DataTypes.ByteType)
        .add("shorttype", DataTypes.ShortType)
        .add("integertype", DataTypes.IntegerType)
        .add("longtype", DataTypes.LongType)
        .add("floattype", DataTypes.FloatType)
        .add("doubletype", DataTypes.DoubleType)
        .add("decimaltype", DataTypes.createDecimalType())
        .add("stringtype", DataTypes.StringType)
        .add("binarytype", DataTypes.BinaryType)
        .add("booleantype", DataTypes.BooleanType)
        .add("datetype", DataTypes.DateType)
        .add("timestamptype", DataTypes.TimestampType);

    Date date = Date.valueOf("2019-02-05");
    Object[] obj = {new Byte("1"),
        new Short("11"),
        121323,
        131231312313212L,
        new Float("3.14"),
        new Double("3123213123123123123123123.141222222"),
        new java.math.BigDecimal(3.15),
        "str",
        new byte[]{97, 98, 99, 100, 101},
        true,
        date,
        new java.sql.Timestamp(1475600502)};

    GenericRow genericRow = new GenericRow(obj);
    Dataset<Row> df = session.createDataFrame(Lists.newArrayList(genericRow), schema);

    byte[] expected = {49, 1, 49, 49, 1, 49, 50, 49, 51, 50, 51, 1, 49, 51, 49, 50, 51, 49, 51, 49, 50, 51,
        49, 51, 50, 49, 50, 1, 51, 46, 49, 52, 1, 51, 46, 49, 50, 51, 50, 49, 51, 49, 50, 51, 49, 50, 51, 49, 50, 51,
        69, 50, 52, 1, 51, 1, 115, 116, 114, 1, 89, 87, 74, 106, 90, 71, 85, 61, 1, 116, 114, 117, 101, 1, 50,
        48, 49, 57, 45, 48, 50, 45, 48, 53, 1, 49, 57, 55, 48, 45, 48, 49, 45, 49, 56, 32, 48, 55, 58,
        50, 51, 58, 50, 48, 46, 53, 48, 50};

    df.write().format(DUMMY_STREAMING_DATA_SOURCE).save();

    byte[] actual = DummyStreamingDataSource.RESULT.get(0);
    assertTrue(Arrays.equals(expected, actual));
  }

  @Test
  public void testStructInArray() {

    String path = RESOURCES_BASE_PATH + "test.json";
    Dataset<Row> json = session.read()
        .option("multiline", "true")
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        .json(path);

    json.write().format(DUMMY_STREAMING_DATA_SOURCE).save();

    byte[] expected = {48, 3, 48, 3, 48, 3, 68, 86, 69, 2, 48, 3, 56, 3, 56, 3, 69, 87, 66, 2, 48, 3, 48, 3, 48,
        3, 84, 83, 87, 2, 45, 51, 3, 50, 3, 50, 3, 84, 82, 69, 2, 50, 3, 50, 3, 50, 3, 83, 68, 83};

    byte[] actual = DummyStreamingDataSource.RESULT.get(0);
    assertTrue(Arrays.equals(expected, actual));
  }

  @Test
  public void testStructInArrayMultiRowsAndCols() {

    String path = RESOURCES_BASE_PATH + "test_with_nulls_multiple_rows.json";
    Dataset<Row> json = session.read()
        .option("multiline", "true")
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        .json(path);

    json.write().format(DUMMY_STREAMING_DATA_SOURCE).save();

    byte[] expected = {48, 3, 48, 3, 48, 3, 68, 86, 69, 2, 92, 78, 3, 56, 3, 56, 3, 69, 87, 66, 2, 48, 3, 48, 3, 48,
        3, 84, 83, 87, 2, 45, 51, 3, 50, 3, 50, 3, 84, 82, 69, 2, 50, 3, 50, 3, 50, 3, 83, 68, 83, 2, 92, 78, 1, 48,
        3, 48, 3, 48, 3, 68, 86, 69, 2, 92, 78, 3, 56, 3, 56, 3, 69, 87, 66, 2, 48, 3, 48, 3, 48, 3, 84, 83, 87, 2,
        45, 51, 3, 50, 3, 50, 3, 84, 82, 69, 2, 50, 3, 50, 3, 50, 3, 83, 68, 83, 2, 92, 78};

    assertTrue(Arrays.equals(expected, DummyStreamingDataSource.RESULT.get(0)));
    assertTrue(Arrays.equals(expected, DummyStreamingDataSource.RESULT.get(1)));
  }

  @Test
  public void testArrayInsideStruct() {

    String path = RESOURCES_BASE_PATH + "test_array_inside_struct.json";
    Dataset<Row> json = session.read()
        .option("multiline", "true")
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        .json(path);

    json.write().format(DUMMY_STREAMING_DATA_SOURCE).save();
    byte[] expected = {97, 49, 3, 98, 49, 2, 97, 50, 4, 98, 50, 2, 97, 51, 5, 98, 51};
    assertTrue(Arrays.equals(expected, DummyStreamingDataSource.RESULT.get(0)));

  }

  @Test
  public void testTimestampSchemaUnsafeRow() throws IOException {
    long microSecs = 1548811552012000L;
    byte[] buffer = new byte[100];
    UnsafeRow record = new UnsafeRow(2);
    record.pointTo(buffer, 100);
    record.setInt(0, 1);
    record.setLong(1, microSecs);

    StructType timeSchema = new StructType()
        .add("c1", "int")
        .add("c2", "timestamp");

    StreamingRecordFormatter formatter = new Builder(timeSchema)
        .withFieldDelimiter(DEFAULT_FIELD_DELIMITER)
        .withCollectionDelimiter(DEFAULT_COLLECTION_DELIMITER)
        .withMapKeyDelimiter(DEFAULT_MAP_KEY_DELIMITER)
        .withNullRepresentationString(DEFAULT_NULL_REPRESENTATION_STRING)
        .withTableProperties(new Properties())
        .build();

    //for timezone differences...we cannot keep some absolute value here
    long millis = TimeUnit.MILLISECONDS.convert(microSecs, TimeUnit.MICROSECONDS);
    String expected = new SimpleDateFormat(StreamingRecordFormatter.HIVE_DATE_FORMAT).format(new Date(millis));

    byte[] actual = formatter.format(record).toByteArray();
    //actual has format integer(0) separator(1) timestamp(2..)
    assertEquals('1', actual[0]);
    int i = 2;
    for (byte b : expected.getBytes()) {
      assertEquals(b, actual[i++]);
    }
  }

  @Override
  @After
  public void tearDown() {
    DummyStreamingDataSource.RESULT.clear();
  }
}