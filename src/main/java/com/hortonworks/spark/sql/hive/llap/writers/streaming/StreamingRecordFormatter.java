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

package com.hortonworks.spark.sql.hive.llap.writers.streaming;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.*;
import scala.Serializable;

/**
 * Converts spark records (InternalRow) to the format required by hive streaming.
 *
 * Separator characters and escape logic taken from:
 * https://github.com/hortonworks/hive/blob/HDP-3.1-maint/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java
 */
public class StreamingRecordFormatter implements Serializable {

  public static final byte DEFAULT_FIELD_DELIMITER = '\001';
  public static final byte DEFAULT_COLLECTION_DELIMITER = '\002';
  public static final byte DEFAULT_MAP_KEY_DELIMITER = '\003';
  public static final String DEFAULT_NULL_REPRESENTATION_STRING = "\\N";
  public final static String HIVE_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  public static final LocalDate UNIX_EPOCH_DATE = LocalDate.parse("1970-01-01");

  private static SimpleDateFormat sdf = null;
  private byte[] separators;

  private final byte fieldDelimiter;
  private final byte collectionDelimiter;
  private final byte mapKeyDelimiter;
  private final StructType schema;
  private final String nullRepresentationString;

  private boolean escaped;
  private byte escapeChar;
  private boolean[] needsEscape = new boolean[256];  // A flag for each byte to indicate if escape is needed.
  private Properties tableProperties;


  public static class Builder {

    private byte fieldDelimiter = DEFAULT_FIELD_DELIMITER;
    private byte collectionDelimiter = DEFAULT_COLLECTION_DELIMITER;
    private byte mapKeyDelimiter = DEFAULT_MAP_KEY_DELIMITER;

    private String nullRepresentationString = DEFAULT_NULL_REPRESENTATION_STRING;

    private final StructType schema;
    private Properties tableProperties;

    public Builder(StructType schema) {
      this.schema = schema;
    }

    public StreamingRecordFormatter.Builder withFieldDelimiter(byte fieldDelimiter) {
      this.fieldDelimiter = fieldDelimiter;
      return this;
    }

    public StreamingRecordFormatter.Builder withCollectionDelimiter(byte collectionDelimiter) {
      this.collectionDelimiter = collectionDelimiter;
      return this;
    }

    public StreamingRecordFormatter.Builder withMapKeyDelimiter(byte mapKeyDelimiter) {
      this.mapKeyDelimiter = mapKeyDelimiter;
      return this;
    }

    public StreamingRecordFormatter.Builder withNullRepresentationString(String nullRepresentationString) {
      this.nullRepresentationString = nullRepresentationString;
      return this;
    }

    public StreamingRecordFormatter.Builder withTableProperties(Properties tableProperties) {
      this.tableProperties = tableProperties;
      return this;
    }

    public StreamingRecordFormatter build() {
      return new StreamingRecordFormatter(this);
    }

  }

  private StreamingRecordFormatter(StreamingRecordFormatter.Builder builder) {
    this.fieldDelimiter = builder.fieldDelimiter;
    this.collectionDelimiter = builder.collectionDelimiter;
    this.mapKeyDelimiter = builder.mapKeyDelimiter;
    this.nullRepresentationString = builder.nullRepresentationString;
    this.tableProperties = builder.tableProperties;
    this.schema = builder.schema;
    initializeSeparatorsAndEscapes();
  }


  /**
   * Separator characters and escape logic taken from:
   * https://github.com/hortonworks/hive/blob/HDP-3.1-maint/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java
   */
  private void initializeSeparatorsAndEscapes() {
    List<Byte> separatorCandidates = new ArrayList<>();

    separatorCandidates.add(fieldDelimiter);
    separatorCandidates.add(collectionDelimiter);
    separatorCandidates.add(mapKeyDelimiter);

    for (byte b = 4; b <= 8; b++) {
      separatorCandidates.add(b);
    }
    separatorCandidates.add((byte) 11);
    for (byte b = 14; b <= 26; b++) {
      separatorCandidates.add(b);
    }
    for (byte b = 28; b <= 31; b++) {
      separatorCandidates.add(b);
    }

    for (byte b = -128; b <= -1; b++) {
      separatorCandidates.add(b);
    }

    separators = new byte[separatorCandidates.size()];
    for (int i = 0; i < separatorCandidates.size(); i++) {
      separators[i] = separatorCandidates.get(i);
    }

    // Get the escape information
    String escapeProperty = tableProperties.getProperty(serdeConstants.ESCAPE_CHAR);
    escaped = (escapeProperty != null);
    if (escaped) {
      escapeChar = LazyUtils.getByte(escapeProperty, (byte) '\\');
      needsEscape[escapeChar & 0xFF] = true;  // Converts the negative byte into positive index
      for (byte b : separators) {
        needsEscape[b & 0xFF] = true;         // Converts the negative byte into positive index
      }

      boolean isEscapeCRLF =
          Boolean.parseBoolean(tableProperties.getProperty(serdeConstants.SERIALIZATION_ESCAPE_CRLF));
      if (isEscapeCRLF) {
        needsEscape['\r'] = true;
        needsEscape['\n'] = true;
      }
    }
  }

  public ByteArrayOutputStream format(InternalRow record) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(1024);
    for (int i = 0; i < record.numFields(); i++) {
      //separate each field
      if (i > 0) {
        outputStream.write(fieldDelimiter);
      }
      DataType dataType = schema.fields()[i].dataType();
      format(record.get(i, dataType), dataType, 1, outputStream);
    }
    return outputStream;
  }


  private void format(Object record, DataType dataType, int nestingLevel, ByteArrayOutputStream outputStream) throws IOException {
    ensureSupportedDataType(dataType);

    if (record == null) {
      outputStream.write(nullRepresentationString.getBytes());
      return;
    }

    byte separator = separators[nestingLevel];
    if (dataType instanceof StructType) {
      StructField[] fields = ((StructType) dataType).fields();
      InternalRow struct = (InternalRow) record;
      for (int i = 0; i < fields.length; i++) {
        if (i > 0) {
          outputStream.write(separator);
        }
        DataType fieldDataType = fields[i].dataType();
        Object rowElement = struct.get(i, fieldDataType);
        format(rowElement, fieldDataType, nestingLevel + 1, outputStream);
      }
    } else if (dataType instanceof ArrayType) {
      DataType arrayElementType = ((ArrayType) dataType).elementType();
      ArrayData arrayData = (ArrayData) record;
      for (int i = 0; i < arrayData.numElements(); i++) {
        if (i > 0) {
          outputStream.write(separator);
        }
        Object arrElement = arrayData.get(i, arrayElementType);
        format(arrElement, arrayElementType, nestingLevel + 1, outputStream);
      }
    } else {
      writePrimitive(record, dataType, outputStream);
    }
  }

  private void ensureSupportedDataType(DataType dataType) {
    if (dataType instanceof MapType || dataType instanceof CalendarIntervalType || dataType instanceof NullType) {
      throw new IllegalArgumentException("MapType/CalendarIntervalType/NullType not yet supported");
    }
  }

  private void writePrimitive(Object record, DataType dataType, ByteArrayOutputStream outputStream) throws IOException {
    if (DataTypes.StringType.equals(dataType)) {
      outputStream.write(formatStringType(record).getBytes());
    } else if (DataTypes.TimestampType.equals(dataType)) {
      String date = formatTimeStampType(record);
      if (date != null) {
        outputStream.write(date.getBytes());
      } else {
        outputStream.write(nullRepresentationString.getBytes());
      }
    } else if (DataTypes.DateType.equals(dataType)) {
      if (record instanceof Integer) {
        //spark internal representation of date is an ordinal from 1970-01-01
        LocalDate localDate = UNIX_EPOCH_DATE.plusDays((Integer) record);
        outputStream.write(localDate.toString().getBytes());
      } else {
        outputStream.write(nullRepresentationString.getBytes());
      }
    } else if (DataTypes.BinaryType.equals(dataType)) {
      byte[] bytes = (byte[]) record;
      outputStream.write(Base64.getEncoder().encode(bytes));
    } else {
      outputStream.write(record.toString().getBytes());
    }
  }


  private String formatTimeStampType(Object record) {
    if (record instanceof Long) {
      // We get from spark the time in microseconds
      long millis = TimeUnit.MILLISECONDS.convert((Long) record, TimeUnit.MICROSECONDS);
      Date date = new Date(millis);
      if (sdf == null) {
        sdf = new SimpleDateFormat(HIVE_DATE_FORMAT);
      }
      return sdf.format(date);
    }
    return null;
  }

  private String formatStringType(Object record) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    byte[] bytes = record.toString().getBytes();
    LazyUtils.writeEscaped(stream, bytes, 0, bytes.length, escaped, escapeChar, needsEscape);
    record = new String(stream.toByteArray());
    String quoteDelimiter = tableProperties.getProperty(serdeConstants.QUOTE_CHAR, null);
    //wrap with quoteDelimiter
    if (quoteDelimiter != null) {
      record = quoteDelimiter + record + quoteDelimiter;
    }
    return record.toString();
  }
}


