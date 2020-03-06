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

package com.hortonworks.spark.sql.hive.llap.record.formatter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import com.hortonworks.spark.sql.hive.llap.writers.streaming.StreamingRecordFormatter;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.writer.*;
import org.apache.spark.sql.types.StructType;
import static com.hortonworks.spark.sql.hive.llap.writers.streaming.StreamingRecordFormatter.*;


public class DummyStreamingDataSource implements DataSourceV2, WriteSupport, Serializable {

  public static final List<byte[]> RESULT = new ArrayList<>();

  @Override
  public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
    return Optional.of(new DummySteamingDataSourceWriter(schema));
  }

  public class DummyStreamingDataWriter implements DataWriter<InternalRow>, Serializable {

    private StructType schema;
    private DataSourceOptions options;
    private StreamingRecordFormatter formatter;

    public DummyStreamingDataWriter(StructType schema) {
      this.schema = schema;
      this.options = options;
      this.formatter = new StreamingRecordFormatter.Builder(schema)
          .withFieldDelimiter(DEFAULT_FIELD_DELIMITER)
          .withCollectionDelimiter(DEFAULT_COLLECTION_DELIMITER)
          .withMapKeyDelimiter(DEFAULT_MAP_KEY_DELIMITER)
          .withNullRepresentationString(DEFAULT_NULL_REPRESENTATION_STRING)
          .withTableProperties(new Properties())
          .build();
    }

    @Override
    public void write(InternalRow record) throws IOException {
      RESULT.add(formatter.format(record).toByteArray());
    }

    @Override
    public WriterCommitMessage commit() {
      return null;
    }

    @Override
    public void abort() {
    }
  }

  public class DummySteamingDataSourceWriter implements SupportsWriteInternalRow, Serializable {

    private StructType schema;

    public DummySteamingDataSourceWriter(StructType schema) {
      this.schema = schema;
    }

    @Override
    public DataWriterFactory<InternalRow> createInternalRowWriterFactory() {
      return new DataWriterFactory<InternalRow>() {
        @Override
        public DataWriter<InternalRow> createDataWriter(int partitionId, int attemptNumber) {
          return new DummyStreamingDataWriter(schema);
        }
      };
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
    }
  }


}