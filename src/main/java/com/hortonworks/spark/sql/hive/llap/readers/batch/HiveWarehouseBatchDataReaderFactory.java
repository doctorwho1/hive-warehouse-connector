package com.hortonworks.spark.sql.hive.llap.readers.batch;

import com.hortonworks.spark.sql.hive.llap.common.CommonBroadcastInfo;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class HiveWarehouseBatchDataReaderFactory implements DataReaderFactory<ColumnarBatch> {
    private byte[] splitBytes;
    private byte[] confBytes;
    private transient InputSplit split;
    private long arrowAllocatorMax;
    private CommonBroadcastInfo commonBroadcastInfo;

    //No-arg constructor for executors
    public HiveWarehouseBatchDataReaderFactory() {}

    //Driver-side setup
    public HiveWarehouseBatchDataReaderFactory(InputSplit split, byte[] serializedJobConf, long arrowAllocatorMax,
                                               CommonBroadcastInfo commonBroadcastInfo) {
        this.split = split;
        this.arrowAllocatorMax = arrowAllocatorMax;
        this.commonBroadcastInfo = commonBroadcastInfo;
        this.confBytes = serializedJobConf;
        ByteArrayOutputStream splitByteArrayStream = new ByteArrayOutputStream();

        try(DataOutputStream splitByteData = new DataOutputStream(splitByteArrayStream)) {
            //Serialize split for executors
            split.write(splitByteData);
            splitBytes = splitByteArrayStream.toByteArray();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String[] preferredLocations() {
        try {
            return this.split.getLocations();
        } catch(Exception e) {
            //preferredLocations specifies to return empty array if no preference
            return new String[0];
        }
    }

    @Override
    public DataReader<ColumnarBatch> createDataReader() {
        LlapInputSplit llapInputSplit = new LlapInputSplit();
        ByteArrayInputStream splitByteArrayStream = new ByteArrayInputStream(splitBytes);
        ByteArrayInputStream confByteArrayStream = new ByteArrayInputStream(confBytes);
        JobConf conf = new JobConf();

        try(DataInputStream splitByteData = new DataInputStream(splitByteArrayStream);
            DataInputStream confByteData = new DataInputStream(confByteArrayStream)) {
            llapInputSplit.readFields(splitByteData);
            conf.readFields(confByteData);
            return getDataReader(llapInputSplit, conf, arrowAllocatorMax, commonBroadcastInfo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected DataReader<ColumnarBatch> getDataReader(LlapInputSplit split, JobConf jobConf, long arrowAllocatorMax, CommonBroadcastInfo commonBroadcastInfo)
        throws Exception {
        return new HiveWarehouseBatchDataReader(split, jobConf, arrowAllocatorMax, commonBroadcastInfo);
    }
}
