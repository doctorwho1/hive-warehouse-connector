package com.hortonworks.spark.sql.hive.llap.readers.row;

import com.hortonworks.spark.sql.hive.llap.common.CommonBroadcastInfo;
import org.apache.hadoop.hive.llap.LlapInputSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.Row;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class HiveWarehouseDataReaderFactory implements DataReaderFactory<Row> {
    private byte[] splitBytes;
    private byte[] confBytes;
    private transient InputSplit split;
    private long arrowAllocatorMax;
    private CommonBroadcastInfo commonBroadcastInfo;

    //No-arg constructor for executors
    public HiveWarehouseDataReaderFactory() {}

    //Driver-side setup
    public HiveWarehouseDataReaderFactory(InputSplit split, byte[] serializedJobConf,
                                          long arrowAllocatorMax, CommonBroadcastInfo commonBroadcastInfo) {
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
    public DataReader<Row> createDataReader() {
        LlapInputSplit llapInputSplit = new LlapInputSplit();
        ByteArrayInputStream splitByteArrayStream = new ByteArrayInputStream(splitBytes);
        ByteArrayInputStream confByteArrayStream = new ByteArrayInputStream(confBytes);
        JobConf conf = new JobConf();

        try(DataInputStream splitByteData = new DataInputStream(splitByteArrayStream);
            DataInputStream confByteData = new DataInputStream(confByteArrayStream)) {
            llapInputSplit.readFields(splitByteData);
            conf.readFields(confByteData);
            return getDataReader(llapInputSplit, conf, commonBroadcastInfo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected DataReader<Row> getDataReader(LlapInputSplit split, JobConf jobConf, CommonBroadcastInfo commonBroadcastInfo)
        throws Exception {
        return new HiveWarehouseDataReader(split, jobConf, arrowAllocatorMax, commonBroadcastInfo);
    }
}
