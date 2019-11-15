package com.hortonworks.spark.sql.hive.llap;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

public class SimpleWriterCommitMessage implements WriterCommitMessage {
  private String message;
  private Path writtenFilePath;

  SimpleWriterCommitMessage(String message, Path writtenFilePath) {
    this.message = message;
    this.writtenFilePath = writtenFilePath;
  }

  public String getMessage() {
    return message;
  }

  public Path getWrittenFilePath() {
    return writtenFilePath;
  }
}
