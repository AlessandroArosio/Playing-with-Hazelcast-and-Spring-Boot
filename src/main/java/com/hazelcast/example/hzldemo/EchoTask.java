package com.hazelcast.example.hzldemo;

import java.io.Serializable;

public class EchoTask implements Runnable, Serializable {

  private Integer taskNumber;

  public EchoTask(Integer taskNumber) {
    this.taskNumber = taskNumber;
  }

  @Override
  public void run() {
    System.out.println("Executing task " + taskNumber);
  }
}
