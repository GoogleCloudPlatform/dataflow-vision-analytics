package com.google.solutions.ml.api.vision;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class BQDestination implements Serializable {

  public static final long serialVersionUID = 1L;

  private String tableName;

  BQDestination() {
    // Needed for AvroCoder
  }

  public BQDestination(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BQDestination that = (BQDestination) o;
    return Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName);
  }
}
