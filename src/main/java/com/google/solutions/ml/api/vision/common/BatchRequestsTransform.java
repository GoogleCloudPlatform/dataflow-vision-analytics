package com.google.solutions.ml.api.vision.common;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class BatchRequestsTransform extends
    PTransform<PCollection<String>, PCollection<Iterable<String>>> {

  private static final long serialVersionUID = 1L;

  public abstract long getBatchSize();

  public static BatchRequestsTransform create(long newBatchSize) {
    return builder()
        .setBatchSize(newBatchSize)
        .build();
  }

  @Override
  @SuppressWarnings("deprecation")
  public PCollection<Iterable<String>> expand(PCollection<String> input) {
    if (getBatchSize() > 1) {
      return input
          .apply("Assign Keys", WithKeys.of("1"))
          .apply("Group Into Batches", GroupIntoBatches.ofSize(getBatchSize()))
          .apply("Convert to Batches", Values.create())
//          .apply("Reshuffle", Reshuffle.viaRandomKey())
          ;
    } else {
      return input.apply("Convert to Iterable", ParDo.of(new DoFn<String, Iterable<String>>() {
        private final static long serialVersionUID = 1L;

        @ProcessElement
        public void process(@Element String element, OutputReceiver<Iterable<String>> out) {
          out.output(Collections.singleton(element));
        }
      }));
    }
  }

  public static Builder builder() {
    return new AutoValue_BatchRequestsTransform.Builder();
  }


  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setBatchSize(long newBatchSize);

    public abstract BatchRequestsTransform build();
  }
}
