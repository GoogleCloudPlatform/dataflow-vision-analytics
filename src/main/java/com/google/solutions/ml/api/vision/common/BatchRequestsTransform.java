package com.google.solutions.ml.api.vision.common;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.Random;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;

@AutoValue
public abstract class BatchRequestsTransform extends
    PTransform<PCollection<String>, PCollection<Iterable<String>>> {

  private static final long serialVersionUID = 1L;

  public abstract long getBatchSize();
  public abstract int getKeyRange();

  public static BatchRequestsTransform create(long newBatchSize, int newKeyRange) {
    return builder()
        .setBatchSize(newBatchSize)
        .setKeyRange(newKeyRange)
        .build();
  }

  @Override
  public PCollection<Iterable<String>> expand(PCollection<String> input) {
    if (getBatchSize() > 1) {
      return input
          .apply("Assign Keys", WithKeys.of(new SerializableFunction<String, Integer>() {
            private static final long serialVersionUID = 1L;
            private Random random = new Random();
            @Override
            public Integer apply(String input) {
              return random.nextInt(getKeyRange());
            }
          }))
          .apply("Group Into Batches", GroupIntoBatches.ofSize(getBatchSize()))
          .apply("Convert to Batches", Values.create());
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

    public abstract Builder setKeyRange(int newKeyRange);

    public abstract BatchRequestsTransform build();
  }
}
