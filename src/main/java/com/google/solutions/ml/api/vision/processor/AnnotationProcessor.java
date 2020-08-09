package com.google.solutions.ml.api.vision.processor;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.solutions.ml.api.vision.BQDestination;
import com.google.solutions.ml.api.vision.TableDetails;
import java.io.Serializable;
import org.apache.beam.sdk.values.KV;

public interface AnnotationProcessor extends Serializable {

  Iterable<KV<BQDestination, TableRow>> extractAnnotations(String gcsURI,
      AnnotateImageResponse response);

  TableDetails destinationTableDetails();
}