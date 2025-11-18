/*
 * Copyright (C) 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.datastream.transforms;

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for FormatDatastreamRecordToJson function. These check appropriate Avro-to-Json conv. */
@RunWith(JUnit4.class)
public class FormatDatastreamJsonToJsonTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String EXAMPLE_DATASTREAM_JSON =
      "{\"uuid\":\"00c32134-f50e-4460-a6c0-399900010010\",\"read_timestamp\":\"2021-12-25"
          + " 05:42:04.408\","
          + "\"source_timestamp\":\"2021-12-25T05:42:04.408\",\"object\":\"HR_JOBS\",\"read_method\":\"oracle-backfill\",\"stream_name\":\"projects/402074789819/locations/us-central1/streams/destroy\",\"schema_key\":\"ebdb5545a7610cee7b1caae4a45dec7fd3b46fdc\",\"sort_keys\":[1640410924408,1706664,\"\",0],\"source_metadata\":{\"schema\":\"HR\",\"table\":\"JOBS\",\"database\":\"XE\",\"row_id\":\"AAAEARAAEAAAAC9AAS\",\"scn\":1706664,\"is_deleted\":false,\"change_type\":\"INSERT\",\"ssn\":0,\"rs_id\":\"\",\"tx_id\":null,\"log_file\":\"\",\"primary_keys\":[\"JOB_ID\"]},\"payload\":{\"JOB_ID\":\"PR_REP\",\"JOB_TITLE\":\"Public"
          + " Relations Representative\",\"MIN_SALARY\":4500,\"MAX_SALARY\":10500}}";

  private static final String EXAMPLE_DATASTREAM_RECORD =
      "{\"_metadata_stream\":\"my-stream\",\"_metadata_timestamp\":1640410924,\"_metadata_read_timestamp\":1640410924,\"_metadata_read_method\":\"oracle-backfill\",\"_metadata_source_type\":\"oracle\",\"_metadata_deleted\":false,\"_metadata_database\":\"XE\",\"_metadata_schema\":\"HR\",\"_metadata_table\":\"JOBS\",\"_metadata_change_type\":\"INSERT\",\"_metadata_primary_keys\":[\"JOB_ID\"],\"_metadata_uuid\":\"00c32134-f50e-4460-a6c0-399900010010\",\"_metadata_row_id\":\"AAAEARAAEAAAAC9AAS\",\"_metadata_scn\":1706664,\"_metadata_ssn\":0,\"_metadata_rs_id\":\"\",\"_metadata_tx_id\":null,\"JOB_ID\":\"PR_REP\",\"JOB_TITLE\":\"Public"
          + " Relations"
          + " Representative\",\"MIN_SALARY\":4500,\"MAX_SALARY\":10500,\"rowid\":\"AAAEARAAEAAAAC9AAS\",\"_metadata_source\":{\"schema\":\"HR\",\"table\":\"JOBS\",\"database\":\"XE\",\"row_id\":\"AAAEARAAEAAAAC9AAS\",\"scn\":1706664,\"is_deleted\":false,\"change_type\":\"INSERT\",\"ssn\":0,\"rs_id\":\"\",\"tx_id\":null,\"log_file\":\"\",\"primary_keys\":[\"JOB_ID\"]}}";

  private static final String EXAMPLE_DATASTREAM_RECORD_WITH_HASH_ROWID =
      "{\"_metadata_stream\":\"my-stream\",\"_metadata_timestamp\":1640410924,\"_metadata_read_timestamp\":1640410924,\"_metadata_read_method\":\"oracle-backfill\",\"_metadata_source_type\":\"oracle\",\"_metadata_deleted\":false,\"_metadata_database\":\"XE\",\"_metadata_schema\":\"HR\",\"_metadata_table\":\"JOBS\",\"_metadata_change_type\":\"INSERT\",\"_metadata_primary_keys\":[\"JOB_ID\"],\"_metadata_uuid\":\"00c32134-f50e-4460-a6c0-399900010010\",\"_metadata_row_id\":1019670290924988842,\"_metadata_scn\":1706664,\"_metadata_ssn\":0,\"_metadata_rs_id\":\"\",\"_metadata_tx_id\":null,\"JOB_ID\":\"PR_REP\",\"JOB_TITLE\":\"Public"
          + " Relations"
          + " Representative\",\"MIN_SALARY\":4500,\"MAX_SALARY\":10500,\"rowid\":1019670290924988842,\"_metadata_source\":{\"schema\":\"HR\",\"table\":\"JOBS\",\"database\":\"XE\",\"row_id\":\"AAAEARAAEAAAAC9AAS\",\"scn\":1706664,\"is_deleted\":false,\"change_type\":\"INSERT\",\"ssn\":0,\"rs_id\":\"\",\"tx_id\":null,\"log_file\":\"\",\"primary_keys\":[\"JOB_ID\"]}}";

  @Test
  public void testProcessElement_validJson() {
    Map<String, String> renameColumns = ImmutableMap.of("_metadata_row_id", "rowid");

    FailsafeElement<String, String> expectedElement =
        FailsafeElement.of(EXAMPLE_DATASTREAM_RECORD, EXAMPLE_DATASTREAM_RECORD);

    FailsafeElementCoder<String, String> failsafeElementCoder =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    PCollection<FailsafeElement<String, String>> pCollection =
        pipeline
            .apply("CreateInput", Create.of(EXAMPLE_DATASTREAM_JSON))
            .apply(
                "FormatDatastreamJsonToJson",
                ParDo.of(
                    (FormatDatastreamJsonToJson)
                        FormatDatastreamJsonToJson.create()
                            .withStreamName("my-stream")
                            .withRenameColumnValues(renameColumns)
                            .withLowercaseSourceColumns(false)))
            .setCoder(failsafeElementCoder)
            .apply("RemoveTimestampProperty", ParDo.of(new RemoveTimestampPropertyFn()))
            .setCoder(failsafeElementCoder);

    PAssert.that(pCollection).containsInAnyOrder(expectedElement);

    pipeline.run();
  }

  @Test
  public void testProcessElement_hashRowId() {
    Map<String, String> renameColumns = ImmutableMap.of("_metadata_row_id", "rowid");

    FailsafeElement<String, String> expectedElement =
        FailsafeElement.of(
            EXAMPLE_DATASTREAM_RECORD_WITH_HASH_ROWID, EXAMPLE_DATASTREAM_RECORD_WITH_HASH_ROWID);

    FailsafeElementCoder<String, String> failsafeElementCoder =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    PCollection<FailsafeElement<String, String>> pCollection =
        pipeline
            .apply("CreateInput", Create.of(EXAMPLE_DATASTREAM_JSON))
            .apply(
                "FormatDatastreamJsonToJson",
                ParDo.of(
                    (FormatDatastreamJsonToJson)
                        FormatDatastreamJsonToJson.create()
                            .withStreamName("my-stream")
                            .withRenameColumnValues(renameColumns)
                            .withHashRowId(true)
                            .withLowercaseSourceColumns(false)))
            .setCoder(failsafeElementCoder)
            .apply("RemoveDataflowTimestampProperty", ParDo.of(new RemoveTimestampPropertyFn()))
            .setCoder(failsafeElementCoder);

    PAssert.that(pCollection).containsInAnyOrder(expectedElement);

    pipeline.run();
  }

  @Test
  public void testProcessElement_mysqlTimestampFormat() throws JsonProcessingException {
    String jsonWithTimestamps =
        "{\"uuid\":\"test-uuid-001\",\"read_timestamp\":\"2023-11-13 10:30:00.123\","
            + "\"source_timestamp\":\"2023-11-13T10:30:00.123Z\",\"object\":\"TEST_TABLE\",\"read_method\":\"mysql-cdc\",\"stream_name\":\"test-stream\",\"schema_key\":\"test-key\",\"source_metadata\":{\"schema\":\"test_schema\",\"table\":\"test_table\",\"database\":\"test_db\",\"is_deleted\":false,\"change_type\":\"INSERT\",\"primary_keys\":[\"id\"]},\"payload\":{\"id\":1,\"created_at\":\"2023-11-13T08:15:30Z\",\"updated_at\":\"2023-11-13T10:30:45.678Z\",\"name\":\"Test Record\"}}";

    Map<String, String> renameColumns = ImmutableMap.of();

    FailsafeElementCoder<String, String> failsafeElementCoder =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    PCollection<FailsafeElement<String, String>> pCollection =
        pipeline
            .apply("CreateInput", Create.of(jsonWithTimestamps))
            .apply(
                "FormatDatastreamJsonToJson",
                ParDo.of(
                    (FormatDatastreamJsonToJson)
                        FormatDatastreamJsonToJson.create()
                            .withStreamName("test-stream")
                            .withRenameColumnValues(renameColumns)
                            .withMysqlTimestampFormat(true)
                            .withLowercaseSourceColumns(false)))
            .setCoder(failsafeElementCoder);

    PAssert.that(pCollection)
        .satisfies(
            (SerializableFunction<Iterable<FailsafeElement<String, String>>, Void>)
                elements -> {
                  ObjectMapper mapper = new ObjectMapper();
                  for (FailsafeElement<String, String> element : elements) {
                    try {
                      JsonNode changeEvent = mapper.readTree(element.getPayload());
                      String createdAt = changeEvent.get("created_at").asText();
                      String updatedAt = changeEvent.get("updated_at").asText();

                      // Verify MySQL format: yyyy-MM-dd HH:mm:ss
                      assertTrue(
                          "created_at should be in MySQL format",
                          createdAt.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"));
                      assertTrue(
                          "updated_at should be in MySQL format",
                          updatedAt.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"));
                    } catch (JsonProcessingException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return null;
                });

    pipeline.run();
  }

  @Test
  public void testProcessElement_isoTimestampFormat() throws JsonProcessingException {
    String jsonWithTimestamps =
        "{\"uuid\":\"test-uuid-002\",\"read_timestamp\":\"2023-11-13 10:30:00.123\","
            + "\"source_timestamp\":\"2023-11-13T10:30:00.123Z\",\"object\":\"TEST_TABLE\",\"read_method\":\"mysql-cdc\",\"stream_name\":\"test-stream\",\"schema_key\":\"test-key\",\"source_metadata\":{\"schema\":\"test_schema\",\"table\":\"test_table\",\"database\":\"test_db\",\"is_deleted\":false,\"change_type\":\"INSERT\",\"primary_keys\":[\"id\"]},\"payload\":{\"id\":1,\"created_at\":\"2023-11-13T08:15:30Z\",\"updated_at\":\"2023-11-13T10:30:45.678Z\",\"name\":\"Test Record\"}}";

    Map<String, String> renameColumns = ImmutableMap.of();

    FailsafeElementCoder<String, String> failsafeElementCoder =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    PCollection<FailsafeElement<String, String>> pCollection =
        pipeline
            .apply("CreateInput", Create.of(jsonWithTimestamps))
            .apply(
                "FormatDatastreamJsonToJson",
                ParDo.of(
                    (FormatDatastreamJsonToJson)
                        FormatDatastreamJsonToJson.create()
                            .withStreamName("test-stream")
                            .withRenameColumnValues(renameColumns)
                            .withMysqlTimestampFormat(false)
                            .withLowercaseSourceColumns(false)))
            .setCoder(failsafeElementCoder);

    PAssert.that(pCollection)
        .satisfies(
            (SerializableFunction<Iterable<FailsafeElement<String, String>>, Void>)
                elements -> {
                  ObjectMapper mapper = new ObjectMapper();
                  for (FailsafeElement<String, String> element : elements) {
                    try {
                      JsonNode changeEvent = mapper.readTree(element.getPayload());
                      String createdAt = changeEvent.get("created_at").asText();
                      String updatedAt = changeEvent.get("updated_at").asText();

                      // Verify ISO format: yyyy-MM-ddTHH:mm:ss[.fraction]Z
                      assertTrue(
                          "created_at should be in ISO format",
                          createdAt.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z"));
                      assertTrue(
                          "updated_at should be in ISO format",
                          updatedAt.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z"));
                    } catch (JsonProcessingException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return null;
                });

    pipeline.run();
  }

  // Static nested DoFn class to remove timestamp property
  static class RemoveTimestampPropertyFn
      extends DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>> {

    @ProcessElement
    public void processElement(
        @Element FailsafeElement<String, String> element,
        OutputReceiver<FailsafeElement<String, String>> out)
        throws JsonProcessingException {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode changeEvent = mapper.readTree(element.getPayload());
      if (changeEvent instanceof ObjectNode) {
        ((ObjectNode) changeEvent).remove("_metadata_dataflow_timestamp");
      }
      out.output(FailsafeElement.of(changeEvent.toString(), changeEvent.toString()));
    }
  }

  // Validates that timestamps in payload are in MySQL format
}
