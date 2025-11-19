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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.datastream.sources.DataStreamIO;
import com.google.cloud.teleport.v2.templates.DataStreamToSQL.Options;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link DataStreamToSQL}. */
@RunWith(JUnit4.class)
public final class DataStreamToSQLTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testParseMappings() {
    // Test empty, and single member parse
    Map<String, Map<String, String>> mappings = DataStreamToSQL.parseMappings("");
    assertEquals(mappings.get("schemas").size(), 0);
    assertEquals(mappings.get("tables").size(), 0);

    mappings = DataStreamToSQL.parseMappings("schema1:schema2");
    assertEquals(mappings.get("schemas").size(), 1);
    assertEquals(mappings.get("tables").size(), 0);
    assertEquals(mappings.get("schemas").get("schema1"), "schema2");

    // Test multi-member parse
    mappings = DataStreamToSQL.parseMappings("schema1:schema2|schemaA.table1:schemaB.table2");
    assertEquals(mappings.get("schemas").size(), 1);
    assertEquals(mappings.get("tables").size(), 1);
    assertEquals(mappings.get("schemas").get("schema1"), "schema2");
    assertEquals(mappings.get("tables").get("schemaA.table1"), "schemaB.table2");

    // Test advanced multi-member parse
    mappings =
        DataStreamToSQL.parseMappings(
            "s1:s2|s1.t1:s2.t2|s1.t3:s2.t4|s3:s4|s3.t5:s4.t6|s5.t7:s6.t8|s7:s8");
    assertEquals(mappings.get("schemas").size(), 3);
    assertEquals(mappings.get("tables").size(), 4);
    assertEquals(mappings.get("schemas").get("s1"), "s2");
    assertEquals(mappings.get("schemas").get("s3"), "s4");
    assertEquals(mappings.get("schemas").get("s7"), "s8");
    assertEquals(mappings.get("tables").get("s1.t1"), "s2.t2");
    assertEquals(mappings.get("tables").get("s1.t3"), "s2.t4");
    assertEquals(mappings.get("tables").get("s3.t5"), "s4.t6");
    assertEquals(mappings.get("tables").get("s5.t7"), "s6.t8");
  }

  @Test
  public void testGetDataSourceConfiguration() {
    Options options = PipelineOptionsFactory.create().as(Options.class);
    options.setDatabaseType("mysql");
    options.setDatabaseHost("1.2.3.4");
    options.setDatabasePort("3306");
    options.setDatabaseName("test_db");
    options.setDatabaseUser("user");
    options.setDatabasePassword("password");

    assertEquals(
        "com.mysql.cj.jdbc.Driver",
        DataStreamToSQL.getDataSourceConfiguration(options).getDriverClassName().get());
    assertEquals(
        "jdbc:mysql://1.2.3.4:3306/test_db",
        DataStreamToSQL.getDataSourceConfiguration(options).getUrl().get());
  }

  @Test
  public void testDataStreamIOWithMysqlTimestampFormat() {
    Options options = PipelineOptionsFactory.create().as(Options.class);
    options.setUseMysqlTimestampFormat(true);

    DataStreamIO dataStreamIO =
        new DataStreamIO(
            options.getStreamName(),
            options.getInputFilePattern(),
            options.getInputFileFormat(),
            options.getGcsPubSubSubscription(),
            options.getRfcStartDateTime());

    DataStreamIO transformedIO = dataStreamIO.withMysqlTimestampFormat(options.getUseMysqlTimestampFormat());

    assertTrue(transformedIO.getUseMysqlTimestampFormat());
  }
}
