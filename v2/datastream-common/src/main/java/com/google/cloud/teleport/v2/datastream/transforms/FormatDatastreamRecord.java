/*
 * Copyright (C) 2018 Google LLC
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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An abstract class to cover the generic requirements Datastream row cleaning processes. */
public abstract class FormatDatastreamRecord<InputT, OutputT> extends DoFn<InputT, OutputT> {

  static final Logger LOG = LoggerFactory.getLogger(FormatDatastreamJsonToJson.class);

  protected String streamName;
  protected boolean lowercaseSourceColumns = false;
  protected Map<String, String> renameColumns = new HashMap<String, String>();
  protected boolean hashRowId = false;
  protected String datastreamSourceType;
  protected Boolean useMysqlTimestampFormat = false;

  static final String ROW_ID_CHARSET =
      "+/0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static final DateTimeFormatter MYSQL_TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);
  static final Map<Character, Long> ROW_ID_CHARSET_MAP =
      LongStream.range(0, ROW_ID_CHARSET.length())
          .mapToObj(i -> i)
          .collect(Collectors.toMap(i -> ROW_ID_CHARSET.charAt(i.intValue()), i -> i));

  /**
   * Set the map of columns values to rename/copy.
   *
   * @param renameColumns The map of columns to new columns to rename/copy.
   */
  public FormatDatastreamRecord withRenameColumnValues(Map<String, String> renameColumns) {
    this.renameColumns = renameColumns;
    return this;
  }

  public FormatDatastreamRecord withLowercaseSourceColumns(Boolean lowercaseSourceColumns) {
    this.lowercaseSourceColumns = lowercaseSourceColumns;
    return this;
  }

  public FormatDatastreamRecord withStreamName(String streamName) {
    this.streamName = streamName;
    return this;
  }

  /** Set the reader to hash Oracle ROWID values into int. */
  public FormatDatastreamRecord withHashRowId(Boolean hashRowId) {
    this.hashRowId = hashRowId;
    return this;
  }

  /** Set the Datastream source type override. */
  public FormatDatastreamRecord withDatastreamSourceType(String datastreamSourceType) {
    this.datastreamSourceType = datastreamSourceType;
    return this;
  }

  /** Set whether to format timestamps in MySQL format. */
  public FormatDatastreamRecord withMysqlTimestampFormat(Boolean useMysqlTimestampFormat) {
    this.useMysqlTimestampFormat = useMysqlTimestampFormat;
    return this;
  }

  /**
   * Format an ISO 8601 timestamp string to MySQL format if useMysqlTimestampFormat is enabled.
   *
   * @param isoTimestamp The ISO 8601 timestamp string to format
   * @return The formatted timestamp string (MySQL format if enabled, original otherwise)
   */
  protected String formatTimestampIfNeeded(String isoTimestamp) {
    if (!useMysqlTimestampFormat || isoTimestamp == null) {
      return isoTimestamp;
    }

    try {
      Instant instant = Instant.parse(isoTimestamp);
      return MYSQL_TIMESTAMP_FORMAT.format(instant);
    } catch (Exception e) {
      LOG.warn("Failed to parse timestamp: {}. Returning original value.", isoTimestamp, e);
      return isoTimestamp;
    }
  }

  protected void applyRenameColumns(ObjectNode outputObject) {
    applyRenameColumns(outputObject, this.renameColumns);
  }

  protected static void applyRenameColumns(
      ObjectNode outputObject, Map<String, String> renameColumns) {
    for (String columnName : renameColumns.keySet()) {
      if (outputObject.get(columnName) != null) {
        String newColumnName = renameColumns.get(columnName);
        outputObject.put(newColumnName, outputObject.get(columnName));
      }
    }
  }

  protected void setOracleRowIdValue(ObjectNode outputObject, String rowId) {
    setOracleRowIdValue(outputObject, rowId, this.hashRowId);
  }

  protected static void setOracleRowIdValue(
      ObjectNode outputObject, String rowId, Boolean hashRowId) {
    if (hashRowId) {
      outputObject.put("_metadata_row_id", hashRowIdToInt(rowId));
    } else {
      outputObject.put("_metadata_row_id", rowId);
    }
  }

  /** Hash an Oracle ROWID into a unique identifier for a row which fits in a long. */
  protected static long hashRowIdToInt(String rowId) {
    if (rowId == null) {
      LOG.warn("Oracle RowId is null: \"{}\"", rowId);
      return -1;
    } else if (rowId.length() != 18) {
      LOG.warn("Oracle RowId Invalid Length: \"{}\" -> {}", rowId, rowId.length());
      return -1;
    } else if (!rowId.matches("[a-zA-Z0-9+/]*")) {
      LOG.warn("Oracle RowId Invalid Value: \"{}\"", rowId);
      return -1;
    }

    String rowLocationData = rowId.substring(8);
    return LongStream.range(0, rowLocationData.length())
        .map(i -> ROW_ID_CHARSET_MAP.get(rowLocationData.charAt((int) i)) * (long) Math.pow(64, i))
        .sum();
  }
}
