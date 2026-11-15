/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.data.Envelope;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests for {@link RecordUtils#upsertBinlog} on primary key updates. */
class UpsertBinlogTest {

    private static final String TOPIC = "mysql_binlog_source.test.customers";

    private final Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA);
    private final Schema rowSchema =
            SchemaBuilder.struct()
                    .field("id", Schema.INT64_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA);
    private final Schema sourceSchema =
            SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build();
    private final Envelope envelope =
            Envelope.defineSchema()
                    .withName(Envelope.schemaName(TOPIC + ".Envelope"))
                    .withRecord(rowSchema)
                    .withSource(sourceSchema)
                    .build();
    private final RowType splitKeyType =
            new RowType(Collections.singletonList(new RowType.RowField("id", new BigIntType())));

    private SourceRecord snapshotRecord(long id, String name) {
        Struct after = new Struct(rowSchema).put("id", id).put("name", name);
        Struct value =
                envelope.read(
                        after,
                        new Struct(sourceSchema).put("ts_ms", 0L),
                        java.time.Instant.ofEpochMilli(0L));
        Struct key = new Struct(keySchema).put("id", id);
        return new SourceRecord(
                Collections.singletonMap("server", "mysql"),
                offset(1L),
                TOPIC,
                keySchema,
                key,
                envelope.schema(),
                value);
    }

    private SourceRecord updateRecord(long oldId, long newId, String name) {
        Struct before = new Struct(rowSchema).put("id", oldId).put("name", "old");
        Struct after = new Struct(rowSchema).put("id", newId).put("name", name);
        Struct value =
                envelope.update(
                        before,
                        after,
                        new Struct(sourceSchema).put("ts_ms", 1L),
                        java.time.Instant.ofEpochMilli(1L));
        // Debezium exposes the new (after-image) primary key in the record key.
        Struct key = new Struct(keySchema).put("id", newId);
        return new SourceRecord(
                Collections.singletonMap("server", "mysql"),
                offset(2L),
                TOPIC,
                keySchema,
                key,
                envelope.schema(),
                value);
    }

    private SourceRecord deleteRecord(long oldId) {
        Struct before = new Struct(rowSchema).put("id", oldId).put("name", "old");
        Struct value =
                envelope.delete(
                        before,
                        new Struct(sourceSchema).put("ts_ms", 1L),
                        java.time.Instant.ofEpochMilli(1L));
        Struct key = new Struct(keySchema).put("id", oldId);
        return new SourceRecord(
                Collections.singletonMap("server", "mysql"),
                offset(2L),
                TOPIC,
                keySchema,
                key,
                envelope.schema(),
                value);
    }

    private SourceRecord createRecord(long newId, String name, Long oldKeyInHeader) {
        Struct after = new Struct(rowSchema).put("id", newId).put("name", name);
        Struct value =
                envelope.create(
                        after,
                        new Struct(sourceSchema).put("ts_ms", 1L),
                        java.time.Instant.ofEpochMilli(1L));
        Struct key = new Struct(keySchema).put("id", newId);
        SourceRecord record =
                new SourceRecord(
                        Collections.singletonMap("server", "mysql"),
                        offset(3L),
                        TOPIC,
                        keySchema,
                        key,
                        envelope.schema(),
                        value);
        if (oldKeyInHeader != null) {
            record.headers()
                    .add(
                            RelationalChangeRecordEmitter.PK_UPDATE_OLDKEY_FIELD,
                            new Struct(keySchema).put("id", oldKeyInHeader),
                            keySchema);
        }
        return record;
    }

    private void applyBackfill(
            Map<Struct, List<SourceRecord>> snapshotRecords,
            List<SourceRecord> binlogRecords,
            Object[] splitStart,
            Object[] splitEnd) {
        for (SourceRecord binlogRecord : binlogRecords) {
            RecordUtils.upsertBinlog(
                    snapshotRecords,
                    binlogRecord,
                    splitKeyType,
                    SchemaNameAdjuster.create(),
                    splitStart,
                    splitEnd);
        }
    }

    private Map<String, ?> offset(long position) {
        Map<String, Object> offset = new HashMap<>();
        offset.put("file", "mysql-bin.000001");
        offset.put("pos", position);
        return offset;
    }

    private Map<Struct, List<SourceRecord>> snapshotWith(long id, String name) {
        Map<Struct, List<SourceRecord>> snapshotRecords = new HashMap<>();
        SourceRecord record = snapshotRecord(id, name);
        snapshotRecords.put((Struct) record.key(), Collections.singletonList(record));
        return snapshotRecords;
    }

    private List<Long> emittedIds(Map<Struct, List<SourceRecord>> snapshotRecords) {
        List<Long> ids = new ArrayList<>();
        snapshotRecords.values().stream()
                .flatMap(List::stream)
                .forEach(
                        record -> {
                            Struct after =
                                    ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
                            ids.add(after.getInt64("id"));
                        });
        return ids;
    }

    @Test
    void testPrimaryKeyUpdateWithinSameSplit() {
        Map<Struct, List<SourceRecord>> snapshotRecords = snapshotWith(1L, "old");

        RecordUtils.upsertBinlog(
                snapshotRecords,
                updateRecord(1L, 2L, "new"),
                splitKeyType,
                SchemaNameAdjuster.create(),
                null,
                null);

        // The old primary key row must be removed, only the new row survives.
        Assertions.assertThat(emittedIds(snapshotRecords)).containsExactly(2L);
    }

    @Test
    void testPrimaryKeyUpdateWithinSameSplitAsDeleteAndCreate() {
        // Debezium emits a primary key change as a DELETE of the old key followed by a CREATE of
        // the new key, linked by the __debezium.oldkey / __debezium.newkey headers.
        Map<Struct, List<SourceRecord>> snapshotRecords = snapshotWith(1L, "old");

        applyBackfill(
                snapshotRecords,
                java.util.Arrays.asList(
                        deleteRecordWithNewKeyHeader(1L, 2L), createRecord(2L, "new", 1L)),
                null,
                null);

        Assertions.assertThat(emittedIds(snapshotRecords)).containsExactly(2L);
    }

    @Test
    void testPrimaryKeyUpdateLeavingSplitRemovesStaleSnapshotRow() {
        // Current split only covers [0, 100). The row moves to primary key 200 which belongs to
        // another split. The stale snapshot row with the old primary key must be cleaned up so no
        // duplicate / stale record is emitted from this split.
        Map<Struct, List<SourceRecord>> snapshotRecords = snapshotWith(50L, "old");

        applyBackfill(
                snapshotRecords,
                java.util.Arrays.asList(
                        deleteRecordWithNewKeyHeader(50L, 200L), createRecord(200L, "new", 50L)),
                new Object[] {0L},
                new Object[] {100L});

        Assertions.assertThat(emittedIds(snapshotRecords)).isEmpty();
    }

    @Test
    void testSingleUpdateLeavingSplitRemovesStaleSnapshotRow() {
        // Even if Debezium keeps the change as a single UPDATE record, the before-image key 50
        // belongs to the current split [0, 100). The stale snapshot row keyed by 50 must be
        // removed and replaced by the after-image row so the old row is never emitted.
        Map<Struct, List<SourceRecord>> snapshotRecords = snapshotWith(50L, "old");

        RecordUtils.upsertBinlog(
                snapshotRecords,
                updateRecord(50L, 200L, "new"),
                splitKeyType,
                SchemaNameAdjuster.create(),
                new Object[] {0L},
                new Object[] {100L});

        // The outer gate uses the before-image key, so the record is processed by this split.
        // The old row (50) must no longer be present; only the moved row (200) remains. The
        // destination split will drop this move later via its high-watermark filter, keeping the
        // global result exactly-once.
        Assertions.assertThat(emittedIds(snapshotRecords)).containsExactly(200L);
        Assertions.assertThat(snapshotRecords)
                .doesNotContainKey(new Struct(keySchema).put("id", 50L));
    }

    @Test
    void testNonPrimaryKeyUpdateKeepsSingleRow() {
        // The primary key stays the same, only a non-key column changes. The snapshot row must be
        // replaced in place, still a single row under the same key.
        Map<Struct, List<SourceRecord>> snapshotRecords = snapshotWith(1L, "old");

        RecordUtils.upsertBinlog(
                snapshotRecords,
                updateRecord(1L, 1L, "new"),
                splitKeyType,
                SchemaNameAdjuster.create(),
                null,
                null);

        Assertions.assertThat(emittedIds(snapshotRecords)).containsExactly(1L);
    }

    @Test
    void testDeleteRemovesSnapshotRow() {
        Map<Struct, List<SourceRecord>> snapshotRecords = snapshotWith(1L, "old");

        RecordUtils.upsertBinlog(
                snapshotRecords,
                deleteRecord(1L),
                splitKeyType,
                SchemaNameAdjuster.create(),
                null,
                null);

        Assertions.assertThat(emittedIds(snapshotRecords)).isEmpty();
    }

    @Test
    void testCreateInsertsNewRow() {
        Map<Struct, List<SourceRecord>> snapshotRecords = snapshotWith(1L, "old");

        RecordUtils.upsertBinlog(
                snapshotRecords,
                createRecord(2L, "inserted", null),
                splitKeyType,
                SchemaNameAdjuster.create(),
                null,
                null);

        Assertions.assertThat(emittedIds(snapshotRecords)).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void testCompositePrimaryKeyUpdateRemovesStaleRow() {
        Schema compositeKeySchema =
                SchemaBuilder.struct()
                        .field("id", Schema.INT64_SCHEMA)
                        .field("tenant", Schema.STRING_SCHEMA);
        Schema compositeRowSchema =
                SchemaBuilder.struct()
                        .field("id", Schema.INT64_SCHEMA)
                        .field("tenant", Schema.STRING_SCHEMA)
                        .field("name", Schema.STRING_SCHEMA);
        Envelope compositeEnvelope =
                Envelope.defineSchema()
                        .withName(Envelope.schemaName(TOPIC + ".CompositeEnvelope"))
                        .withRecord(compositeRowSchema)
                        .withSource(sourceSchema)
                        .build();

        Struct oldKey = new Struct(compositeKeySchema).put("id", 1L).put("tenant", "a");
        Struct snapshotAfter =
                new Struct(compositeRowSchema).put("id", 1L).put("tenant", "a").put("name", "old");
        Struct snapshotValue =
                compositeEnvelope.read(
                        snapshotAfter,
                        new Struct(sourceSchema).put("ts_ms", 0L),
                        java.time.Instant.ofEpochMilli(0L));
        Map<Struct, List<SourceRecord>> snapshotRecords = new HashMap<>();
        snapshotRecords.put(
                oldKey,
                Collections.singletonList(
                        new SourceRecord(
                                Collections.singletonMap("server", "mysql"),
                                offset(1L),
                                TOPIC,
                                compositeKeySchema,
                                oldKey,
                                compositeEnvelope.schema(),
                                snapshotValue)));

        Struct newKey = new Struct(compositeKeySchema).put("id", 2L).put("tenant", "a");
        Struct before =
                new Struct(compositeRowSchema).put("id", 1L).put("tenant", "a").put("name", "old");
        Struct after =
                new Struct(compositeRowSchema).put("id", 2L).put("tenant", "a").put("name", "new");
        Struct updateValue =
                compositeEnvelope.update(
                        before,
                        after,
                        new Struct(sourceSchema).put("ts_ms", 1L),
                        java.time.Instant.ofEpochMilli(1L));
        SourceRecord updateRecord =
                new SourceRecord(
                        Collections.singletonMap("server", "mysql"),
                        offset(2L),
                        TOPIC,
                        compositeKeySchema,
                        newKey,
                        compositeEnvelope.schema(),
                        updateValue);

        RecordUtils.upsertBinlog(
                snapshotRecords,
                updateRecord,
                splitKeyType,
                SchemaNameAdjuster.create(),
                null,
                null);

        Assertions.assertThat(snapshotRecords).hasSize(1).containsOnlyKeys(newKey);
    }

    private SourceRecord deleteRecordWithNewKeyHeader(long oldId, long newId) {
        SourceRecord record = deleteRecord(oldId);
        record.headers()
                .add(
                        RelationalChangeRecordEmitter.PK_UPDATE_NEWKEY_FIELD,
                        new Struct(keySchema).put("id", newId),
                        keySchema);
        return record;
    }
}
