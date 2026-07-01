/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for table with vector. */
public class VectorTypeTableTest extends TableTestBase {

    private final float[] testVector = randomVector();

    @Test
    public void testBasic() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(100, 1));

        AtomicInteger integer = new AtomicInteger(0);

        readDefault(
                row -> {
                    integer.incrementAndGet();
                    if (integer.get() % 50 == 0) {
                        Assertions.assertArrayEquals(
                                row.getVector(2).toFloatArray(), testVector, 0);
                    }
                });

        assertThat(integer.get()).isEqualTo(100);
    }

    @Test
    public void testCompactVectorStoreFiles() throws Exception {
        int vectorLength = testVector.length;
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.VECTOR(vectorLength, DataTypes.FLOAT()));
        schemaBuilder.option(CoreOptions.FILE_FORMAT.key(), "json");
        schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), "none");
        schemaBuilder.option(CoreOptions.VECTOR_FILE_FORMAT.key(), "json");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        catalog.createTable(identifier(), schemaBuilder.build(), true);

        FileStoreTable table = getTableDefault();

        int numBatches = 5;
        int rowsPerBatch = 3;
        Map<Integer, float[]> expected = new HashMap<>();
        List<CommitMessage> messages = new ArrayList<>();
        int id = 0;
        for (int b = 0; b < numBatches; b++) {
            StreamWriteBuilder builder = table.newStreamWriteBuilder();
            builder.withCommitUser(commitUser);
            try (StreamTableWrite write = builder.newWrite()) {
                for (int r = 0; r < rowsPerBatch; r++) {
                    float[] vector = randomFixedLengthVector(vectorLength);
                    expected.put(id, vector);
                    write.write(
                            GenericRow.of(
                                    id,
                                    BinaryString.fromString("row-" + id),
                                    BinaryVector.fromPrimitiveArray(vector)));
                    id++;
                }
                messages.addAll(write.prepareCommit(false, Long.MAX_VALUE));
            }
        }
        commitDefault(messages);

        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false, true);
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        assertThat(tasks.stream().anyMatch(DataEvolutionCompactTask::isVectorTask)).isTrue();

        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : tasks) {
            compactMessages.add(task.doCompact(table, commitUser));
        }
        commitDefault(compactMessages);

        Map<Integer, float[]> actual = new HashMap<>();
        readDefault(row -> actual.put(row.getInt(0), row.getVector(2).toFloatArray()));

        assertThat(actual.size()).isEqualTo(expected.size());
        for (Map.Entry<Integer, float[]> entry : expected.entrySet()) {
            Assertions.assertArrayEquals(entry.getValue(), actual.get(entry.getKey()), 0);
        }
    }

    private float[] randomFixedLengthVector(int length) {
        float[] vector = new float[length];
        for (int i = 0; i < length; i++) {
            vector[i] = RANDOM.nextFloat();
        }
        return vector;
    }

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.VECTOR(testVector.length, DataTypes.FLOAT()));
        // schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.FILE_FORMAT.key(), "json");
        schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), "none");
        return schemaBuilder.build();
    }

    @Override
    protected InternalRow dataDefault(int time, int size) {
        return GenericRow.of(
                RANDOM.nextInt(),
                BinaryString.fromBytes(randomBytes()),
                BinaryVector.fromPrimitiveArray(testVector));
    }

    @Override
    protected byte[] randomBytes() {
        byte[] binary = new byte[RANDOM.nextInt(1024) + 1];
        RANDOM.nextBytes(binary);
        return binary;
    }

    private float[] randomVector() {
        byte[] randomBytes = randomBytes();
        float[] vector = new float[randomBytes.length];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomBytes[i];
        }
        return vector;
    }
}
