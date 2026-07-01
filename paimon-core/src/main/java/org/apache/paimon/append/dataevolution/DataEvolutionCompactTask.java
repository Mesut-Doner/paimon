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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.FileWriter;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RowDataFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SetUtils;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;
import static org.apache.paimon.types.BlobType.fieldNamesInBlobFile;
import static org.apache.paimon.types.VectorType.fieldNamesInVectorFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Data evolution table compaction task. */
public class DataEvolutionCompactTask extends AppendCompactTask {

    private static final Logger LOG = LoggerFactory.getLogger(DataEvolutionCompactTask.class);

    private static final Map<String, String> DYNAMIC_WRITE_OPTIONS = dynamicWriteOptions();
    private static final Map<String, String> BLOB_COMPACT_READ_OPTIONS =
            Collections.singletonMap(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true");

    private static Map<String, String> dynamicWriteOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "99999 G");
        options.put(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "99999 G");
        return Collections.unmodifiableMap(options);
    }

    /** The kind of files this task compacts. */
    public enum TaskKind {
        NORMAL,
        BLOB,
        VECTOR
    }

    private final TaskKind kind;

    public DataEvolutionCompactTask(BinaryRow partition, List<DataFileMeta> files, TaskKind kind) {
        super(partition, files);
        this.kind = kind;
    }

    public TaskKind kind() {
        return kind;
    }

    public boolean isBlobTask() {
        return kind == TaskKind.BLOB;
    }

    public boolean isVectorTask() {
        return kind == TaskKind.VECTOR;
    }

    public CommitMessage doCompact(FileStoreTable table, String commitUser) throws Exception {
        CoreOptions options = table.coreOptions();
        checkArgument(
                !options.deletionVectorsEnabled(),
                "Data evolution compaction does not support deletion vectors.");

        switch (kind) {
            case BLOB:
                return doCompactBlobFiles(table, commitUser);
            case VECTOR:
                return doCompactVectorStoreFiles(table, commitUser);
            default:
                return doCompactNormalFiles(table, commitUser);
        }
    }

    private CommitMessage doCompactNormalFiles(FileStoreTable table, String commitUser)
            throws Exception {
        CoreOptions options = table.coreOptions();
        Set<String> fieldsInDedicatedFile =
                SetUtils.union(
                        fieldNamesInBlobFile(table.rowType(), options.blobInlineField()),
                        fieldNamesInVectorFile(table.rowType(), options.withVectorFormat()));

        table = table.copy(DYNAMIC_WRITE_OPTIONS);
        long firstRowId = compactBefore.get(0).nonNullFirstRowId();

        RowType readWriteType =
                new RowType(
                        table.rowType().getFields().stream()
                                .filter(f -> !fieldsInDedicatedFile.contains(f.name()))
                                .collect(Collectors.toList()));
        FileStorePathFactory pathFactory = table.store().pathFactory();
        AppendOnlyFileStore store = (AppendOnlyFileStore) table.store();

        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(compactBefore)
                        .withBucketPath(pathFactory.bucketPath(partition, 0).toString())
                        .rawConvertible(false)
                        .build();
        RecordReader<InternalRow> reader =
                store.newDataEvolutionRead().withReadType(readWriteType).createReader(dataSplit);
        AppendFileStoreWrite storeWrite = (AppendFileStoreWrite) store.newWrite(commitUser);
        storeWrite.withWriteType(readWriteType);
        RecordWriter<InternalRow> writer = storeWrite.createWriter(partition, 0);

        reader.forEachRemaining(
                row -> {
                    try {
                        writer.write(row);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        List<DataFileMeta> writeResult = writer.prepareCommit(false).newFilesIncrement().newFiles();
        checkArgument(
                writeResult.size() == 1, "Data evolution compaction should produce one file.");

        try {
            writer.close();
            storeWrite.close();
        } catch (Exception e) {
            LOG.warn("Failed to close reader and writer.", e);
        }

        DataFileMeta dataFileMeta = writeResult.get(0).assignFirstRowId(firstRowId);
        long minSequenceId =
                compactBefore.stream()
                        .mapToLong(DataFileMeta::minSequenceNumber)
                        .min()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Cannot get min sequence id from compact before files."));
        long maxSequenceId =
                compactBefore.stream()
                        .mapToLong(DataFileMeta::maxSequenceNumber)
                        .max()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Cannot get max sequence id from compact before files."));
        dataFileMeta = dataFileMeta.assignSequenceNumber(minSequenceId, maxSequenceId);
        compactAfter.add(dataFileMeta);

        CompactIncrement compactIncrement =
                new CompactIncrement(
                        compactBefore,
                        compactAfter,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());
        return new CommitMessageImpl(
                partition, 0, null, DataIncrement.emptyIncrement(), compactIncrement);
    }

    private CommitMessage doCompactBlobFiles(FileStoreTable table, String commitUser)
            throws Exception {
        CoreOptions options = table.coreOptions();
        List<DataFileMeta> sortedCompactBefore = sortedByFirstRowId(compactBefore);
        DataField blobField = blobField(table, options, sortedCompactBefore);
        Range compactBeforeRange = checkContiguousRowRange(sortedCompactBefore);
        checkArgument(
                sortedCompactBefore.size() > 1,
                "Blob compaction task %s should contain at least two files to compact.",
                this);

        RowType blobWriteType = new RowType(Collections.singletonList(blobField));

        FileStoreTable readTable = table.copy(BLOB_COMPACT_READ_OPTIONS);
        AppendOnlyFileStore store = (AppendOnlyFileStore) readTable.store();
        DataFilePathFactory pathFactory =
                store.pathFactory().createDataFilePathFactory(partition, 0);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(sortedCompactBefore)
                        .withBucketPath(pathFactory.parent().toString())
                        .rawConvertible(false)
                        .build();
        RecordReader<InternalRow> reader =
                store.newDataEvolutionRead().withReadType(blobWriteType).createReader(dataSplit);
        FileWriter<InternalRow, DataFileMeta> writer =
                createBlobFileWriter(table, options, blobWriteType, blobField.name(), pathFactory);

        try {
            reader.forEachRemaining(
                    row -> {
                        try {
                            writer.write(row);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            writer.close();
        } catch (Exception e) {
            writer.abort();
            throw e;
        }

        long minSequenceId = minSequenceId(sortedCompactBefore);
        long maxSequenceId = maxSequenceId(sortedCompactBefore);
        DataFileMeta compactedFile =
                writer.result()
                        .assignFirstRowId(compactBeforeRange.from)
                        .assignSequenceNumber(minSequenceId, maxSequenceId);
        compactAfter.add(compactedFile);
        checkArgument(compactAfter.size() == 1, "Blob file compaction should produce one file.");
        checkSameRowRange(sortedCompactBefore, compactAfter);

        CompactIncrement compactIncrement =
                new CompactIncrement(
                        sortedCompactBefore,
                        compactAfter,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());
        return new CommitMessageImpl(
                partition, 0, null, DataIncrement.emptyIncrement(), compactIncrement);
    }

    private FileWriter<InternalRow, DataFileMeta> createBlobFileWriter(
            FileStoreTable table,
            CoreOptions options,
            RowType blobWriteType,
            String blobFieldName,
            DataFilePathFactory pathFactory) {
        BlobFileFormat blobFileFormat = new BlobFileFormat();
        return new RowDataFileWriter(
                table.fileIO(),
                RollingFileWriter.createFileWriterContext(
                        blobFileFormat,
                        blobWriteType,
                        new SimpleColStatsCollector.Factory[] {NoneSimpleColStatsCollector::new},
                        "none"),
                pathFactory.newBlobPath(),
                blobWriteType,
                table.schema().id(),
                () -> new LongCounter(0),
                new FileIndexOptions(),
                FileSource.COMPACT,
                false,
                options.statsDenseStore(),
                pathFactory.isExternalPath(),
                Collections.singletonList(blobFieldName));
    }

    private CommitMessage doCompactVectorStoreFiles(FileStoreTable table, String commitUser)
            throws Exception {
        CoreOptions options = table.coreOptions();
        List<DataFileMeta> sortedCompactBefore = sortedByFirstRowId(compactBefore);
        RowType vectorWriteType = vectorWriteType(table, options, sortedCompactBefore);
        Range compactBeforeRange = checkContiguousRowRange(sortedCompactBefore);
        checkArgument(
                sortedCompactBefore.size() > 1,
                "Vector-store compaction task %s should contain at least two files to compact.",
                this);

        AppendOnlyFileStore store = (AppendOnlyFileStore) table.store();
        DataFilePathFactory pathFactory =
                store.pathFactory().createDataFilePathFactory(partition, 0);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(sortedCompactBefore)
                        .withBucketPath(pathFactory.parent().toString())
                        .rawConvertible(false)
                        .build();
        RecordReader<InternalRow> reader =
                store.newDataEvolutionRead().withReadType(vectorWriteType).createReader(dataSplit);
        FileWriter<InternalRow, DataFileMeta> writer =
                createVectorStoreFileWriter(table, options, vectorWriteType, pathFactory);

        try {
            reader.forEachRemaining(
                    row -> {
                        try {
                            writer.write(row);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            writer.close();
        } catch (Exception e) {
            writer.abort();
            throw e;
        }

        long minSequenceId = minSequenceId(sortedCompactBefore);
        long maxSequenceId = maxSequenceId(sortedCompactBefore);
        DataFileMeta compactedFile =
                writer.result()
                        .assignFirstRowId(compactBeforeRange.from)
                        .assignSequenceNumber(minSequenceId, maxSequenceId);
        compactAfter.add(compactedFile);
        checkArgument(
                compactAfter.size() == 1, "Vector-store file compaction should produce one file.");
        checkSameRowRange(sortedCompactBefore, compactAfter);

        CompactIncrement compactIncrement =
                new CompactIncrement(
                        sortedCompactBefore,
                        compactAfter,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList());
        return new CommitMessageImpl(
                partition, 0, null, DataIncrement.emptyIncrement(), compactIncrement);
    }

    private FileWriter<InternalRow, DataFileMeta> createVectorStoreFileWriter(
            FileStoreTable table,
            CoreOptions options,
            RowType vectorWriteType,
            DataFilePathFactory pathFactory) {
        FileFormat vectorFileFormat = FileFormat.vectorFileFormat(options);
        checkArgument(
                vectorFileFormat != null,
                "Cannot compact vector-store files without a configured vector file format.");
        List<String> vectorFieldNames = vectorWriteType.getFieldNames();
        SimpleColStatsCollector.Factory[] statsCollectors =
                new StatsCollectorFactories(options).statsCollectors(vectorFieldNames);
        return new RowDataFileWriter(
                table.fileIO(),
                RollingFileWriter.createFileWriterContext(
                        vectorFileFormat,
                        vectorWriteType,
                        statsCollectors,
                        options.fileCompression()),
                pathFactory.newVectorPath(vectorFileFormat.getFormatIdentifier()),
                vectorWriteType,
                table.schema().id(),
                () -> new LongCounter(0),
                new FileIndexOptions(),
                FileSource.COMPACT,
                false,
                options.statsDenseStore(),
                pathFactory.isExternalPath(),
                vectorFieldNames);
    }

    private RowType vectorWriteType(
            FileStoreTable table, CoreOptions options, List<DataFileMeta> files) {
        Set<Integer> vectorFieldIds = null;
        Map<Long, RowType> schemaCache = new HashMap<>();
        for (DataFileMeta file : files) {
            checkArgument(
                    file.writeCols() != null && !file.writeCols().isEmpty(),
                    "Vector-store file %s should contain at least one write column.",
                    file);
            RowType fileRowType =
                    schemaCache.computeIfAbsent(
                            file.schemaId(),
                            schemaId -> table.schemaManager().schema(schemaId).logicalRowType());
            Set<Integer> currentFieldIds =
                    file.writeCols().stream()
                            .map(name -> fileRowType.getField(name).id())
                            .collect(Collectors.toSet());
            if (vectorFieldIds == null) {
                vectorFieldIds = currentFieldIds;
            } else {
                checkArgument(
                        vectorFieldIds.equals(currentFieldIds),
                        "Vector-store compact before files %s should contain the same fields.",
                        files);
            }
        }

        checkArgument(vectorFieldIds != null, "Vector-store compaction task should not be empty.");
        Set<Integer> fieldIds = vectorFieldIds;
        List<DataField> fields =
                table.rowType().getFields().stream()
                        .filter(f -> fieldIds.contains(f.id()))
                        .collect(Collectors.toList());
        checkArgument(
                fields.size() == fieldIds.size(),
                "Cannot find all vector-store field ids %s in latest schema for compaction task %s.",
                fieldIds,
                this);
        Set<String> vectorFieldNames =
                fieldNamesInVectorFile(table.rowType(), options.withVectorFormat());
        for (DataField field : fields) {
            checkArgument(
                    vectorFieldNames.contains(field.name()),
                    "Field %s in latest schema is not a vector-store file field.",
                    field.name());
        }
        return new RowType(fields);
    }

    private List<DataFileMeta> sortedByFirstRowId(List<DataFileMeta> files) {
        List<DataFileMeta> sorted = new ArrayList<>(files);
        sorted.sort(comparingLong(DataFileMeta::nonNullFirstRowId));
        return sorted;
    }

    private DataField blobField(
            FileStoreTable table, CoreOptions options, List<DataFileMeta> files) {
        Integer blobFieldId = null;
        Map<Long, RowType> schemaCache = new HashMap<>();
        for (DataFileMeta file : files) {
            checkArgument(
                    file.writeCols() != null && file.writeCols().size() == 1,
                    "Blob file %s should contain exactly one write column.",
                    file);
            RowType fileRowType =
                    schemaCache.computeIfAbsent(
                            file.schemaId(),
                            schemaId -> table.schemaManager().schema(schemaId).logicalRowType());
            int currentFieldId = fileRowType.getField(file.writeCols().get(0)).id();
            if (blobFieldId == null) {
                blobFieldId = currentFieldId;
            } else {
                checkArgument(
                        blobFieldId == currentFieldId,
                        "Blob compact before files %s should contain the same field.",
                        files);
            }
        }

        checkArgument(blobFieldId != null, "Blob compaction task should not be empty.");
        checkArgument(
                table.rowType().containsField(blobFieldId),
                "Cannot find blob field id %s in latest schema for compaction task %s.",
                blobFieldId,
                this);
        DataField field = table.rowType().getField(blobFieldId);
        Set<String> blobFieldNames =
                fieldNamesInBlobFile(table.rowType(), options.blobInlineField());
        checkArgument(
                blobFieldNames.contains(field.name()),
                "Field %s in latest schema is not a blob file field.",
                field.name());
        return field;
    }

    private Range checkContiguousRowRange(List<DataFileMeta> files) {
        checkArgument(!files.isEmpty(), "%s should not be empty.", "Blob compact files");
        List<Range> ranges =
                files.stream().map(DataFileMeta::nonNullRowIdRange).collect(Collectors.toList());
        List<Range> merged = Range.sortAndMergeOverlap(ranges, true);
        checkArgument(
                merged.size() == 1,
                "%s should have a contiguous row range, but got %s.",
                "Blob compact files",
                merged);
        return merged.get(0);
    }

    private void checkSameRowRange(
            List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        Range beforeRange = checkContiguousRowRange(compactBefore);
        Range afterRange = checkContiguousRowRange(compactAfter);
        checkArgument(
                beforeRange.equals(afterRange),
                "%s compact after files should have the same row range as compact before files, "
                        + "before range is %s, but after range is %s.",
                "Blob compact files",
                beforeRange,
                afterRange);
    }

    private long minSequenceId(List<DataFileMeta> files) {
        return files.stream()
                .mapToLong(DataFileMeta::minSequenceNumber)
                .min()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Cannot get min sequence id from compact before files."));
    }

    private long maxSequenceId(List<DataFileMeta> files) {
        return files.stream()
                .mapToLong(DataFileMeta::maxSequenceNumber)
                .max()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Cannot get max sequence id from compact before files."));
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, compactBefore, compactAfter, kind);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataEvolutionCompactTask that = (DataEvolutionCompactTask) o;
        return kind == that.kind
                && Objects.equals(partition, that.partition)
                && Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter);
    }

    @Override
    public String toString() {
        return String.format(
                "DataEvolutionCompactTask {"
                        + "partition = %s, "
                        + "compactBefore = %s, "
                        + "compactAfter = %s, "
                        + "kind = %s}",
                partition, compactBefore, compactAfter, kind);
    }
}
