# Java Producer Recipe

Copy-pasteable flow for a Java producer on ECS or Lambda using `iceberg-core` + `iceberg-aws`.

**Read [`docs/using-iceshelf.md`](../using-iceshelf.md) first.** This recipe assumes you understand the 8-phase lifecycle and skill reference.

---

## Hypothetical producer: `datamesh-producer-kestrel`

| Setting | Value |
|---------|-------|
| Producer name | `datamesh-producer-kestrel` |
| Stack | ECS Fargate (container), Java 21, Iceberg 1.10.1 |
| Source | Kinesis Data Stream `kestrel-transactions-stream` |
| Target | `s3://kestrel-confirmed-us-east-1/warehouse/` (database `kestrel_confirmed`, table `transactions`) |
| Primary region | `us-east-1` |
| DR region | `us-west-2` |
| DR bucket | `s3://kestrel-confirmed-us-west-2/warehouse/` |
| Orchestrator | ECS service, always-on |
| Format version | 2 |

Same recipe applies to Lambda Java — differences in the "Lambda-specific notes" section.

---

## Phase 0: Profile setup

Run `/iceberg-profile-setup`. Profile is stored in `profiles/datamesh-producer-kestrel.yaml`.

## Phase 1: Assess (`/iceberg-onboard`)

Java-specific things the architect will flag:

- **JVM memory.** Data writers buffer whole Parquet row groups in memory. Size your container to 2x expected row-group size at minimum.
- **Schema management.** Java API is low-level — you write records one at a time, so schema evolution requires careful coordination with table schema changes.
- **Commit retry.** `CommitFailedException` on concurrent writers must be handled explicitly; there's no Spark-style automatic retry.

## Phase 2: Create (`/iceberg-ddl`)

Java uses the catalog API directly, not SQL:

```java
import org.apache.iceberg.*;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import com.google.common.collect.ImmutableMap;

GlueCatalog catalog = new GlueCatalog();
catalog.initialize("glue_catalog", ImmutableMap.of(
    "warehouse",   "s3://kestrel-confirmed-us-east-1/warehouse/",
    "io-impl",     "org.apache.iceberg.aws.s3.S3FileIO",
    "glue.region", "us-east-1",
    "s3.region",   "us-east-1"
));

catalog.createNamespace(Namespace.of("kestrel_confirmed"));

Schema schema = new Schema(
    Types.NestedField.required(1, "transaction_id",   Types.StringType.get()),
    Types.NestedField.required(2, "transaction_time", Types.TimestampType.withZone()),
    Types.NestedField.optional(3, "amount",           Types.DecimalType.of(18, 2)),
    Types.NestedField.required(4, "account_id",       Types.StringType.get()),
    Types.NestedField.required(5, "region",           Types.StringType.get())
);

PartitionSpec spec = PartitionSpec.builderFor(schema)
    .day("transaction_time", "transaction_date")
    .identity("region")
    .build();

TableIdentifier id = TableIdentifier.of("kestrel_confirmed", "transactions");
Table table = catalog.buildTable(id, schema)
    .withPartitionSpec(spec)
    .withProperty("format-version", "2")
    .withProperty("write.parquet.compression-codec", "zstd")
    .withProperty("write.target-file-size-bytes", "134217728")
    .withProperty("write.metadata.delete-after-commit.enabled", "true")
    .withProperty("write.metadata.previous-versions-max", "10")
    .create();
```

Both `glue.region` and `s3.region` are required — same reasoning as the PyIceberg recipe.

## Phase 3: Backfill

Java doesn't have equivalents to `system.migrate` or `system.add_files` procedures — those are Spark-specific. For backfill with Java, either:

**A. Run a one-off Spark job for the backfill.** Most common — use the PySpark recipe's Phase 3 as a one-time tool, then switch to the Java producer for ongoing writes.

**B. Read + write via Java API.** For smaller datasets:

```java
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.exceptions.CommitFailedException;

Table table = catalog.loadTable(TableIdentifier.of("kestrel_confirmed", "transactions"));
Schema schema = table.schema();

String dataPath = table.locationProvider()
    .newDataLocation("data-" + UUID.randomUUID() + ".parquet");
OutputFile outputFile = table.io().newOutputFile(dataPath);

DataWriter<GenericRecord> writer = Parquet.writeData(outputFile)
    .schema(schema)
    .createWriterFunc(GenericParquetWriter::buildWriter)
    .overwrite()
    .build();

DataFile dataFile;
try {
    for (Record r : sourceRecords) {
        writer.write(r);
    }
    writer.close();          // explicit close — required before toDataFile()
    dataFile = writer.toDataFile();
} catch (Exception e) {
    // Best-effort cleanup of the orphaned file before re-throwing
    try {
        writer.close();
    } catch (Exception closeEx) {
        e.addSuppressed(closeEx);
    }
    try {
        table.io().deleteFile(dataPath);
    } catch (Exception delEx) {
        e.addSuppressed(delEx);
    }
    throw e;
}

table.newAppend()
    .appendFile(dataFile)
    .commit();
```

**Do not** put `writer.close()` inside a single try-with-resources that also calls `writer.toDataFile()` — you need the data file reference only after a successful close. The pattern above gives you clean orphan cleanup on failure.

## Phase 4: Dual-write (Kinesis consumer loop)

```java
while (running) {
    List<Record> batch = kinesisConsumer.poll(Duration.ofSeconds(5));
    if (batch.isEmpty()) continue;

    // Existing Parquet write — KEEP
    parquetWriter.writeBatch(batch, legacyPath(batch));

    // NEW Iceberg write — ADD
    writeBatchToIceberg(table, batch);

    kinesisConsumer.commit();   // advance checkpoint only after BOTH writes succeed
}
```

Critical: advance the Kinesis checkpoint only after both writes commit. If the Parquet write succeeds but Iceberg fails (or vice versa), the batch must be retried from the same Kinesis position.

### Commit retry pattern

```java
int attempt = 0;
while (true) {
    try {
        table.newAppend().appendFile(dataFile).commit();
        break;
    } catch (CommitFailedException e) {
        if (++attempt >= 5) throw e;
        Thread.sleep((1L << attempt) * 100);  // exponential backoff
        table.refresh();  // pick up the latest snapshot before retrying
    }
}
```

`CommitFailedException` means another writer won the race on the same snapshot. Refresh the table state and retry; the append is idempotent at the data-file level (you already wrote the Parquet file; you're just changing which snapshot it's linked from).

## Phase 5: Reconciliation + cutover

Java reconciliation is awkward — typically offloaded to a Spark job that reads both sides. Schedule a daily Glue job that runs the reconciliation query from the PySpark recipe.

Cutover: remove the Parquet write call from your consumer loop. Redeploy. Iceberg is now the sole target.

## Phase 6: Maintain (`/iceberg-maintenance`)

Same situation as PyIceberg: Java API has limited maintenance primitives.

### Snapshot expiration (Java API supports this)

```java
table.expireSnapshots()
    .expireOlderThan(System.currentTimeMillis() - Duration.ofDays(7).toMillis())
    .retainLast(10)
    .commit();
```

### Compaction and orphan cleanup → scheduled Spark job

Same pattern as PyIceberg: schedule a daily Glue job that calls `system.rewrite_data_files` and a monthly one that calls `system.remove_orphan_files`. Don't try to reimplement these in Java.

## Phase 7: Multi-region (`/iceberg-multi-region`)

Java repointing utility (runs on ECS or Lambda in us-west-2). The Avro rewrite preserves file-level metadata via explicit `setMeta()` calls:

```java
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;

public void rewriteManifestAvro(String srcKey, String dstKey) throws IOException {
    byte[] raw = s3.getObject(b -> b.bucket(dstBucket).key(srcKey))
                   .readAllBytes();

    try (var inStream = new SeekableByteArrayInput(raw);
         var dataReader = new DataFileReader<>(inStream,
             new GenericDatumReader<GenericRecord>())) {

        Schema schema = dataReader.getSchema();
        var out = new ByteArrayOutputStream();

        try (var writer = new DataFileWriter<>(
                new GenericDatumWriter<GenericRecord>(schema))) {

            // Preserve file-level metadata (iceberg.schema, content, partition-spec-id, etc.)
            for (String key : dataReader.getMetaKeys()) {
                if (!key.startsWith("avro.")) {
                    byte[] value = dataReader.getMeta(key);
                    if (value != null) writer.setMeta(key, value);
                }
            }

            writer.create(schema, out);

            while (dataReader.hasNext()) {
                GenericRecord rec = dataReader.next();
                rewritePathsInRecord(rec);
                writer.append(rec);
            }
        }

        s3.putObject(b -> b.bucket(dstBucket).key(dstKey),
                     RequestBody.fromBytes(out.toByteArray()));
    }
}
```

Key detail: `dataReader.getMetaKeys()` iteration and `writer.setMeta()` preservation is **mandatory**. Iceberg's own code reads `iceberg.schema` and `content` from the Avro file-level metadata; dropping them breaks table reads.

## Phase 8: Failover flip

Same runbook as the other recipes. For Java-on-ECS, redeployment is done via ECS service update with environment variables:

```
WAREHOUSE_BUCKET=kestrel-confirmed-us-west-2
GLUE_REGION=us-west-2
S3_REGION=us-west-2
AWS_REGION=us-west-2
```

The catalog is re-initialized on container startup with the new region.

---

## Maven dependencies

```xml
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-core</artifactId>
    <version>1.10.1</version>
</dependency>
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-data</artifactId>
    <version>1.10.1</version>
</dependency>
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-parquet</artifactId>
    <version>1.10.1</version>
</dependency>
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-aws</artifactId>
    <version>1.10.1</version>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>bundle</artifactId>
    <version>2.28.16</version>
</dependency>
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>url-connection-client</artifactId>
    <version>2.28.16</version>
</dependency>
```

Use `iceberg-aws-bundle` (single fat jar) instead of `iceberg-aws` + AWS SDK if you want to avoid version coordination — it's a better choice for Lambda packaging.

---

## Lambda-specific notes

### Packaging

- **Use a container image, not a zip.** Iceberg + AWS SDK exceeds the 250MB Lambda zip limit by far. Container images give you 10GB.
- **Custom runtime (AWS Lambda Java 21) or GraalVM native image.** Cold start on standard runtime is 3-5 seconds for Iceberg's first catalog load. GraalVM native image brings this to <500ms but requires reflection config.

### Memory

- Minimum 2048 MB. Data writers buffer Parquet row groups in JVM heap.
- Memory also drives CPU allocation on Lambda — underprovision and writes stall.

### Cold start mitigation

- Provisioned concurrency for hot tables.
- Lazy-load the catalog singleton across invocations (Lambda reuses the execution environment across invocations when warm).

### IAM

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
    "glue:GetTable", "glue:UpdateTable", "glue:GetDatabase",
    "glue:GetTableVersions", "glue:CreateTable"
  ],
  "Resource": "*"
}
```

Scope to specific ARNs in production.

---

## Gotchas specific to Java

- **Explicit `writer.close()` before `writer.toDataFile()`.** `toDataFile()` reads state that is only valid after close. If you haven't closed, you get an incomplete metadata object.
- **Orphan cleanup on exception.** If your write fails mid-stream, the partially-written Parquet file stays in S3 unreferenced. Explicit delete in the catch block prevents orphan accumulation between scheduled orphan-cleanup runs.
- **`CommitFailedException` handling.** Manual retry with `table.refresh()` is required. Unlike Spark, the Java API does not auto-retry.
- **`Region.of()` double-wrap.** `S3Client.builder().region(Region.of("us-east-1"))` is correct; `Region.of(Region.of(...))` is a type error you won't catch at runtime because of overloaded builder methods.
- **Glue version vs Iceberg version.** Java runtime is your choice, but if you're using Glue connectors elsewhere in the pipeline, match Iceberg versions to avoid metadata-format drift (1.6.1, 1.7.1, 1.10.0, 1.10.1 are all v2-compatible but emit slightly different snapshot summaries).
- **Record type vs GenericRecord.** Use `GenericRecord.create(schema)` from `iceberg-data` — not Avro's `GenericRecord`. They have the same name but different semantics.
