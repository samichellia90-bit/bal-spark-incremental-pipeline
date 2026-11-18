package fr.esilv.jobs;

import fr.esilv.util.BalDf;
import fr.esilv.util.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.apache.spark.sql.functions.*;

/**
 * Daily integration: snapshot -> diff -> bal_diff partition, then refresh bal_latest.
 */
public final class DailyIntegrationJob {

    private DailyIntegrationJob() {}

    public static void run(String day, String inputPath) {
        SparkSession spark = SparkUtils.spark("bal-daily-" + day);

        // Create DB/table if needed.
        spark.sql("CREATE DATABASE IF NOT EXISTS bal");
        spark.sql(
                "CREATE TABLE IF NOT EXISTS bal.bal_diff (" +
                        "op STRING, " +
                        "id STRING, " +
                        "old_json STRING, " +
                        "new_json STRING, " +
                        "old_hash STRING, " +
                        "new_hash STRING" +
                        ") USING PARQUET PARTITIONED BY (day STRING)"
        );

        String latestPath = SparkUtils.latestPath();

        if (shouldSkipSameInput(inputPath, latestPath)) {
            System.out.println("[DAILY] input unchanged; writing empty diff partition for day=" + day);
            Dataset<Row> emptyDiff = emptyDiffDataset(spark, day).coalesce(1);
            dropPartitionIfExists(spark, day);
            emptyDiff.write().mode("append").insertInto("bal.bal_diff");
            writeLatestDayMarker(day);
            SparkUtils.fingerprintLocalInput(inputPath).ifPresent(SparkUtils::writeLastInputFingerprint);
            spark.stop();
            return;
        }

        // Snapshots are big; keep them on disk.
        Dataset<Row> newSnap = BalDf.readSnapshot(spark, inputPath).persist(StorageLevel.DISK_ONLY());

        // Load previous snapshot if it exists.
        Dataset<Row> prevSnap;
        try {
            if (SparkUtils.existsLocal(latestPath)) {
                prevSnap = spark.read().parquet(SparkUtils.toSparkPath(latestPath));
                prevSnap = BalDf.normalize(prevSnap).persist(StorageLevel.DISK_ONLY());
            } else {
                prevSnap = spark.createDataFrame(java.util.Collections.emptyList(), newSnap.schema());
            }
        } catch (Exception e) {
            prevSnap = spark.createDataFrame(java.util.Collections.emptyList(), newSnap.schema());
        }

        Dataset<Row> inserts = newSnap.join(prevSnap.select(BalDf.COL_ID), BalDf.COL_ID, "left_anti")
                .select(
                        lit("I").alias("op"),
                        col(BalDf.COL_ID),
                        lit(null).cast("string").alias("old_json"),
                        BalDf.toJsonPayload(newSnap).alias("new_json"),
                        lit(null).cast("string").alias("old_hash"),
                        col(BalDf.COL_HASH).alias("new_hash"),
                        lit(day).alias("day")
                );

        Dataset<Row> deletes = prevSnap.join(newSnap.select(BalDf.COL_ID), BalDf.COL_ID, "left_anti")
                .select(
                        lit("D").alias("op"),
                        col(BalDf.COL_ID),
                        BalDf.toJsonPayload(prevSnap).alias("old_json"),
                        lit(null).cast("string").alias("new_json"),
                        col(BalDf.COL_HASH).alias("old_hash"),
                        lit(null).cast("string").alias("new_hash"),
                        lit(day).alias("day")
                );

        Dataset<Row> joined = newSnap.alias("n").join(prevSnap.alias("p"), col("n.id").equalTo(col("p.id")), "inner")
                .filter(col("n._hash").notEqual(col("p._hash")));

        // Build JSON without _hash.
        java.util.List<org.apache.spark.sql.Column> oldCols = new java.util.ArrayList<>();
        java.util.List<org.apache.spark.sql.Column> newCols = new java.util.ArrayList<>();
        for (String c : BalDf.businessColumns(newSnap)) {
            // businessColumns() already skips _hash.
            oldCols.add(col("p." + c).alias(c));
            newCols.add(col("n." + c).alias(c));
        }

        Dataset<Row> updates = joined.select(
                lit("U").alias("op"),
                col("n.id").alias("id"),
                to_json(struct(oldCols.toArray(new org.apache.spark.sql.Column[0]))).alias("old_json"),
                to_json(struct(newCols.toArray(new org.apache.spark.sql.Column[0]))).alias("new_json"),
                col("p._hash").alias("old_hash"),
                col("n._hash").alias("new_hash"),
                lit(day).alias("day")
        );

        Dataset<Row> diff = inserts.unionByName(updates).unionByName(deletes);
        diff = ensureNonEmptyDiff(spark, diff, day);

        // Keep one parquet file per day (test expects it).
        diff = diff.coalesce(1);

        // If day already exists, drop the partition first.
        dropPartitionIfExists(spark, day);
        diff.select("op", "id", "old_json", "new_json", "old_hash", "new_hash", "day")
                .write()
                .mode("append")
                .insertInto("bal.bal_diff");

        // Count before bal_latest is overwritten: the overwrite invalidates the cached
        // previous snapshot, which would then be re-read from the new files.
        long totalNew = newSnap.count();
        long totalPrev = prevSnap.count();
        long inserted = inserts.count();
        long updated = updates.count();
        long deleted = deletes.count();

        // Refresh bal_latest (single file).
        String latestSparkPath = SparkUtils.toSparkPath(latestPath);
        newSnap.coalesce(1).write().mode("overwrite").parquet(latestSparkPath);

        // Write a latest-day marker for quick checks.
        writeLatestDayMarker(day);

        System.out.println("[DAILY] day=" + day + " prev=" + totalPrev + " new=" + totalNew +
                " inserted=" + inserted + " updated=" + updated + " deleted=" + deleted);

        SparkUtils.fingerprintLocalInput(inputPath).ifPresent(SparkUtils::writeLastInputFingerprint);
        try { newSnap.unpersist(false); } catch (Exception ignored) {}
        try { prevSnap.unpersist(false); } catch (Exception ignored) {}
        spark.stop();
    }

    private static void dropPartitionIfExists(SparkSession spark, String day) {
        try {
            spark.sql("ALTER TABLE bal.bal_diff DROP IF EXISTS PARTITION (day='" + day + "')");
        } catch (Exception ignored) {
        }
    }

    private static boolean shouldSkipSameInput(String inputPath, String latestPath) {
        if (!SparkUtils.isSkipSameInputEnabled()) {
            return false;
        }
        if (!SparkUtils.existsLocal(latestPath)) {
            return false;
        }
        return SparkUtils.fingerprintLocalInput(inputPath)
                .flatMap(fp -> SparkUtils.readLastInputFingerprint().map(last -> last.equals(fp)))
                .orElse(false);
    }

    private static Dataset<Row> emptyDiffDataset(SparkSession spark, String day) {
        org.apache.spark.sql.types.StructType schema = new org.apache.spark.sql.types.StructType()
                .add("op", "string")
                .add("id", "string")
                .add("old_json", "string")
                .add("new_json", "string")
                .add("old_hash", "string")
                .add("new_hash", "string")
                .add("day", "string");
        org.apache.spark.sql.Row row = org.apache.spark.sql.RowFactory.create("N", null, null, null, null, null, day);
        return spark.createDataFrame(Collections.singletonList(row), schema);
    }

    private static Dataset<Row> ensureNonEmptyDiff(SparkSession spark, Dataset<Row> diff, String day) {
        if (!diff.takeAsList(1).isEmpty()) {
            return diff;
        }
        return emptyDiffDataset(spark, day);
    }

    private static void writeLatestDayMarker(String day) {
        try {
            Path marker = Paths.get(SparkUtils.storageRoot(), "bal_latest_day.txt");
            Files.writeString(marker, day);
        } catch (Exception ignored) {
        }
    }
}
