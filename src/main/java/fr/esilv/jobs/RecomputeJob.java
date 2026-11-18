package fr.esilv.jobs;

import fr.esilv.util.BalDf;
import fr.esilv.util.SparkUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Rebuild a full dump as-of a date by replaying diffs backward from bal_latest.
 */
public final class RecomputeJob {

    private RecomputeJob() {}

    public static void run(String targetDate, String outputDir) {
        SparkSession spark = SparkUtils.spark("bal-recompute-" + targetDate);

        Dataset<Row> current;
        try {
            current = spark.read().parquet(SparkUtils.toSparkPath(SparkUtils.latestPath()));
            current = BalDf.normalize(current); // make sure id/_hash are present
        } catch (Exception e) {
            throw new RuntimeException("bal_latest not found; cannot recompute", e);
        }

        // Schema for JSON payloads (no _hash).
        StructType businessSchema = businessSchema(current.schema());

        // Days strictly after targetDate.
        spark.sql("CREATE DATABASE IF NOT EXISTS bal");
        List<String> days = SparkUtils.listDiffDaysAfter(targetDate);

        System.out.println("[RECOMPUTE] Starting from bal_latest; reverse-applying " + days.size() + " day(s)");

        Dataset<Row> df = current;
        for (String day : days) {
            df = reverseApplyDay(spark, df, day, businessSchema);
        }

        // Write parquet, drop _hash.
        String out = SparkUtils.toSparkPath(outputDir);
        SparkUtils.ensureDir(outputDir);
        Dataset<Row> outDf = df.drop(BalDf.COL_HASH);
        if (SparkUtils.isRecomputeSingleFileEnabled()) {
            outDf = outDf.coalesce(1);
        }
        outDf.write()
                .mode(SaveMode.Overwrite)
                .parquet(out);

        System.out.println("[RECOMPUTE] Written parquet dump as-of " + targetDate + " to: " + outputDir);
        spark.stop();
    }

    private static Dataset<Row> reverseApplyDay(SparkSession spark, Dataset<Row> current, String day, StructType businessSchema) {
        Dataset<Row> diff = spark.table("bal.bal_diff")
                .where(col("day").equalTo(day))
                .where(col("op").isin("I", "U", "D"))
                .cache();
        if (isEmpty(diff)) {
            diff.unpersist();
            return current;
        }

        Dataset<Row> insertedIds = diff.where(col("op").equalTo("I")).select(col("id")).distinct();
        Dataset<Row> updatedOld = parseJsonRows(diff.where(col("op").equalTo("U")), "old_json", businessSchema);
        Dataset<Row> deletedOld = parseJsonRows(diff.where(col("op").equalTo("D")), "old_json", businessSchema);

        // 1) undo INSERT: remove ids
        Dataset<Row> df = current.join(insertedIds, "id", "left_anti");

        // 2) undo UPDATE: put back old rows
        if (!isEmpty(updatedOld)) {
            Dataset<Row> ids = updatedOld.select(col("id")).distinct();
            df = df.join(ids, "id", "left_anti").unionByName(updatedOld);
        }

        // 3) undo DELETE: reinsert rows
        if (!isEmpty(deletedOld)) {
            Dataset<Row> ids = deletedOld.select(col("id")).distinct();
            df = df.join(ids, "id", "left_anti").unionByName(deletedOld);
        }

        df = df.dropDuplicates("id");

        System.out.println("[RECOMPUTE] reversed day=" + day + " (applied inverse ops)");

        diff.unpersist();
        return df;
    }

    private static Dataset<Row> parseJsonRows(Dataset<Row> diffSubset, String jsonCol, StructType businessSchema) {
        Dataset<Row> parsed = diffSubset
                .where(col(jsonCol).isNotNull())
                .select(from_json(col(jsonCol), businessSchema).alias("r"))
                .select("r.*");
        return BalDf.normalize(parsed);
    }

    private static StructType businessSchema(StructType fullSchema) {
        List<StructField> fields = new ArrayList<>();
        for (StructField f : fullSchema.fields()) {
            if (!f.name().equals(BalDf.COL_HASH)) {
                fields.add(f);
            }
        }
        return new StructType(fields.toArray(new StructField[0]));
    }

    private static boolean isEmpty(Dataset<Row> df) {
        // Avoid count(); take(1) is cheaper.
        return df.takeAsList(1).isEmpty();
    }

}
