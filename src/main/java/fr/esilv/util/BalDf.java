package fr.esilv.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;

import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Helpers for BAL/BAN snapshots: read, normalize, hash, JSON payloads.
 */
public final class BalDf {

    public static final String COL_ID = "id";
    public static final String COL_HASH = "_hash";

    private BalDf() {}

    public static Dataset<Row> readSnapshot(SparkSession spark, String inputPath) {
        String p = SparkUtils.toSparkPath(inputPath);

        // Try CSV first, fallback to Parquet.
        try {
            Dataset<Row> raw = spark.read()
                    .format("csv")
                    .option("header", "true")
                    .option("sep", ";")
                    .option("encoding", "UTF-8")
                    .option("inferSchema", "false")
                    .option("mode", "PERMISSIVE")
                    .load(p);

            return normalize(raw);
        } catch (Exception csvEx) {
            // Parquet fallback.
            Dataset<Row> raw = spark.read().parquet(p);
            return normalize(raw);
        }
    }

    /**
     * Normalize columns, pick the id, cast to string, and add a stable row hash.
     */
    public static Dataset<Row> normalize(Dataset<Row> df) {
        // Clean column names.
        String[] cols = df.columns();
        for (String c : cols) {
            String cleaned = c == null ? c : c.trim();
            if (cleaned != null && cleaned.startsWith("\uFEFF")) {
                cleaned = cleaned.substring(1);
            }
            if (!Objects.equals(c, cleaned) && cleaned != null && !cleaned.isBlank()) {
                df = df.withColumnRenamed(c, cleaned);
            }
        }

        // Pick the id column and rename it.
        String idCol = findIdColumn(df.columns());
        if (!idCol.equals(COL_ID)) {
            df = df.withColumnRenamed(idCol, COL_ID);
        }

        // Cast all columns to string (id included).
        List<Column> casted = new ArrayList<>();
        for (String c : df.columns()) {
            casted.add(col(c).cast("string").alias(c));
        }
        df = df.select(casted.toArray(new Column[0]));

        // Drop empty ids, then dedupe by id.
        df = df.filter(col(COL_ID).isNotNull().and(length(trim(col(COL_ID))).gt(0)))
               .dropDuplicates(COL_ID);

        // Hash non-id columns for a stable row signature.
        List<String> hashCols = new ArrayList<>();
        for (String c : df.columns()) {
            if (!c.equals(COL_ID) && !c.equals(COL_HASH)) {
                hashCols.add(c);
            }
        }
        Collections.sort(hashCols);

        List<Column> parts = new ArrayList<>();
        for (String c : hashCols) {
            parts.add(coalesce(col(c), lit("")));
        }
        Column concatenated = concat_ws("\u0001", parts.toArray(new Column[0]));
        df = df.withColumn(COL_HASH, md5(concatenated));

        return df;
    }

    public static String findIdColumn(String[] columns) {
        Set<String> set = new HashSet<>();
        for (String c : columns) {
            if (c != null) set.add(c);
        }
        if (set.contains("id")) return "id";
        if (set.contains("cle_interop")) return "cle_interop";
        if (set.contains("uid_adresse")) return "uid_adresse";
        if (set.contains("id_ban")) return "id_ban";
        throw new IllegalArgumentException("No identifier column found. Expected one of: id, cle_interop, uid_adresse, id_ban. Found: " + Arrays.toString(columns));
    }

    public static Column toJsonPayload(Dataset<Row> df) {
        // JSON payload without _hash.
        List<Column> cols = new ArrayList<>();
        for (String c : df.columns()) {
            if (!c.equals(COL_HASH)) {
                cols.add(col(c));
            }
        }
        return to_json(struct(cols.toArray(new Column[0])));
    }

    public static List<String> businessColumns(Dataset<Row> df) {
        List<String> cols = new ArrayList<>();
        for (String c : df.columns()) {
            if (!c.equals(COL_HASH)) {
                cols.add(c);
            }
        }
        return cols;
    }
}
