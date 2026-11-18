package fr.esilv.jobs;

import fr.esilv.util.BalDf;
import fr.esilv.util.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.*;

/**
 * Diff two parquet snapshots (left -> right) and print I/U/D counts.
 */
public final class DiffJob {

    private DiffJob() {}

    public static void run(String parquetDir1, String parquetDir2) {
        SparkSession spark = SparkUtils.spark("bal-diff");

        Dataset<Row> left = BalDf.normalize(spark.read().parquet(SparkUtils.toSparkPath(parquetDir1)))
                .persist(StorageLevel.DISK_ONLY());
        Dataset<Row> right = BalDf.normalize(spark.read().parquet(SparkUtils.toSparkPath(parquetDir2)))
                .persist(StorageLevel.DISK_ONLY());

        Dataset<Row> inserted = right.join(left.select(BalDf.COL_ID), BalDf.COL_ID, "left_anti");
        Dataset<Row> deleted = left.join(right.select(BalDf.COL_ID), BalDf.COL_ID, "left_anti");

        Dataset<Row> updated = right.alias("r").join(left.alias("l"), col("r.id").equalTo(col("l.id")), "inner")
                .filter(col("r._hash").notEqual(col("l._hash")))
                .select(col("r.id").alias("id"));

        long ins = inserted.count();
        long del = deleted.count();
        long upd = updated.count();

        System.out.println("[DIFF] left=" + parquetDir1 + " right=" + parquetDir2);
        System.out.println("[DIFF] inserted=" + ins + " updated=" + upd + " deleted=" + del);

        System.out.println("[DIFF] Sample inserted ids:");
        inserted.select(col("id")).limit(20).show(false);

        System.out.println("[DIFF] Sample updated ids:");
        updated.limit(20).show(false);

        System.out.println("[DIFF] Sample deleted ids:");
        deleted.select(col("id")).limit(20).show(false);

        try { left.unpersist(false); } catch (Exception ignored) {}
        try { right.unpersist(false); } catch (Exception ignored) {}
        spark.stop();
    }
}
