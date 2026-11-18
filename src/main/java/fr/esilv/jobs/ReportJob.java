package fr.esilv.jobs;

import fr.esilv.util.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Daily report on the latest snapshot.
 */
public final class ReportJob {

    private ReportJob() {}

    public static void run() {
        SparkSession spark = SparkUtils.spark("bal-report");

        String latestPath = SparkUtils.latestPath();
        Dataset<Row> latest;
        try {
            latest = spark.read().parquet(SparkUtils.toSparkPath(latestPath));
        } catch (Exception e) {
            System.out.println("[REPORT] No bal_latest found at: " + latestPath);
            spark.stop();
            return;
        }

        long total = latest.count();
        System.out.println("[REPORT] Total addresses (latest): " + total);

        String inseeCol = null;
        if (hasColumn(latest, "code_insee")) {
            inseeCol = "code_insee";
        } else if (hasColumn(latest, "commune_insee")) {
            inseeCol = "commune_insee";
        }

        if (inseeCol != null) {
            // Dept code: 97/98 => 3 digits, else 2.
            Dataset<Row> byDept = latest
                    .withColumn("dept",
                            when(col(inseeCol).startsWith("97").or(col(inseeCol).startsWith("98")), substring(col(inseeCol), 1, 3))
                                    .otherwise(substring(col(inseeCol), 1, 2))
                    )
                    .groupBy(col("dept"))
                    .count()
                    .orderBy(col("dept"));

            System.out.println("[REPORT] Addresses per department:");
            byDept.show(200, false);
        } else {
            System.out.println("[REPORT] INSEE column not found, skipping department stats.");
        }

        // Also show last diff day stats.
        try {
            spark.sql("CREATE DATABASE IF NOT EXISTS bal");
            Dataset<Row> maxDay = spark.sql("SELECT max(day) AS day FROM bal.bal_diff");
            Row r = maxDay.collectAsList().get(0);
            String day = r.isNullAt(0) ? null : r.getString(0);
            if (day != null) {
                System.out.println("[REPORT] Last diff day: " + day);
                Dataset<Row> ops = spark.table("bal.bal_diff")
                        .where(col("day").equalTo(day).and(col("op").isin("I", "U", "D")))
                        .groupBy(col("op"))
                        .count()
                        .orderBy(col("op"));
                ops.show(false);
            }
        } catch (Exception e) {
            System.out.println("[REPORT] Diff table not available yet.");
        }

        spark.stop();
    }

    private static boolean hasColumn(Dataset<Row> df, String name) {
        for (String c : df.columns()) {
            if (c != null && c.equalsIgnoreCase(name)) return true;
        }
        return false;
    }
}
