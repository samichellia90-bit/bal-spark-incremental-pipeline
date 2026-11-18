package fr.esilv;

import fr.esilv.jobs.DailyIntegrationJob;
import fr.esilv.jobs.DiffJob;
import fr.esilv.jobs.RecomputeJob;
import fr.esilv.jobs.ReportJob;

import java.util.HashMap;
import java.util.Map;

/**
 * CLI entry point for the four jobs.
 *
 * Commands:
 *   daily     --date YYYY-MM-DD --input <csv_file_or_dir>
 *   report
 *   recompute --date YYYY-MM-DD --output <output_dir>
 *   diff      --left <parquet_dir_1> --right <parquet_dir_2>
 */
public class SparkMain {

    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            usageAndExit("Missing command");
        }

        String command = args[0].trim().toLowerCase();
        Map<String, String> kv = parseArgs(args, 1);

        switch (command) {
            case "daily": {
                String date = required(kv, "date");
                String input = required(kv, "input");
                DailyIntegrationJob.run(date, input);
                break;
            }
            case "report": {
                ReportJob.run();
                break;
            }
            case "recompute": {
                String date = required(kv, "date");
                String output = required(kv, "output");
                RecomputeJob.run(date, output);
                break;
            }
            case "diff": {
                String left = required(kv, "left");
                String right = required(kv, "right");
                DiffJob.run(left, right);
                break;
            }
            default:
                usageAndExit("Unknown command: " + command);
        }
    }

    private static Map<String, String> parseArgs(String[] args, int start) {
        Map<String, String> kv = new HashMap<>();
        for (int i = start; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--")) {
                continue;
            }
            String key = a.substring(2);
            if (i + 1 >= args.length) {
                usageAndExit("Missing value after " + a);
            }
            String value = args[i + 1];
            if (value.startsWith("--")) {
                usageAndExit("Missing value after " + a);
            }
            kv.put(key, value);
            i++;
        }
        return kv;
    }

    private static String required(Map<String, String> kv, String key) {
        String v = kv.get(key);
        if (v == null || v.isBlank()) {
            usageAndExit("Missing --" + key);
        }
        return v;
    }

    private static void usageAndExit(String error) {
        System.err.println("ERROR: " + error);
        System.err.println();
        System.err.println("Usage:");
        System.err.println("  daily     --date YYYY-MM-DD --input <csv_file_or_dir>");
        System.err.println("  report");
        System.err.println("  recompute --date YYYY-MM-DD --output <output_dir>");
        System.err.println("  diff      --left <parquet_dir_1> --right <parquet_dir_2>");
        System.exit(2);
    }
}
