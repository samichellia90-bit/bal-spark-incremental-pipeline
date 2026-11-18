package fr.esilv.util;

import org.apache.spark.sql.SparkSession;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Shared Spark helpers. */
public final class SparkUtils {

    private SparkUtils() {}

    public static SparkSession spark(String appName) {
        String storageRoot = storageRoot();
        // Keep Hive tables under <storageRoot>/bal.db (spec requirement).
        String warehouseDir = Paths.get(storageRoot).toAbsolutePath().toString();
        // Use file:/ URI for Windows/Linux.
        String warehouseUri = toFileUri(warehouseDir);
        // Separate metastore to avoid Derby collisions.
        String metastoreDir = Paths.get(storageRoot, "metastore_db_v2").toAbsolutePath().toString();
        String metastoreUrl = "jdbc:derby:;databaseName=" + metastoreDir + ";create=true";

        return SparkSession.builder()
                .appName(appName)
                .config("spark.sql.warehouse.dir", warehouseUri)
                .config("javax.jdo.option.ConnectionURL", metastoreUrl)
                .config("spark.hadoop.javax.jdo.option.ConnectionURL", metastoreUrl)
                .config("datanucleus.schema.autoCreateAll", "true")
                .config("spark.sql.catalogImplementation", "hive")
                .enableHiveSupport()
                // Fewer shuffle partitions, fewer tiny files.
                .config("spark.sql.shuffle.partitions", System.getProperty("spark.sql.shuffle.partitions", "8"))
                .getOrCreate();
    }

    public static String storageRoot() {
        String fromSysProp = System.getProperty("bal.storage.root");
        if (fromSysProp != null && !fromSysProp.isBlank()) {
            return normalizeOsPath(fromSysProp);
        }
        String fromEnv = System.getenv("BAL_STORAGE_ROOT");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return normalizeOsPath(fromEnv);
        }
        return normalizeOsPath(".");
    }

    public static String latestPath() {
        return Paths.get(storageRoot(), "bal_latest").toString();
    }

    public static boolean existsLocal(String path) {
        try {
            return Files.exists(Paths.get(path));
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Normalize paths into something Spark/Hadoop accepts; keep URIs as-is.
     */
    public static String toSparkPath(String inputPath) {
        if (inputPath == null) return null;
        String p = stripQuotes(inputPath.trim());

        // MSYS /c/... -> C:/...
        p = normalizeOsPath(p);

        if (p.matches("^[a-zA-Z]+:.*")) {
            // Already a URI (file:/, hdfs:/, s3a:/...).
            if (p.startsWith("file:/") || p.startsWith("hdfs:/") || p.startsWith("s3a:/") || p.startsWith("abfss:/")) {
                return p;
            }
            // Plain drive path, e.g. C:/data/...
            if (p.matches("^[A-Za-z]:/.*")) {
                return toFileUri(p);
            }
        }
        // Relative or POSIX path, leave it alone.
        return p;
    }

    public static boolean isSkipSameInputEnabled() {
        String fromSysProp = System.getProperty("bal.skip.same.input");
        if (fromSysProp != null && !fromSysProp.isBlank()) {
            return Boolean.parseBoolean(fromSysProp);
        }
        String fromEnv = System.getenv("BAL_SKIP_IF_SAME_INPUT");
        return fromEnv != null && fromEnv.equalsIgnoreCase("true")
                || fromEnv != null && fromEnv.equalsIgnoreCase("1");
    }

    public static boolean isRecomputeSingleFileEnabled() {
        String fromSysProp = System.getProperty("bal.recompute.single.file");
        if (fromSysProp != null && !fromSysProp.isBlank()) {
            return Boolean.parseBoolean(fromSysProp);
        }
        String fromEnv = System.getenv("BAL_RECOMPUTE_SINGLE_FILE");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv.equalsIgnoreCase("true") || fromEnv.equalsIgnoreCase("1");
        }
        return true;
    }

    public static Optional<InputFingerprint> fingerprintLocalInput(String inputPath) {
        if (inputPath == null || inputPath.isBlank()) return Optional.empty();
        String raw = stripQuotes(inputPath.trim());
        if (raw.startsWith("http://") || raw.startsWith("https://")) {
            return Optional.empty();
        }

        Path path = null;
        try {
            if (raw.startsWith("file:/")) {
                path = Paths.get(URI.create(raw));
            } else {
                path = Paths.get(normalizeOsPath(raw));
            }
        } catch (Exception ignored) {
            return Optional.empty();
        }

        try {
            if (!Files.exists(path)) return Optional.empty();
            long size = Files.size(path);
            FileTime mtime = Files.getLastModifiedTime(path);
            return Optional.of(new InputFingerprint(path.toAbsolutePath().toString(), size, mtime.toMillis()));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public static Optional<InputFingerprint> readLastInputFingerprint() {
        try {
            Path marker = Paths.get(storageRoot(), "bal_last_input.txt");
            if (!Files.exists(marker)) return Optional.empty();
            String[] parts = Files.readString(marker).trim().split("\\|", -1);
            if (parts.length != 3) return Optional.empty();
            String path = parts[0];
            long size = Long.parseLong(parts[1]);
            long mtime = Long.parseLong(parts[2]);
            return Optional.of(new InputFingerprint(path, size, mtime));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public static void writeLastInputFingerprint(InputFingerprint fingerprint) {
        try {
            Path marker = Paths.get(storageRoot(), "bal_last_input.txt");
            String line = fingerprint.path() + "|" + fingerprint.size() + "|" + fingerprint.mtime();
            Files.writeString(marker, line);
        } catch (Exception ignored) {
        }
    }

    private static String toFileUri(String absolutePath) {
        String p = normalizeOsPath(absolutePath).replace('\\', '/');

        // Already a URI.
        if (p.startsWith("file:/")) return p;

        // Drive path: C:/...
        if (p.matches("^[A-Za-z]:/.*")) {
            // file:///C:/... works best with Hadoop Path parsing.
            return "file:///" + p.substring(0, 1).toUpperCase() + p.substring(1);
        }

        // POSIX absolute path: /...
        if (p.startsWith("/")) {
            return "file:" + p;
        }

        // Relative path.
        return "file:" + p;
    }

    /**
     * Normalize paths across shells (MSYS /c/... and Windows backslashes).
     */
    private static String normalizeOsPath(String raw) {
        if (raw == null) return null;
        String p = stripQuotes(raw.trim());

        // MSYS /c/... -> C:/...
        if (p.matches("^/[A-Za-z]/.*")) {
            char drive = Character.toUpperCase(p.charAt(1));
            String rest = p.substring(3); // trim "/c/"
            return drive + ":/" + rest;
        }

        // Backslashes to slashes on Windows.
        if (p.matches("^[A-Za-z]:\\\\.*")) {
            return p.replace('\\', '/');
        }

        return p;
    }

    private static String stripQuotes(String value) {
        if (value == null || value.length() < 2) return value;
        char first = value.charAt(0);
        char last = value.charAt(value.length() - 1);
        if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    public record InputFingerprint(String path, long size, long mtime) {}

    public static Path ensureDir(String outputDir) {
        Path out = Paths.get(outputDir);
        if (!Files.exists(out)) {
            try {
                Files.createDirectories(out);
            } catch (Exception e) {
                // Spark will try too; ignore here.
            }
        }
        return out;
    }

    public static List<String> listDiffDaysAfter(String targetDate) {
        Path base = Paths.get(storageRoot(), "bal.db", "bal_diff");
        if (!Files.exists(base)) {
            return new ArrayList<>();
        }
        try (Stream<Path> paths = Files.list(base)) {
            return paths
                    .filter(Files::isDirectory)
                    .map(p -> p.getFileName().toString())
                    .filter(name -> name.startsWith("day="))
                    .map(name -> name.substring("day=".length()))
                    .filter(day -> day.compareTo(targetDate) > 0)
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }
}
