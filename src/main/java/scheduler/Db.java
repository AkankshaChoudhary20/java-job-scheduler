package scheduler;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public final class Db {

    private Db() {}

    /**
     * Opens a connection to the SQLite file at {@code path}, tuned for
     * multi-connection concurrent access: WAL journal mode so readers don't
     * block the writer, and a busy timeout so a connection waits for a lock
     * instead of immediately throwing SQLITE_BUSY.
     */
    public static Connection connect(String path) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + path);
        try (Statement st = conn.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("PRAGMA busy_timeout=5000");
            st.execute("PRAGMA foreign_keys=ON");
        }
        return conn;
    }

    public static void initSchema(Connection conn) throws SQLException {
        String sql = readResourceOrFile("/schema/schema.sql", "schema/schema.sql");
        try (Statement st = conn.createStatement()) {
            for (String stmt : sql.split(";")) {
                String trimmed = stmt.trim();
                if (!trimmed.isEmpty()) {
                    st.execute(trimmed);
                }
            }
        }
    }

    private static String readResourceOrFile(String resourcePath, String filePath) {
        try (InputStream in = Db.class.getResourceAsStream(resourcePath)) {
            if (in != null) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            return java.nio.file.Files.readString(java.nio.file.Path.of(filePath));
        } catch (IOException e) {
            throw new UncheckedIOException("could not load schema from resource or file: " + e.getMessage(), e);
        }
    }
}
