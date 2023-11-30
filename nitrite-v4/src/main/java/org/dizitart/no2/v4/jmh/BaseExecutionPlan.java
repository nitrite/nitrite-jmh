package org.dizitart.no2.v4.jmh;

import lombok.*;
import lombok.experimental.Accessors;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.common.util.StringUtils;
import org.dizitart.no2.mvstore.MVStoreModule;
import org.dizitart.no2.rocksdb.RocksDBModule;
import org.dizitart.no2.store.StoreModule;
import org.jetbrains.annotations.Nullable;
import org.jooq.lambda.Unchecked;
import org.openjdk.jmh.annotations.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Anindya Chatterjee
 */
@Data
@State(Scope.Benchmark)
public abstract class BaseExecutionPlan<T> {
    protected AtomicInteger sequence = null;
    protected Nitrite nitrite = null;
    protected Connection sqliteConnection = null;
    protected PreparedStatement sqliteQuery = null;

    @Param({
            "SQLITE_FILE",
            "SQLITE_MEMORY",
            "NITRITE_MVSTORE_FILE",
            "NITRITE_ROCKSDB_FILE",
            "NITRITE_MVSTORE_MEMORY",
    })
    protected Database database;

    @Param({"10000", "50000", "100000"})
    protected int dataSetSize;

    protected abstract void insertDataIntoNitrite(T[] data);
    protected abstract void insertDataIntoSQLite(T[] data) throws SQLException;
    protected abstract void setupNitrite(Database db) throws IOException;
    protected abstract T[] randomData();

    public abstract Collection<T> inquireNitrite(int indexValue, double value);
    public abstract Collection<T> inquireSQLite(int indexValue, double value) throws SQLException;

    @Setup
    public void setup() throws Exception {
        val data = randomData();
        switch (database) {
            case SQLITE_FILE:
            case SQLITE_MEMORY:
                setupSQLite(database.path);
                insertDataIntoSQLite(data);
                break;
            case NITRITE_MVSTORE_FILE:
            case NITRITE_ROCKSDB_FILE:
            case NITRITE_MVSTORE_MEMORY:
                setupNitrite(database);
                insertDataIntoNitrite(data);
                break;
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        switch (database) {
            case SQLITE_FILE:
            case SQLITE_MEMORY:
                tearDownSQLite();
                break;
            case NITRITE_MVSTORE_FILE:
            case NITRITE_ROCKSDB_FILE:
            case NITRITE_MVSTORE_MEMORY:
                tearDownNitrite(database);
                break;
        }
    }

    protected StoreModule getStoreModule(Database db) throws IOException {
        StoreModule storeModule = null;
        switch (db) {
            case NITRITE_MVSTORE_MEMORY:
                storeModule = MVStoreModule.withConfig().build();
                break;
            case NITRITE_MVSTORE_FILE:
                assert db.path != null;
                Files.deleteIfExists(Paths.get(db.path));
                storeModule = MVStoreModule.withConfig().filePath(db.path).build();
                break;
            case NITRITE_ROCKSDB_FILE:
                assert db.path != null;
                FileUtils.deleteDirectory(new File(db.path));
                storeModule = RocksDBModule.withConfig().filePath(db.path).build();
                break;
            default:
                break;
        }
        return storeModule;
    }

    private void setupSQLite(@NonNull String path) throws SQLException, IOException {
        Files.deleteIfExists(Paths.get(path));
        val jdbcUrl = String.format("jdbc:sqlite:%s", path);
        sqliteConnection = DriverManager.getConnection(jdbcUrl);
        sqliteConnection.createStatement().execute(BenchmarkParam.CREATE_TABLE_STATEMENT);
        sqliteConnection.createStatement().execute(BenchmarkParam.CREATE_INDEX1_STATEMENT);
        sqliteQuery = sqliteConnection.prepareStatement(BenchmarkParam.SELECT_INDEX1_STATEMENT);
    }

    private void tearDownSQLite() throws IOException {
        Optional.ofNullable(sqliteQuery)
                .ifPresent(Unchecked.consumer(Statement::close));
        Optional.ofNullable(sqliteConnection)
                .ifPresent(Unchecked.consumer(Connection::close));
        Files.deleteIfExists(Paths.get(String.format("%s/sqlite.db", BenchmarkParam.TMP)));
    }

    private void tearDownNitrite(Database db) throws IOException {
        if (nitrite != null) {
            nitrite.close();
        }

        if (!StringUtils.isNullOrEmpty(db.path)) {
            if (db == Database.NITRITE_ROCKSDB_FILE) {
                FileUtils.deleteDirectory(new File(db.path));
            } else {
                Files.deleteIfExists(Paths.get(db.path));
            }
        }
    }

    @Getter
    @Accessors(fluent = true)
    @RequiredArgsConstructor
    public enum Database {
        NITRITE_MVSTORE_MEMORY(null),
        NITRITE_MVSTORE_FILE(String.format("%s/nitrite-mvstore-v4.db", BenchmarkParam.TMP)),
        NITRITE_ROCKSDB_FILE(String.format("%s/nitrite-rocksdb-v4.db", BenchmarkParam.TMP)),
        SQLITE_MEMORY(":memory:"),
        SQLITE_FILE(String.format("%s/sqlite.db", BenchmarkParam.TMP));

        @Nullable
        private final String path;
    }
}
