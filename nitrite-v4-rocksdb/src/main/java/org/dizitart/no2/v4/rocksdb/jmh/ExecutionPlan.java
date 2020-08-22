package org.dizitart.no2.v4.rocksdb.jmh;

import lombok.*;
import lombok.experimental.Accessors;
import org.apache.commons.io.FileUtils;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.index.IndexOptions;
import org.dizitart.no2.index.IndexType;

import org.dizitart.no2.mapper.JacksonMapperModule;
import org.dizitart.no2.repository.ObjectRepository;
import org.dizitart.no2.rocksdb.RocksDBModule;
import org.jetbrains.annotations.Nullable;
import org.jooq.lambda.Unchecked;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.dizitart.no2.filters.FluentFilter.where;

/**
 * @author Anindya Chatterjee
 */
@Data
@State(Scope.Benchmark)
public class ExecutionPlan {

    private AtomicInteger sequence = null;
    private Nitrite nitrite = null;
    private Connection sqliteConnection = null;
    private PreparedStatement sqliteQuery = null;
    private ObjectRepository<ArbitraryData> repository = null;
    @Param({"SQLITE_FILE", "NITRITE_FILE"})
    private Database database;
    @Param({"10000", "50000", "100000", "1000000"})
    private int dataSetSize;

    private ArbitraryData randomDatum() {
        return new ArbitraryData()
                .id(sequence.incrementAndGet())
                .flag1(BenchmarkParam.RANDOM.nextBoolean())
                .flag2(BenchmarkParam.RANDOM.nextBoolean())
                .number1(BenchmarkParam.RANDOM.nextDouble())
                .number2(BenchmarkParam.RANDOM.nextDouble())
                .index1(BenchmarkParam.RANDOM.nextInt())
                .text(BenchmarkParam.GENERATOR.generate(100));
    }

    private ArbitraryData[] randomData() {
        sequence = new AtomicInteger(0);
        return IntStream.range(0, dataSetSize)
                .mapToObj(index -> randomDatum())
                .toArray(ArbitraryData[]::new);
    }

    private void insertDataIntoNitrite(ArbitraryData[] data) {
        repository.insert(data);
    }

    private void insertDataIntoSQLite(ArbitraryData[] data) throws SQLException {
        sqliteConnection.setAutoCommit(false);
        val statement = sqliteConnection.prepareStatement(BenchmarkParam.INSERT_TABLE_STATEMENT);
        for (ArbitraryData datum : data) {
            statement.setInt(1, datum.id());
            statement.setString(2, datum.text());
            statement.setDouble(3, datum.number1());
            statement.setDouble(4, datum.number2());
            statement.setInt(5, datum.index1());
            statement.setBoolean(6, datum.flag1());
            statement.setBoolean(7, datum.flag2());
            statement.addBatch();
        }
        statement.executeBatch();
        sqliteConnection.setAutoCommit(true);
    }

    private void setupSQLite(@NonNull String path) throws SQLException, IOException {
        Files.deleteIfExists(Paths.get(path));
        val jdbcUrl = String.format("jdbc:sqlite:%s", path);
        sqliteConnection = DriverManager.getConnection(jdbcUrl);
        sqliteConnection.createStatement().execute(BenchmarkParam.CREATE_TABLE_STATEMENT);
        sqliteConnection.createStatement().execute(BenchmarkParam.CREATE_INDEX1_STATEMENT);
        sqliteQuery = sqliteConnection.prepareStatement(BenchmarkParam.SELECT_INDEX1_STATEMENT);
    }

    private void tearDownSQLite() {
        Optional.ofNullable(sqliteQuery)
                .ifPresent(Unchecked.consumer(Statement::close));
        Optional.ofNullable(sqliteConnection)
                .ifPresent(Unchecked.consumer(Connection::close));
    }

    private void setupNitrite(@Nullable String path) throws IOException {
        deleteFile(path);
        RocksDBModule module = RocksDBModule.withConfig()
                .filePath(path)
                .build();
        nitrite = Nitrite.builder().loadModule(module).loadModule(new JacksonMapperModule()).openOrCreate();

        repository = nitrite.getRepository(ArbitraryData.class);
        repository.createIndex("index1", IndexOptions.indexOptions(IndexType.NonUnique));
    }

    private void deleteFile(@NonNull String path) throws IOException {
        FileUtils.deleteDirectory(new File(path));
    }

    private void tearDownNitrite() {
        Optional.ofNullable(nitrite)
                .ifPresent(Nitrite::close);
    }

    public Collection<ArbitraryData> inquireNitrite(int indexValue, double value) {
        return repository.find(where("index1").eq(indexValue).and(where("number1").eq(value))).toList();
//        return repository.find(where("index1").eq(indexValue)).toList();
    }

    public Collection<ArbitraryData> inquireSQLite(int indexValue, double value) throws SQLException {
        sqliteQuery.clearParameters();
        sqliteQuery.setInt(1, indexValue);
        sqliteQuery.setDouble(2, value);
        val result = sqliteQuery.executeQuery();
        val data = new ArrayList<ArbitraryData>();
        while (result.next()) {
            val datum = new ArbitraryData()
                    .id(result.getInt("id"))
                    .text(result.getString("text"))
                    .number1(result.getDouble("number1"))
                    .number2(result.getDouble("number2"))
                    .index1(result.getInt("index1"))
                    .flag1(result.getBoolean("flag1"))
                    .flag2(result.getBoolean("flag2"));
            data.add(datum);
        }
        return data;
    }

    @Setup
    public void setup() throws Exception {
        val data = randomData();
        switch (database) {
            case SQLITE_FILE:
                setupSQLite(database.path);
                insertDataIntoSQLite(data);
                break;
            case NITRITE_FILE:
                setupNitrite(database.path);
                insertDataIntoNitrite(data);
                break;
        }
    }

    @TearDown
    public void tearDown() {
        switch (database) {
            case SQLITE_FILE:
                tearDownSQLite();
                break;
            case NITRITE_FILE:
                tearDownNitrite();
                break;
        }
    }

    @Accessors(fluent = true)
    @RequiredArgsConstructor
    public enum Database {
        NITRITE_FILE(String.format("%s/nitrite-v4-rocksdb.db", BenchmarkParam.TMP)),
        SQLITE_FILE(String.format("%s/sqlite.db", BenchmarkParam.TMP));

        @Nullable
        @Getter
        private final String path;
    }
}