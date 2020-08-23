package org.dizitart.no2.v3.jmh;

import lombok.*;
import lombok.experimental.Accessors;
import org.dizitart.no2.IndexOptions;
import org.dizitart.no2.IndexType;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.objects.ObjectRepository;
import org.jetbrains.annotations.Nullable;
import org.jooq.lambda.Unchecked;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.dizitart.no2.objects.filters.ObjectFilters.and;
import static org.dizitart.no2.objects.filters.ObjectFilters.eq;
import static org.dizitart.no2.v3.jmh.BenchmarkParam.*;

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
    @Param({"SQLITE_FILE", "SQLITE_MEMORY", "NITRITE_FILE", "NITRITE_MEMORY"})
    private Database database;
    @Param({"10000", "50000", "100000", "1000000"})
    private int dataSetSize;

    private ArbitraryData randomDatum() {
        return new ArbitraryData()
                .id(sequence.incrementAndGet())
                .flag1(RANDOM.nextBoolean())
                .flag2(RANDOM.nextBoolean())
                .number1(RANDOM.nextDouble())
                .number2(RANDOM.nextDouble())
                .index1(RANDOM.nextInt())
                .text(GENERATOR.generate(100));
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
        val statement = sqliteConnection.prepareStatement(INSERT_TABLE_STATEMENT);
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
        sqliteConnection.createStatement().execute(CREATE_TABLE_STATEMENT);
        sqliteConnection.createStatement().execute(CREATE_INDEX1_STATEMENT);
        sqliteQuery = sqliteConnection.prepareStatement(SELECT_INDEX1_STATEMENT);
    }

    private void tearDownSQLite() throws IOException {
        Optional.ofNullable(sqliteQuery)
                .ifPresent(Unchecked.consumer(Statement::close));
        Optional.ofNullable(sqliteConnection)
                .ifPresent(Unchecked.consumer(Connection::close));
        Files.deleteIfExists(Paths.get(String.format("%s/sqlite.db", TMP)));
    }

    private void setupNitrite(@Nullable String path) {
        nitrite = Optional.ofNullable(path).map(Unchecked.function(p -> {
            deleteFile(p);
            return Nitrite.builder().filePath(p).openOrCreate();
        })).orElseGet(() -> Nitrite.builder().openOrCreate());
        repository = nitrite.getRepository(ArbitraryData.class);
        repository.createIndex("index1", IndexOptions.indexOptions(IndexType.NonUnique));
    }

    private void deleteFile(@NonNull String path) throws IOException {
        Files.deleteIfExists(Paths.get(path));
    }

    private void tearDownNitrite() throws IOException {
        Optional.ofNullable(nitrite)
                .ifPresent(Nitrite::close);
        Files.deleteIfExists(Paths.get(String.format("%s/sqlite.db", TMP)));
    }

    public Collection<ArbitraryData> inquireNitrite(int indexValue, double value) {
        return repository.find(and(eq("index1", indexValue), eq("number1", value))).toList();
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
            case SQLITE_MEMORY:
                setupSQLite(database.path);
                insertDataIntoSQLite(data);
                break;
            case NITRITE_FILE:
            case NITRITE_MEMORY:
                setupNitrite(database.path);
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
            case NITRITE_FILE:
            case NITRITE_MEMORY:
                tearDownNitrite();
                break;
        }
    }

    @Getter
    @Accessors(fluent = true)
    @RequiredArgsConstructor
    public enum Database {
        NITRITE_MEMORY(null),
        NITRITE_FILE(String.format("%s/nitrite-v3.db", TMP)),
        SQLITE_MEMORY(":memory:"),
        SQLITE_FILE(String.format("%s/sqlite.db", TMP));

        @Nullable
        private final String path;
    }
}
