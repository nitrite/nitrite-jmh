package org.dizitart.no2.v3.jmh;

import lombok.val;
import org.dizitart.no2.IndexOptions;
import org.dizitart.no2.IndexType;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.objects.ObjectRepository;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.dizitart.no2.objects.filters.ObjectFilters.eq;
import static org.dizitart.no2.v3.jmh.BenchmarkParam.*;

/**
 * @author Anindya Chatterjee
 */
public class NitriteProfiler {
    private static final AtomicInteger sequence = new AtomicInteger(1);

    public static void main(String[] args) throws Exception {
        String path = String.format("%s/nitrite-v3.db", TMP);
        Files.deleteIfExists(Paths.get(path));
        Nitrite db = Nitrite.builder().filePath(path).openOrCreate();
        ObjectRepository<ArbitraryData> repository = db.getRepository(ArbitraryData.class);
        repository.createIndex("index1", IndexOptions.indexOptions(IndexType.NonUnique));

        for (int i = 0; i < 5000; i++) {
            repository.insert(randomDatum());
        }

        for (int i = 0; i < 100000; i++) {
            val indexValue = RANDOM.nextInt();
            val value = RANDOM.nextDouble();

            List<ArbitraryData> list = repository.find(eq("index1", indexValue)).toList();
            list.forEach(item -> { assert item != null; });
        }
    }

    private static ArbitraryData randomDatum() {
        return new ArbitraryData()
                .id(sequence.incrementAndGet())
                .flag1(RANDOM.nextBoolean())
                .flag2(RANDOM.nextBoolean())
                .number1(RANDOM.nextDouble())
                .number2(RANDOM.nextDouble())
                .index1(RANDOM.nextInt())
                .text(GENERATOR.generate(100));
    }
}
