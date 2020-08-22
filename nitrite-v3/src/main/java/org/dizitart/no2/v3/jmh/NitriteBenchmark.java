package org.dizitart.no2.v3.jmh;

import lombok.val;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.dizitart.no2.v3.jmh.BenchmarkParam.*;

/**
 * @author Anindya Chatterjee
 */
@BenchmarkMode({
        Mode.AverageTime,
        Mode.Throughput,
})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class NitriteBenchmark {

    @Benchmark
    @Fork(value = FORKS, jvmArgsAppend = {
            "-Xmx8192m",
            "-Xmn6144m"})
    @Warmup(iterations = WARMUPS, time = MILLISECONDS, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = ITERATIONS, time = MILLISECONDS, timeUnit = TimeUnit.MILLISECONDS)
    public void queryWithJacksonMapper(ExecutionPlan plan, Blackhole blackhole) throws Exception {
        val indexValue = RANDOM.nextInt();
        val value = RANDOM.nextDouble();
        Collection<ArbitraryData> results = null;
        switch (plan.getDatabase()) {
            case NITRITE_FILE:
            case NITRITE_MEMORY:
                results = plan.inquireNitrite(indexValue, value);
                break;
            case SQLITE_FILE:
            case SQLITE_MEMORY:
                results = plan.inquireSQLite(indexValue, value);
                break;
        }
        blackhole.consume(results);
    }

    @Benchmark
    @Fork(value = FORKS, jvmArgsAppend = {
            "-Xmx8192m",
            "-Xmn6144m"})
    @Warmup(iterations = WARMUPS, time = MILLISECONDS, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = ITERATIONS, time = MILLISECONDS, timeUnit = TimeUnit.MILLISECONDS)
    public void queryWithMappable(MappableExecutionPlan plan, Blackhole blackhole) throws Exception {
        val indexValue = RANDOM.nextInt();
        val value = RANDOM.nextDouble();
        Collection<MappableArbitraryData> results = null;
        switch (plan.getDatabase()) {
            case NITRITE_FILE:
            case NITRITE_MEMORY:
                results = plan.inquireNitrite(indexValue, value);
                break;
            case SQLITE_FILE:
            case SQLITE_MEMORY:
                results = plan.inquireSQLite(indexValue, value);
                break;
        }
        blackhole.consume(results);
    }
}
