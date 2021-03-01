package com.wb.day08;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 */
public class MetricDemo {


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost",8888);

        SingleOutputStreamOperator<String> stream = dataStreamSource.map(new RichMapFunction<String, String>() {
            private Counter counter;
            private transient int valueToExpose = 0;
            private transient Histogram histogram;
            private transient DropwizardMeterWrapper meter;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.counter = getRuntimeContext().getMetricGroup().counter("myCounter",new MyCounter());
                getRuntimeContext().getMetricGroup().gauge("myGuage", new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return valueToExpose;
                    }
                });

                com.codahale.metrics.Histogram dropwizardHistogram =
                        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
                this.histogram = getRuntimeContext()
                        .getMetricGroup()
                        .histogram("myHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));

                com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();

                this.meter = getRuntimeContext()
                        .getMetricGroup()
                        .meter("myMeter", new DropwizardMeterWrapper(dropwizardMeter));
            }

            @Override
            public String map(String value) throws Exception {
                counter.inc();
                valueToExpose++;
                histogram.update(valueToExpose);
                this.meter.markEvent();
                return value;
            }
        });

        stream.print();
        env.execute("Flink Streaming Java API Skeleton");
    }


}

/**
 * 自定义counter聚合
 */
class MyCounter implements Counter {

    private long count = 0;
    @Override
    public void inc() {
        count++;
    }

    @Override
    public void inc(long l) {
        count += l;
    }

    @Override
    public void dec() {
        count--;
    }

    @Override
    public void dec(long l) {
        count -= l;
    }

    @Override
    public long getCount() {
        return count;
    }
}

class MyHistogram implements Histogram {

    @Override
    public void update(long l) {

    }

    @Override
    public long getCount() {
        return 0;
    }

    @Override
    public HistogramStatistics getStatistics() {
        return null;
    }
}


