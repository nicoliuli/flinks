package com.wb.day02;

import com.alibaba.fastjson.JSON;
import com.wb.common.SourceModel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/**
 *
 */
public class KafkaDemo {


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        //设置eventTime，默认为processTime即系统处理时间，我们需要统计一小时内的数据，也就是数据带的时间eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setStateBackend(new FsStateBackend("file:///Users/liuli/Desktop/kafka"));
        env.enableCheckpointing(10000);


        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), kafkaProp()));

        SingleOutputStreamOperator<SourceModel> stream = dataStreamSource.map(new MapFunction<String, SourceModel>() {
            @Override
            public SourceModel map(String value) throws Exception {
                return JSON.parseObject(value, SourceModel.class);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SourceModel>(Time.milliseconds(10)) {
            @Override
            public long extractTimestamp(SourceModel sensor) {
                return sensor.getTime();
            }
        });


        DataStream<String> result = stream
                .keyBy(new KeySelector<SourceModel, Long>() {
                    @Override
                    public Long getKey(SourceModel commonModel) throws Exception {
                        return commonModel.getId();
                    }
                })
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<SourceModel, String, Long, TimeWindow>() {

                    private ValueState<Long> playingLiveStreamNumberValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor descriptor = new ValueStateDescriptor("playingLiveStreamNumberValueState", Long.class);
                        this.playingLiveStreamNumberValueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void process(Long key, Context context, Iterable<SourceModel> iterable,
                                        Collector<String> collector) throws Exception {
                        Long playingLiveStreamNumber = this.playingLiveStreamNumberValueState.value();

                        if (null == playingLiveStreamNumber) {
                            playingLiveStreamNumber = 0L;
                        }

                        List<SourceModel> sourceModels = (List<SourceModel>) iterable;

                        for (SourceModel sourceModel : sourceModels) {
                            if ("open".equals(sourceModel.getType())) {
                                playingLiveStreamNumber++;
                            } else {
                                playingLiveStreamNumber--;
                            }
                            if (playingLiveStreamNumber <= 0L) {
                                playingLiveStreamNumber = 0L;
                            }
                        }


                        this.playingLiveStreamNumberValueState.update(playingLiveStreamNumber);
                        collector.collect("key：" + key + "，直播数：" + playingLiveStreamNumber);
                    }

                    /*@Override
                    public void clear(Context context) throws Exception {
                        if(playingLiveStreamNumberValueState!=null){
                            playingLiveStreamNumberValueState.clear();
                        }
                    }*/
                });

        result.print();


        env.execute("Flink Streaming Java API Skeleton");
    }


    // kafka配置
    private static Properties kafkaProp() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("zookeeper.connect", "localhost:2181");
        prop.put("group.id", "test");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("auto.offset.reset", "latest");
        return prop;
    }

}


