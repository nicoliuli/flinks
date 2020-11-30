package com.wb.day01;

import com.alibaba.fastjson.JSONObject;
import com.sun.jmx.snmp.Timestamp;
import com.wb.common.UserAction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

/**
 *
 */
public class TopNDemo {


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        //设置eventTime，默认为processTime即系统处理时间，我们需要统计一小时内的数据，也就是数据带的时间eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<String>("USER_ACTION", new SimpleStringSchema(), kafkaProp()));

        dataStreamSource.map(value -> JSONObject.parseObject(value, UserAction.class)) // 从kafka收到的string转化为Object
                .assignTimestampsAndWatermarks(new UserActionTSExtractor()) //将乱序的数据进行抽取出来，设置watermark，数据如果晚到10秒的会被丢弃
                .filter(value -> value.getBehavior().contains("buy"))// 过滤出购买的行为
                .keyBy("itemId") // 根据商品id聚合
                .timeWindow(Time.minutes(60L), Time.minutes(5L))
                .aggregate(new CountAgg(), new WindowResultFunction()) // 1、积攒了分组后的，当前窗口的所有数据
                .keyBy("windowEnd")                        // 2、然后分组
                .process(new TopNHotItems(3))                   // 3、整个窗口的数据收集齐，然后排序得出前三名
                .print();

        env.execute("Flink Streaming Java API Skeleton");
    }


    // kafka配置
    private static Properties kafkaProp() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("zookeeper.connect", "localhost:2181");
        prop.put("group.id", "USER_ACTION");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("auto.offset.reset", "latest");
        return prop;
    }

}

class UserActionTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserAction> {

    public UserActionTSExtractor() {
        super(Time.seconds(10L));//最对延迟到达的时间
    }

    @Override
    public long extractTimestamp(UserAction userAction) {
        return userAction.getTimestamp();
    }
}

/**
 * COUNT 聚合函数实现，每出现一条记录加一。AggregateFunction<输入，汇总，输出>
 * 属于增量计算，效率高
 */
class CountAgg implements AggregateFunction<UserAction, Long, Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserAction userAction, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}

// 自定义窗口输出。用于输出结果的窗口WindowFunction<输入，输出，键，窗口>
// WindowFunction输入是CountAgg的输出
class WindowResultFunction extends ProcessWindowFunction<Long, ItemBuyCount, Tuple, TimeWindow> {

    /* 这个是旧版本的，implements WindowFunction
    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Long> iterable, Collector<ItemBuyCount> collector) throws Exception {
        Long itemId = ((Tuple1<Long>) key).f0;
        Long count = iterable.iterator().next();
        collector.collect(ItemBuyCount.of(itemId, window.getEnd(), count));

    }*/

    // iterable会缓存keyby后所有的当前窗口时间内的聚合后的数据
    @Override
    public void process(Tuple key, Context context, Iterable<Long> iterable, Collector<ItemBuyCount> collector) throws Exception {
        Long itemId = ((Tuple1<Long>) key).f0;
        Long count = iterable.iterator().next(); // 返回当前keyby的窗口聚合后的数据
        collector.collect(ItemBuyCount.of(itemId, context.window().getEnd(), count));  // 输出到下一个算子
    }
}

// 商品购买量（窗口操作的输出类型）
class ItemBuyCount {
    public long itemId; //商品ID;
    public long windowEnd; //窗口结束时间戳
    public long buyCount; //购买数量

    public static ItemBuyCount of(long itemId, long windowEnd, long buyCount) {
        ItemBuyCount itemBuyCount = new ItemBuyCount();
        itemBuyCount.itemId = itemId;
        itemBuyCount.windowEnd = windowEnd;
        itemBuyCount.buyCount = buyCount;
        return itemBuyCount;
    }
}

// 求某个窗口中前N名的热门点击商品，key为窗口时间戳，输出为Top N 的结果字符串
class TopNHotItems extends KeyedProcessFunction<Tuple, ItemBuyCount, List<ItemBuyCount>> {

    private final int topSize;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    //用于存储商品与购买数的状态，待收齐同一个窗口的数据后，再触发 Top N 计算
    private ListState<ItemBuyCount> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //状态注册
        ListStateDescriptor<ItemBuyCount> itemViewStateDesc = new ListStateDescriptor<ItemBuyCount>(
                "itemState-state", ItemBuyCount.class
        );
        itemState = getRuntimeContext().getListState(itemViewStateDesc);
    }

    @Override
    public void processElement(
            ItemBuyCount input,
            Context context,
            Collector<List<ItemBuyCount>> collector
    ) throws Exception {
        //每条数据都保存到状态
        itemState.add(input);
        //注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收集好了所有 windowEnd的商品数据
        context.timerService().registerEventTimeTimer(input.windowEnd + 1);
    }

     // 当时间窗口到了，触发这个操作
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<ItemBuyCount>> out) throws Exception {
        //获取收集到的所有商品点击量
        List<ItemBuyCount> allItems = new ArrayList<ItemBuyCount>();
        for (ItemBuyCount item : itemState.get()) {
            allItems.add(item);
        }
        //提前清除状态中的数据，释放空间
        itemState.clear();
        //按照点击量从大到小排序
        allItems.sort(new Comparator<ItemBuyCount>() {
            @Override
            public int compare(ItemBuyCount o1, ItemBuyCount o2) {
                return (int) (o2.buyCount - o1.buyCount);
            }
        });

        List<ItemBuyCount> itemBuyCounts = new ArrayList<>();
        //将排名信息格式化成String，方便打印
        StringBuilder result = new StringBuilder();
        result.append("========================================\n");
        result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < topSize; i++) {
            ItemBuyCount currentItem = allItems.get(i);
            // No1:  商品ID=12224  购买量=2
            result.append("No").append(i).append(":")
                    .append("  商品ID=").append(currentItem.itemId)
                    .append("  购买量=").append(currentItem.buyCount)
                    .append("\n");
            itemBuyCounts.add(currentItem);
        }
        result.append("====================================\n\n");

        out.collect(itemBuyCounts);

    }
}