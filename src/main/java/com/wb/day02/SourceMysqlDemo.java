package com.wb.day02;

import com.alibaba.fastjson.JSON;
import com.wb.common.ChatMsg;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class SourceMysqlDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        env.addSource(new MysqlSource()).map(new MapFunction<ChatMsg, Tuple2<Integer, Long>>() {
            @Override
            public Tuple2<Integer, Long> map(ChatMsg value) throws Exception {
                Tuple2 tuple2 = new Tuple2();
                tuple2.f0 = value.getFormat();
                tuple2.f1 = 1L;
                return tuple2;
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<Integer, Long>>() {
            @Override
            public Tuple2<Integer, Long> reduce(Tuple2<Integer, Long> value1, Tuple2<Integer, Long> value2) throws Exception {
                Tuple2 reduce = new Tuple2();
                reduce.f0 = value1.f0;
                reduce.f1 = value1.f1 + value2.f1;
                return reduce;
            }
        }).map(new MapFunction<Tuple2<Integer, Long>, String>() {
            @Override
            public String map(Tuple2<Integer, Long> value) throws Exception {
                return value.f0 + "-->" + value.f1;
            }
        }).print();

        env.execute("Flink Streaming Java API Skeleton");
    }

}


class MysqlSource extends RichSourceFunction<ChatMsg> {
    PreparedStatement ps;
    private Connection connection;
    volatile long idx = 1568;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select id,msg from chat_offline_msg_0 where id > " + idx + " limit 5000";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null)
            connection.close();
        if (ps != null)
            ps.close();
    }

    @Override
    public void run(SourceContext<ChatMsg> ctx) throws Exception {
        do {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) { // 循环读取
                String msg = resultSet.getString("msg");
                long id = resultSet.getLong("id");
                idx = id;
                ChatMsg chatMsg = JSON.parseObject(msg, ChatMsg.class);
                ctx.collect(chatMsg);
            }
            String sql = "select id,msg from chat_offline_msg_0 where id > " + idx + " limit 100";
            ps = this.connection.prepareStatement(sql);
        } while (true);

    }

    @Override
    public void cancel() {

    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("org.gjt.mm.mysql.Driver");
            con = DriverManager.getConnection("jdbc:mysql://devtest.wb.sql.wb-intra.com:13306/spy?useUnicode=true&characterEncoding=UTF-8", "test_liuli", "p!rM+LXMR9*e=");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}