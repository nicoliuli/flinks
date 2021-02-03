package com.wb.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkMysqlDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        sinkMysql(env);
        env.execute("Flink Streaming Java API Skeleton");
    }


    private static void sinkMysql(StreamExecutionEnvironment env){
        env.socketTextStream("localhost", 8888).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).addSink(new MysqlSink());
    }
}

// 为什么要rich呢？rich是富函数，有上下文、生命周期等方法
class MysqlSink extends RichSinkFunction<String> {
    PreparedStatement ps;
    Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建jdbc连接
        connection = getConnection();
        String sql = "INSERT INTO abc_test (testabc) VALUES(?)";
        ps = this.connection.prepareStatement(sql);

    }

    @Override
    public void close() throws Exception {
        // 关闭jdbc连接
        if(connection != null)
            connection.close();
        if(ps != null)
            ps.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        ps.setString(1,value.toString()); // 填充字段
        ps.execute();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("org.gjt.mm.mysql.Driver");
            con = DriverManager.getConnection("jdbc:mysql://xxxx", "root", "root");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
