package com.wb.day04.demo01;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class PlannerTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 基于老版本planner流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment.create(env,settings);

        // 基于老版本的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment.create(batchEnv);

        // 基于blink planner 的流处理
        EnvironmentSettings blinkSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment.create(env,blinkSetting);

        // 基于blink planner的批处理
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment.create(build);
    }
}
