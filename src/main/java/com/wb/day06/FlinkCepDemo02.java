package com.wb.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class FlinkCepDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Login> loginEventStream = env.socketTextStream("localhost", 8888).map(new MapFunction<String, Login>() {
            @Override
            public Login map(String value) throws Exception {
                Login login = new Login("1", value);
                return login;
            }
        });

        Pattern<Login, Login> loginFailPattern = test8();


        PatternStream<Login> patternStream = CEP.pattern(
                loginEventStream.keyBy(Login::getUserId),
                loginFailPattern);


        DataStream<LoginWarn> loginFailDataStream = patternStream.select((Map<String, List<Login>> pattern) -> {
            List<Login> first = pattern.get("begin");
            List<Login> second = pattern.get("next");

            LoginWarn loginWarning = new LoginWarn();
            loginWarning.setType(first.get(0).getType());
            loginWarning.setUserId(first.get(0).getUserId());
            return loginWarning;
        });
        loginFailDataStream.print();

        env.execute("Flink Streaming Java API Skeleton");
    }

    private static Pattern<Login, Login> test8() {
        return Pattern.<Login>
                begin("begin")
                .where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context context) throws Exception {
                        return login.getType().equals("x");
                    }
                }).next("next").where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context<Login> context) throws Exception {
                        return login.getType().equals("c");
                    }
                }).times(2);
    }

    private static Pattern<Login, Login> test7() {
        return Pattern.<Login>
                begin("begin")
                .where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context context) throws Exception {
                        return login.getType().equals("x");
                    }
                }).next("next").where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context<Login> context) throws Exception {
                        return login.getType().equals("a");
                    }
                }).oneOrMore().until(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context<Login> context) throws Exception {
                        return login.getType().equals("c");
                    }
                });
    }

    // 直到输入c，否则一直能匹配上
    private static Pattern<Login, Login> test6() {
        return Pattern.<Login>
                begin("begin")
                .where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context context) throws Exception {
                        return login.getType().equals("x");
                    }
                }).oneOrMore().until(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context<Login> context) throws Exception {
                        return login.getType().equals("c");
                    }
                });
    }

    // 非确定性宽松连续性，上次匹配过的会继续匹配上 xxc xc xac
    private static Pattern<Login, Login> test5() {
        return Pattern.<Login>
                begin("begin")
                .where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context context) throws Exception {
                        return login.getType().equals("x");
                    }
                }).followedByAny("next").where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context<Login> context) throws Exception {
                        return login.getType().equals("c");
                    }
                });
    }

    // 宽松连续性,上次匹配过的回跳过  xxac xc xac ...
    private static Pattern<Login, Login> test4() {
        return Pattern.<Login>
                begin("begin")
                .where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context context) throws Exception {
                        return login.getType().equals("x");
                    }
                }).followedBy("next").where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context<Login> context) throws Exception {
                        return login.getType().equals("c");
                    }
                });
    }

    // 连续3次输入x
    private static Pattern<Login, Login> test3() {
        return Pattern.<Login>
                begin("begin")
                .where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context context) throws Exception {
                        return login.getType().equals("x");
                    }
                }).times(3);
    }

    private static Pattern<Login, Login> test2() {
        return Pattern.<Login>
                begin("begin")
                .where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context context) throws Exception {
                        return login.getType().equals("x");
                    }
                })
                .notNext("next")
                .where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login login, Context<Login> context) throws Exception {
                        return login.getType().equals("x");
                    }
                });
    }

    // 10s内连续两次输入x
    private static Pattern<Login, Login> test1() {
        return Pattern.<Login>
                begin("begin")
                .where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("x");
                    }
                })
                .next("next")
                .where(new IterativeCondition<Login>() {
                    @Override
                    public boolean filter(Login loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("x");
                    }
                })
                .within(Time.seconds(10));
    }

}


class Login {
    private String userId;//用户ID
    private String type;//登录类型

    public Login() {
    }

    public Login(String userId, String type) {
        this.userId = userId;
        this.type = type;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}

class LoginWarn {
    private String userId;
    private String type;

    public LoginWarn() {
    }

    public LoginWarn(String userId, String type) {
        this.userId = userId;
        this.type = type;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


    @Override
    public String toString() {
        return "LoginWarning{" +
                "userId='" + userId + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}

