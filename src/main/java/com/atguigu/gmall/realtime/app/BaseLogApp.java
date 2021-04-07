package com.atguigu.gmall.realtime.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 *解析ods层用户行为日志数据
 */
public class BaseLogApp {
    // 定义用户行为主题信息
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_PAGE = "dwd_page_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度 和Kafka分区保持一直
        env.setParallelism(1);
        // 5000ms 开始一次 checkpoint
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        if(SystemUtils.IS_OS_WINDOWS){
            env.setStateBackend(new FsStateBackend("file:///F://weining//BigData//大数据技术之Flink实时项目//data//flink//checkpoint"));
        }else{
            env.setStateBackend(new FsStateBackend("file:///data/flink/checkpoints"));
        }

        // 用于解决hadoop权限问题
//        System.setProperty("","");

        String groupId = "ods_dwd_base_log_app";
        String topic = "ods_base_log";



        // 获取 KafkaStream流
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
//        kafkaSource.setCommitOffsetsOnCheckpoints(false);
//        kafkaSource.setStartFromGroupOffsets();

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);


        // 转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS
                .map(item -> {
                    String str = item;
                    return JSONObject.parseObject(item);

                }).returns(JSONObject.class);

        jsonObjectDS.print("=========>");

        env.execute("====>dwd_base_log Job");
    }
}
