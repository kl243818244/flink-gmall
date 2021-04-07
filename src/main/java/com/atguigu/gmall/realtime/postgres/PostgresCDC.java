package com.atguigu.gmall.realtime.postgres;

import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 *解析ods层用户行为日志数据
 */
public class PostgresCDC {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

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

        bsTableEnv.executeSql("CREATE TABLE flink_cdc_source (\n" +
                "  id INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'postgres-cdc',\n" +
                "  'hostname' = '172.16.6.161',\n" +
                "  'port' = '5432',\n" +
                "  'database-name' = 'postgres',\n" +
                "  'schema-name' = 'public',\n" +
                "  'username' = 'win60_dcs',\n" +
                "  'password' = '@Welcome1',\n" +
                "  'table-name' = 'pg_cdc_source',\n" +
                "  'decoding.plugin.name' = 'pgoutput'\n" +
                ")");

/*        bsTableEnv.executeSql("CREATE TABLE flink_cdc_source (\n" +
                "  id INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:postgresql://172.16.6.161:5432/postgres',\n" +
                "   'table-name' = 'pg_cdc_source',\n" +
                "   'username' = 'win60_dcs',\n" +
                "   'password' = '@Welcome1'\n" +
                ")");*/

        Table table = bsTableEnv.sqlQuery("select * from flink_cdc_source");

//        DataStream<Row> rowDataStream = bsTableEnv.toAppendStream(table, Row.class);

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = bsTableEnv.toRetractStream(table, Row.class);

//        tuple2DataStream.addSink();


        tuple2DataStream.print("===============>");

        env.execute("====>set");
    }
}
