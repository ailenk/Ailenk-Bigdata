package ink.bignose.task;

import ink.bignose.entity.KeyWordEntity;
import ink.bignose.map.KeywordMap;
import ink.bignose.map.KeywordMap2;
import ink.bignose.reduce.KeyWordReduce2;
import ink.bignose.reduce.KeywordReduce;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Created by ailenk on 2019/1/20.
 */
public class QuarterKeyWordTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<KeyWordEntity> mapresult = text.map(new KeywordMap());
        DataSet<KeyWordEntity> reduceresutl = mapresult.groupBy("userid").reduce(new KeywordReduce());
        DataSet<KeyWordEntity> mapresult2 = reduceresutl.map(new KeywordMap2());
        DataSet<KeyWordEntity> reduceresult2 = mapresult2.reduce(new KeyWordReduce2());
        Long totaldoucment = 0l;
        try {
            totaldoucment = reduceresult2.collect().get(0).getTotaldocumet();
            DataSet<KeyWordEntity> mapfinalresult = mapresult.map(new KeyWordMapfinal(totaldoucment,3,"quarter"));
            mapfinalresult.writeAsText("hdfs://bignose/test/quarter");//hdfs的路径
            env.execute("QuarterKeyWordTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
