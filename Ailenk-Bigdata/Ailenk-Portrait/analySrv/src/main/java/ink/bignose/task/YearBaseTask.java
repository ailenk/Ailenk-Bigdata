package ink.bignose.task;

import ink.bignose.entity.YearBase;
import ink.bignose.map.YearBaseMap;
import ink.bignose.reduce.YearBaseReduce;
import ink.bignose.util.MongoUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;

/**
 * Created by ailenk on 2019/1/5.
 */
public class YearBaseTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<YearBase> mapresult = text.map(new YearBaseMap());
        DataSet<YearBase> reduceresutl = mapresult.groupBy("groupfield").reduce(new YearBaseReduce());
        try {
            List<YearBase> reusltlist = reduceresutl.collect();
            for(YearBase yearBase:reusltlist){
                    String yeartype = yearBase.getYeartype();
                    Long count = yearBase.getCount();

                Document doc = MongoUtils.findoneby("yearbasestatics","bignosePortrait",yeartype);
                if(doc == null){
                    doc = new Document();
                    doc.put("info",yeartype);
                    doc.put("count",count);
                }else{
                    Long countpre = doc.getLong("count");
                    Long total = countpre+count;
                    doc.put("count",total);
                }
                MongoUtils.saveorupdatemongo("yearbasestatics","bignosePortrait",doc);
            }
            env.execute("year base analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
