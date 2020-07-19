package ink.bignose.reduce;

import ink.bignose.entity.UseTypeInfo;
import ink.bignose.util.MongoUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

/**
 * Created by ailenk on 2019/1/6.
 */
public class UseTypeSink implements SinkFunction<UseTypeInfo> {
    @Override
    public void invoke(UseTypeInfo value, Context context) throws Exception {
        String usetype = value.getUsetype();
        long count = value.getCount();
        Document doc = MongoUtils.findoneby("usetypestatics","bignosePortrait",usetype);
        if(doc == null){
            doc = new Document();
            doc.put("info",usetype);
            doc.put("count",count);
        }else{
            Long countpre = doc.getLong("count");
            Long total = countpre+count;
            doc.put("count",total);
        }
        MongoUtils.saveorupdatemongo("usetypestatics","bignosePortrait",doc);
    }
}
