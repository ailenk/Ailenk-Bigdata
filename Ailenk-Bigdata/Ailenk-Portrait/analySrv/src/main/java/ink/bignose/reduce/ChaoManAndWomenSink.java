package ink.bignose.reduce;

import ink.bignose.entity.ChaomanAndWomenInfo;
import ink.bignose.util.MongoUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

/**
 * Created by ailenk on 2019/1/6.
 */
public class ChaoManAndWomenSink implements SinkFunction<ChaomanAndWomenInfo> {
    @Override
    public void invoke(ChaomanAndWomenInfo value, Context context) throws Exception {
        String chaotype = value.getChaotype();
        long count = value.getCount();
        Document doc = MongoUtils.findoneby("chaoManAndWomenstatics","bignosePortrait",chaotype);
        if(doc == null){
            doc = new Document();
            doc.put("info",chaotype);
            doc.put("count",count);
        }else{
            Long countpre = doc.getLong("count");
            Long total = countpre+count;
            doc.put("count",total);
        }
        MongoUtils.saveorupdatemongo("chaoManAndWomenstatics","bignosePortrait",doc);
    }
}
