package ink.bignose.reduce;

import ink.bignose.entity.EmaiInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Created by ailenk on 2019/1/5.
 */
public class EmailReduce implements ReduceFunction<EmaiInfo>{


    @Override
    public EmaiInfo reduce(EmaiInfo emaiInfo, EmaiInfo t1) throws Exception {
        String emailtype = emaiInfo.getEmailtype();
        Long count1 = emaiInfo.getCount();

        Long count2 = t1.getCount();

        EmaiInfo emaiInfofinal = new EmaiInfo();
        emaiInfofinal.setEmailtype(emailtype);
        emaiInfofinal.setCount(count1+count2);

        return emaiInfofinal;
    }
}
