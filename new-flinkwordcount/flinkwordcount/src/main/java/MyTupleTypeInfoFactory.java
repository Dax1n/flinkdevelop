import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Created by Daxin on 2017/4/17.
 */

public class MyTupleTypeInfoFactory extends TypeInfoFactory<MyTuple> {

    @Override
    public TypeInformation<MyTuple> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {

      // return new MyTupleTypeInfo(genericParameters.get("T0"), genericParameters.get("T1"));
          return null;
    }
}