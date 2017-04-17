import org.apache.flink.api.common.typeinfo.TypeInfo;

/**
 * Created by Daxin on 2017/4/17.
 */
@TypeInfo(MyTupleTypeInfoFactory.class)
public class MyTuple<T0, T1> {
    public T0 myfield0;
    public T1 myfield1;
}