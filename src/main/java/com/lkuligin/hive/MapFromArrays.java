package com.lkuligin.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.*;

/**
 * Created by lkuligin on 10/01/2017.
 */

public class MapFromArrays extends GenericUDF {
    private ListObjectInspector listKeys;
    private ListObjectInspector listValues;

    @Override
    public String getDisplayString(String[] arg0) {
        return "createmapudf(list_keys,list_values)";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("createmapudf only takes 2 lists - keys and values - as arguments");
        }

        ObjectInspector firstArg = arguments[0];
        ObjectInspector secArg = arguments[1];

        if (!(firstArg instanceof ListObjectInspector) || !(secArg instanceof  ListObjectInspector)) {
            throw new UDFArgumentException("both arguments must be lists");
        }

        this.listKeys = (ListObjectInspector) firstArg;
        this.listValues = (ListObjectInspector) secArg;

        if (!(this.listKeys.getListElementObjectInspector() instanceof StringObjectInspector) || !(this.listValues.getListElementObjectInspector() instanceof StringObjectInspector)) {
            throw new UDFArgumentException("both arguments must be lists of strings");
        }

        ObjectInspector returnType = ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return returnType;
    }

    @Override
    public Map<String, String> evaluate(DeferredObject[] arguments) throws HiveException {
        Map<String, String> output = new TreeMap<String,String>();

        if (this.listValues == null) {
            return output;
        }

        List<?> keys = this.listKeys.getList(arguments[0].get());
        List<?> values = this.listValues.getList(arguments[1].get());


        Iterator<?> itrKeys = keys.iterator();
        Iterator<?> itrValues = values.iterator();

        while (itrKeys.hasNext() && itrValues.hasNext()) {
            Object key = itrKeys.next();
            Object value = itrValues.next();
            if (key != null && value != null) {
                output.put(key.toString(), value.toString());
            }
        }

        return output;
    }
}
