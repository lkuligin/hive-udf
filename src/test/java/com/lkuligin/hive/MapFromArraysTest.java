package com.lkuligin.hive;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MapFromArraysTest {
    @Test
    public void testCreateMapUdfSimpleArrays() throws HiveException {
        MapFromArrays example = new MapFromArrays();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector arrayOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        MapObjectInspector resultInspector = (MapObjectInspector) example.initialize(new ObjectInspector[]{arrayOI, arrayOI});

        List<String> keys = new ArrayList<String>();
        keys.add("1");
        keys.add("2");
        List<String> values = new ArrayList<String>();
        values.add("1");
        values.add("2");

        Map<String, String> expectedMap = new TreeMap<String, String>();
        expectedMap.put("1", "1");
        expectedMap.put("2", "2");

        Object result = example.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(keys), new GenericUDF.DeferredJavaObject(values)});
        Map<?,?> mapResult = resultInspector.getMap(result);
        Assert.assertEquals(expectedMap.size(), mapResult.size());
        Assert.assertTrue(expectedMap.equals(mapResult));

    }

    @Test
    public void testCreateMapUdfNullArrays() throws HiveException {
        MapFromArrays example = new MapFromArrays();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector arrayOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        MapObjectInspector resultInspector = (MapObjectInspector) example.initialize(new ObjectInspector[]{arrayOI, arrayOI});

        List<String> keys = new ArrayList<String>();
        keys.add("1");
        keys.add("2");
        keys.add("3");
        List<String> values = new ArrayList<String>();
        values.add("1");
        values.add(null);
        values.add("3");

        Map<String, String> expectedMap = new TreeMap<String, String>();
        expectedMap.put("1", "1");
        expectedMap.put("3", "3");

        Object result = example.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(keys), new GenericUDF.DeferredJavaObject(values)});
        Map<?,?> mapResult = resultInspector.getMap(result);
        Assert.assertEquals(expectedMap.size(), mapResult.size());
        Assert.assertTrue(expectedMap.equals(mapResult));

    }

    @Test
    public void testCreateMapUdfNull() throws HiveException {
        MapFromArrays example = new MapFromArrays();
        ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector arrayOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
        MapObjectInspector resultInspector = (MapObjectInspector) example.initialize(new ObjectInspector[]{arrayOI, arrayOI});

        List<String> keys = new ArrayList<String>();
        List<String> values = new ArrayList<String>();

        Map<String, String> expectedMap = new TreeMap<String, String>();

        Object result = example.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(keys), new GenericUDF.DeferredJavaObject(values)});
        Map<?,?> mapResult = resultInspector.getMap(result);
        Assert.assertEquals(expectedMap.size(), mapResult.size());
        Assert.assertTrue(expectedMap.equals(mapResult));

    }

}
