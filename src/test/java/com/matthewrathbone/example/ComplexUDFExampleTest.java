package com.matthewrathbone.example;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

public class ComplexUDFExampleTest {
  
  
  @Test
  public void testComplexUDFReturnsCorrectValues() throws HiveException {
    
    // set up the models we need
    ComplexUDFExample example = new ComplexUDFExample();
    ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);
    JavaBooleanObjectInspector resultInspector = (JavaBooleanObjectInspector) example.initialize(new ObjectInspector[]{listOI, stringOI});
    
    // create the actual UDF arguments
    List<String> list = new ArrayList<String>();
    list.add("a");
    list.add("b");
    list.add("c");
    
    // test our results
    
    // the value exists
    Object result = example.evaluate(new DeferredObject[]{new DeferredJavaObject(list), new DeferredJavaObject("a")});
    Assert.assertEquals(true, resultInspector.get(result));
    
    // the value doesn't exist
    Object result2 = example.evaluate(new DeferredObject[]{new DeferredJavaObject(list), new DeferredJavaObject("d")});
    Assert.assertEquals(false, resultInspector.get(result2));
    
    // arguments are null
    Object result3 = example.evaluate(new DeferredObject[]{new DeferredJavaObject(null), new DeferredJavaObject(null)});
    Assert.assertNull(result3);
  }
}
