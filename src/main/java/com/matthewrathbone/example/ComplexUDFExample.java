package com.matthewrathbone.example;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

class ComplexUDFExample extends GenericUDF {

  ListObjectInspector listOI;
  StringObjectInspector elementOI;

  @Override
  public String getDisplayString(String[] arg0) {
    return "arrayContainsExample()"; // this should probably be better
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: List<T>, T");
    }
    // 1. Check we received the right object types.
    ObjectInspector a = arguments[0];
    ObjectInspector b = arguments[1];
    if (!(a instanceof ListObjectInspector) || !(b instanceof StringObjectInspector)) {
      throw new UDFArgumentException("first argument must be a list / array, second argument must be a string");
    }
    this.listOI = (ListObjectInspector) a;
    this.elementOI = (StringObjectInspector) b;
    
    // 2. Check that the list contains strings
    if(!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)) {
      throw new UDFArgumentException("first argument must be a list of strings");
    }
    
    // the return type of our function is a boolean, so we provide the correct object inspector
    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }
  
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    
    // get the list and string from the deferred objects using the object inspectors
    List<String> list = (List<String>) this.listOI.getList(arguments[0].get());
    String arg = elementOI.getPrimitiveJavaObject(arguments[1].get());
    
    // check for nulls
    if (list == null || arg == null) {
      return null;
    }
    
    // see if our list contains the value we need
    for(String s: list) {
      if (arg.equals(s)) return new Boolean(true);
    }
    return new Boolean(false);
  }
  
}