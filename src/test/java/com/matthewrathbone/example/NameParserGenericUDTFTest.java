package com.matthewrathbone.example;

import java.util.ArrayList;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.junit.Assert;
import org.junit.Test;

public class NameParserGenericUDTFTest {
    @Test
    public void testUDTFNoSpaceAtAll() {
	    // set up the models we need
		NameParserGenericUDTF example = new NameParserGenericUDTF();
	    ObjectInspector[] inputOI = {PrimitiveObjectInspectorFactory.javaStringObjectInspector};
        
	    // create the actual UDF arguments
	    String name = "Smith";
	    
	    // the value exists
	    try{
	    	example.initialize(inputOI);		    
	    }catch(Exception ex){
	    	;
	    }
        
	    ArrayList<Object[]> results = example.processInputRecord(name);
	    Assert.assertEquals(0, results.size());
	}
    
    @Test
    public void testUDTFOneSpace() {
	    // set up the models we need
		NameParserGenericUDTF example = new NameParserGenericUDTF();
	    ObjectInspector[] inputOI = {PrimitiveObjectInspectorFactory.javaStringObjectInspector};
        
	    // create the actual UDF arguments
	    String name = "John Smith";
	    
	    // the value exists
	    try{
	    	example.initialize(inputOI);		    
	    }catch(Exception ex){
	    	;
	    }
        
	    ArrayList<Object[]> results = example.processInputRecord(name);
	    Assert.assertEquals(1, results.size());
	    Assert.assertEquals("John", results.get(0)[0]);
	    Assert.assertEquals("Smith", results.get(0)[1]);
	}
    
    @Test
    public void testUDTFSpaceAndConstruction() {
	    // set up the models we need
		NameParserGenericUDTF example = new NameParserGenericUDTF();
	    ObjectInspector[] inputOI = {PrimitiveObjectInspectorFactory.javaStringObjectInspector};
        
	    // create the actual UDF arguments
	    String name = "John and Ann White";
	    
	    // the value exists
	    try{
	    	example.initialize(inputOI);		    
	    }catch(Exception ex){
	    	;
	    }
        
	    ArrayList<Object[]> results = example.processInputRecord(name);
	    Assert.assertEquals(2, results.size());
	    Assert.assertEquals("John", results.get(0)[0]);
	    Assert.assertEquals("White", results.get(0)[1]);
	    Assert.assertEquals("Ann", results.get(1)[0]);
	    Assert.assertEquals("White", results.get(1)[1]);
	}
    
    @Test
    public void testUDTFTooManySpaces() {
	    // set up the models we need
		NameParserGenericUDTF example = new NameParserGenericUDTF();
	    ObjectInspector[] inputOI = {PrimitiveObjectInspectorFactory.javaStringObjectInspector};
        
	    // create the actual UDF arguments
	    String name = "Blah Blah Blah Blah";
	    
	    // the value exists
	    try{
	    	example.initialize(inputOI);		    
	    }catch(Exception ex){
	    	;
	    }
        
	    ArrayList<Object[]> results = example.processInputRecord(name);
	    Assert.assertEquals(0, results.size());
	} 
}
