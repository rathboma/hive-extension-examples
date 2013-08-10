package com.matthewrathbone.example;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class SimpleUDFExampleTest {
  
  @Test
  public void testUDF() {
    SimpleUDFExample example = new SimpleUDFExample();
    Assert.assertEquals("Hello world", example.evaluate(new Text("world")).toString());
  }
  
  @Test
  public void testUDFNullCheck() {
    SimpleUDFExample example = new SimpleUDFExample();
    Assert.assertNull(example.evaluate(null));
  }
}