package com.matthewrathbone.example;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


@Description(
  name="SimpleUDFExample",
  value="returns 'hello x', where x is whatever you give it (STRING)",
  extended="SELECT simpleudfexample('world') from foo limit 1;"
  )
class SimpleUDFExample extends UDF {
  
  public Text evaluate(Text input) {
    if(input == null) return null;
    return new Text("Hello " + input.toString());
  }
}