package com.matthewrathbone.example;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.ql.exec.Description;

@Description(name = "letters", value = "_FUNC_(expr) - Returns total number of letters in all the strings of a column.")
public class TotalNumOfLettersGenericUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        
        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,
                            "Argument must be PRIMITIVE, but "
                            + oi.getCategory().name()
                            + " was passed.");
        }
        
        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;
        
        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentTypeException(0,
                            "Argument must be String, but "
                            + inputOI.getPrimitiveCategory().name()
                            + " was passed.");
        }
        
        return new TotalNumOfLettersEvaluator();
    }

    public static class TotalNumOfLettersEvaluator extends GenericUDAFEvaluator {

        PrimitiveObjectInspector inputOI;
        ObjectInspector outputOI;
        PrimitiveObjectInspector integerOI;
        
        int total = 0;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
        	
            assert (parameters.length == 1);
            super.init(m, parameters);
           
            // init input object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
            	integerOI = (PrimitiveObjectInspector) parameters[0];
            }

            // init output object inspectors
            // For partial function - array of integers
            outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                    ObjectInspectorOptions.JAVA);
            return outputOI;

        }

        /**
         * class for storing the current sum of letters
         */
        static class LetterSumAgg implements AggregationBuffer {
            int sum = 0;
            void add(int num){
            	sum += num;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            LetterSumAgg result = new LetterSumAgg();
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
        	LetterSumAgg myagg = new LetterSumAgg();
        }
        
        private boolean warned = false;

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            if (parameters[0] != null) {
                LetterSumAgg myagg = (LetterSumAgg) agg;
                Object p1 = ((PrimitiveObjectInspector) inputOI).getPrimitiveJavaObject(parameters[0]);
                myagg.add(String.valueOf(p1).length());
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            LetterSumAgg myagg = (LetterSumAgg) agg;
            total += myagg.sum;
            return total;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial != null) {
                
                LetterSumAgg myagg1 = (LetterSumAgg) agg;
                
                Integer partialSum = (Integer) integerOI.getPrimitiveJavaObject(partial);
                
                LetterSumAgg myagg2 = new LetterSumAgg();
                
                myagg2.add(partialSum);
                myagg1.add(myagg2.sum);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            LetterSumAgg myagg = (LetterSumAgg) agg;
            total = myagg.sum;
            return myagg.sum;
        }

    }
}
