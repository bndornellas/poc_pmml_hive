package com.poc.pmml;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.util.ArrayList;
import java.util.List;


@Description(
        name = "DecisionTreeUDF"
)
public class DecisionTreeUDF extends GenericUDF {
    private static final Log LOG = LogFactory.getLog(DecisionTreeUDF.class.getName());
    PMMLUtil _evaluator;
    List<PrimitiveObjectInspector> inputOI;


    private ObjectInspector[] _inputObjectInspector = null;


    private GenericUDFUtils.ReturnObjectInspectorResolver
            _returnObjectInspectorResolver = null;


    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        LOG.info("meulog init");



        LOG.info("meulog tentando evaluator");
        _evaluator = new PMMLUtil("/home/admhadoop/sample/DecisionTreePoc.pmml");
        LOG.info("meulog evaluator" + _evaluator);
        _inputObjectInspector = arguments;
        for (ObjectInspector o :
                arguments) {
            inputOI.add((PrimitiveObjectInspector) o);
        }
        return _evaluator.Initialize(arguments);


    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {


        return _evaluator.evaluateComplex(_inputObjectInspector, arguments);

    }


    @Override
    public String getDisplayString(String[] children) {
        return "DecisionTreePoc";
    }
}
