package com.itau.pmml;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class RegressionUDF extends GenericUDF {
    private static final Log LOG = LogFactory.getLog(DecisionTreeUDF.class.getName());
    PMMLUtil _evaluator;
    List<PrimitiveObjectInspector> inputOI;
    private ObjectInspector[] _inputObjectInspector = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        LOG.info("meulog init");

        inputOI = new ArrayList<>();
        LOG.info("meulog tentando evaluator");
        _evaluator = new PMMLUtil("LogisticRegressionPOC.pmml");
        LOG.info("meulog evaluator" + _evaluator);
        _inputObjectInspector = arguments;
        for (ObjectInspector o :
                arguments) {
            inputOI.add((PrimitiveObjectInspector) o);
        }
        ObjectInspector ret = _evaluator.Initialize(arguments);

        return ret;

    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return _evaluator.evaluateComplex(_inputObjectInspector, arguments);
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Regressão";
    }
}
