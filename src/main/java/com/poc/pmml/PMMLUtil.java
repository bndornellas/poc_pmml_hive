package com.poc.pmml;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PMMLUtil {
    String _path;
    PMML pmml;
    Evaluator evaluator;
    private static final Log LOG = LogFactory.getLog(PMMLUtil.class.getName());

    private PrimitiveObjectInspector toObjectInspector(DataType dataType) throws UDFArgumentException {

        switch (dataType) {
            case STRING:
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            case INTEGER:
                return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
            case FLOAT:
                return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
            case DOUBLE:
                return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
            case BOOLEAN:
                return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
            default:
                throw new UDFArgumentException();
        }
    }

    public PMMLUtil(String path) throws UDFArgumentException {
        try {
            _path = path;


            InputStream is = new FileInputStream(path);
            pmml = org.jpmml.model.PMMLUtil.unmarshal(is);

            ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
            ModelEvaluator<?> modelEvaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
            evaluator = modelEvaluator;

        } catch (FileNotFoundException e) {
            throw new UDFArgumentException(e.getMessage());
        } catch (SAXException e) {
            throw new UDFArgumentException(e.getMessage());

        } catch (JAXBException e) {
            throw new UDFArgumentException(e.getMessage());
        }


    }

    public PMMLUtil(String path, boolean teste) throws UDFArgumentException {
        try {
            _path = path;

            InputStream is = getClass().getResourceAsStream(path);

            InputStream stream = new ByteArrayInputStream(path.getBytes(StandardCharsets.UTF_8.name()));


            pmml = org.jpmml.model.PMMLUtil.unmarshal(is);

            ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
            ModelEvaluator<?> modelEvaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
            evaluator = modelEvaluator;
        } catch (SAXException e) {
            throw new UDFArgumentException(e.getMessage());

        } catch (JAXBException e) {
            throw new UDFArgumentException(e.getMessage());
        }
        catch (Exception ex){
            throw new UDFArgumentException(ex.getMessage());
        }


    }


    public ObjectInspector Initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //objeto de retorno
        ObjectInspector ret = null;
        //pego os outputs para gerar o ObjectInspector seja struct ou não
        List<OutputField> outputFields = evaluator.getOutputFields();

        //se for um parametro retorna um ObjectInsoector Primitive
        if (outputFields.size() == 1) {
            if (outputFields.get(0).getDataType() != null) {
                ret = toObjectInspector(outputFields.get(0).getDataType());
            } else {
                ret = toObjectInspector(evaluator.getTargetFields().get(0).getDataType());
            }
        } else {
            //se forem dois ou mais vai ser struct
            List<ObjectInspector> fieldInspectors = Lists.newArrayList();
            List<String> names = new ArrayList<>();
            for (OutputField out : outputFields) {
                names.add(out.getName().getValue());
                LOG.info("*****************" + out.getName().getValue());
                fieldInspectors.add(toObjectInspector(out.getDataType()));
            }
            ret = ObjectInspectorFactory.getStandardStructObjectInspector(names, fieldInspectors);

        }

        return ret;


    }

    public Object evaluateComplex(ObjectInspector[] inspectors, GenericUDF.DeferredObject[] objects) throws HiveException {


        Map<FieldName, FieldValue> arguments = loadArguments(inspectors, objects);

        Map<FieldName, ?> result = evaluator.evaluate(arguments);

        Collection<?> values = result.values();
        for (Object o :
                values) {
            LOG.info(o != null ? o.toString() : "nada");
            LOG.info(o != null ? o.getClass().getName() : "nada");

        }

        LOG.info("values size" + values.size());
        if (evaluator.getOutputFields().size() == 1) {

            //Object targetValue = new DoubleWritable( Double.parseDouble(result.get(evaluator.getTargetFields().get(0)).toString()));
            //LOG.info("o aqui");
            return values.toArray()[0];
        }
        return storeResult(result);
    }

    private Object storeResult(Map<FieldName, ?> result) {
        return storeStruct(result);
    }


    private Object[] storeStruct(Map<FieldName, ?> result) {
        List<Object> resultStruct = Lists.newArrayList();

        /*
        List<TargetField> targetFields = evaluator.getTargetFields();
        for (TargetField targetField : targetFields) {
            resultStruct.add(EvaluatorUtil.decode(result.get(targetField)));
        }
        */


        List<OutputField> outputFields = evaluator.getOutputFields();
        for (OutputField outputField : outputFields) {
            resultStruct.add(result.get(outputField.getName()));
        }

        return resultStruct.toArray(new Object[resultStruct.size()]);
    }


    private Map<FieldName, FieldValue> loadArguments(ObjectInspector[] inspectors, GenericUDF.DeferredObject[] objects) throws HiveException {

        if (inspectors.length == 1) {
            ObjectInspector inspector = inspectors[0];

            ObjectInspector.Category category = inspector.getCategory();
            switch (category) {
                case STRUCT:
                    return loadStruct(inspectors[0], objects[0]);
                default:
                    return loadPrimitiveList(inspectors, objects);
            }
        }

        return loadPrimitiveList(inspectors, objects);
    }

    private Map<FieldName, FieldValue> loadPrimitiveList(ObjectInspector[] inspectors, GenericUDF.DeferredObject[] objects) throws HiveException {
        Map<FieldName, FieldValue> result = Maps.newLinkedHashMap();

        int i = 0;

        List<InputField> inputs = evaluator.getActiveFields();

        for (InputField input : inputs) {


            Object primitiveObject = Double.parseDouble(objects[i].get().toString());
            LOG.info("meulog ******************* tentando obj type " + objects[i].get().getClass());
            LOG.info("meulog ******************* tentando obj");
            LOG.info(primitiveObject.toString());
            LOG.info(input.getField().getName().getValue());
            FieldValue inputFieldValue = input.prepare(primitiveObject);


            //ieldValue value = EvaluatorUtil.prepare(evaluator, activeField, primitiveInspector.getPrimitiveJavaObject(primitiveObject));

            result.put(input.getName(), inputFieldValue);

            i++;
        }

        return result;
    }

    private Map<FieldName, FieldValue> loadStruct(ObjectInspector inspector, GenericUDF.DeferredObject object) throws HiveException {
        Map<FieldName, FieldValue> result = Maps.newLinkedHashMap();

        StructObjectInspector structInspector = (StructObjectInspector) inspector;

        Object structObject = object.get();

        List<InputField> activeFields = evaluator.getActiveFields();
        for (InputField activeField : activeFields) {
            StructField structField = structInspector.getStructFieldRef(activeField.getName().getValue());

            PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) structField.getFieldObjectInspector();

            Object primitiveObject = structInspector.getStructFieldData(structObject, structField);

            FieldValue inputFieldValue = activeField.prepare(primitiveObject);

            result.put(activeField.getName(), inputFieldValue);
        }

        return result;
    }


}
