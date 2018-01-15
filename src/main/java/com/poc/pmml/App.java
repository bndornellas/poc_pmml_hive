package com.poc.pmml;

import org.dmg.pmml.FieldName;

import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.FileInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class App {

    private static class TesteResource {

        public InputStream Retorna() {
            return getClass().getResourceAsStream("LogisticRegressionPOC.pmml");
        }
    }

    public static void main(String[] args) throws IOException, JAXBException, SAXException {


        PMML pmml;
        InputStream is = new FileInputStream("./DecisionTreePoc.pmml");
        pmml = org.jpmml.model.PMMLUtil.unmarshal(is);

        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        ModelEvaluator<?> modelEvaluator = modelEvaluatorFactory.newModelEvaluator(pmml);

        Evaluator evaluator = (Evaluator)modelEvaluator;

        System.out.println("target");
        List<TargetField> targetFields = evaluator.getTargetFields();
        for(TargetField f : targetFields){
            System.out.println(f.getName().getValue());
            System.out.println(f.getDataType().value());
        }

        System.out.println("out");
        List<OutputField> _out = evaluator.getOutputFields();
        for(OutputField f : _out){
            System.out.println(f.getName().getValue());
            System.out.println(f.getOutputField().getDataType());
        }


        Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();

        Double[] arr = new Double[]{2d,3d,5d};
        int cont = 0;

        List<InputField> inputFields = evaluator.getActiveFields();
        for(InputField inputField : inputFields){
            FieldName inputFieldName = inputField.getName();
            System.out.println(inputFieldName.getValue());
            // The raw (ie. user-supplied) value could be any Java primitive value
            Object rawValue = arr[cont];
            System.out.println(rawValue);


            // The raw value is passed through: 1) outlier treatment, 2) missing value treatment, 3) invalid value treatment and 4) type conversion
            FieldValue inputFieldValue = inputField.prepare(rawValue);

            arguments.put(inputFieldName, inputFieldValue);
            cont++;
        }

        Map<FieldName, ?> evaluate = evaluator.evaluate(arguments);


        Collection<?> values = evaluate.values();
        for (Object o:
             values) {
            System.out.println(o);

        }



        System.out.println(modelEvaluator);


    }
}
