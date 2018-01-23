package com.poc.tree;

import static com.poc.tree.feature.PredicateFeature.newFeature;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

import com.google.common.collect.Lists;
import com.poc.tree.data.DataSample;
import com.poc.tree.data.SimpleDataSample;
import com.poc.tree.feature.Feature;
import com.poc.tree.feature.P;
import com.poc.tree.impurity.GiniIndexImpurityCalculation;
import com.poc.tree.impurity.ImpurityCalculationMethod;
import com.poc.tree.label.BooleanLabel;
import com.poc.tree.label.Label;

/**
 * Decision tree implementation.
 *
 */
public class DecisionTreeMain {

    private Logger log = LoggerFactory.getLogger(DecisionTreeMain.class);
    private Node root;
    private ImpurityCalculationMethod impurityCalculationMethod = new GiniIndexImpurityCalculation();
    private double homogenityPercentage = 0.90;

    /**
     * Max depth parameter. Growth of the tree is stopped once this depth is reached. Limiting depth of the tree can
     * help with over-fitting, however if depth will be set too low tree will not be accurate.
     */
    private int maxDepth = 15;

    public Node getRoot() {
        return root;
    }

    /**
     * Trains tree on training data for provided features.
     */
    public void train(List<DataSample> trainingData, List<Feature> features) {
        root = growTree(trainingData, features, 1);
    }

    /**
     * Grow tree during training by splitting data recursively on best feature.
     */
    protected Node growTree(List<DataSample> trainingData, List<Feature> features, int currentDepth) {

        Label currentNodeLabel = null;
        // if dataset already homogeneous make this node a leaf
        if ((currentNodeLabel = getLabel(trainingData)) != null) {
            log.debug("New leaf is created because data is homogeneous: {}", currentNodeLabel.getName());
            return Node.newLeafNode(currentNodeLabel);
        }
        
        boolean stoppingCriteriaReached = features.isEmpty() || currentDepth >= maxDepth;
        if (stoppingCriteriaReached) {
            Label majorityLabel = getMajorityLabel(trainingData);
            log.debug("New leaf is created because stopping criteria reached: {}", majorityLabel.getName());
            return Node.newLeafNode(majorityLabel);
        }

        Feature bestSplit = findBestSplitFeature(trainingData, features); 
        log.debug("Best split found: {}", bestSplit.toString());
        List<List<DataSample>> splitData = bestSplit.split(trainingData);
        log.debug("Data is split into sublists of sizes: {}", splitData.stream().map(List::size).collect(Collectors.toList()));

        List<Feature> newFeatures = features.stream().filter(p -> !p.equals(bestSplit)).collect(toList());
        Node node = Node.newNode(bestSplit);
        for (List<DataSample> subsetTrainingData : splitData) { // add children to current node according to split
            if (subsetTrainingData.isEmpty()) {
                // if subset data is empty add a leaf with label calculated from initial data
                node.addChild(Node.newLeafNode(getMajorityLabel(trainingData)));
            } else {
                // grow tree further recursively
                node.addChild(growTree(subsetTrainingData, newFeatures, currentDepth + 1));
            }
        }

        return node;
    }

    /**
     * Classify dataSample.
     */
    public Label classify(DataSample dataSample) {
        Node node = root;
        while (!node.isLeaf()) { // go through tree until leaf is reached
            // only binary splits for now - has feature first child node(left branch), does not have feature second child node(right branch).
            if (dataSample.has(node.getFeature())) {
                node = node.getChildren().get(0); 
            } else {
                node = node.getChildren().get(1);
            }
        }
        return node.getLabel();
    }

    /**
     * Finds best feature to split on which is the one whose split results in lowest impurity measure.
     */
    protected Feature findBestSplitFeature(List<DataSample> data, List<Feature> features) {
        double currentImpurity = 1;
        Feature bestSplitFeature = null; // rename split to feature

        for (Feature feature : features) {
            List<List<DataSample>> splitData = feature.split(data);
            // splitImpurity is average of leaf impurities
            double calculatedSplitImpurity = splitData.parallelStream().filter(list -> !list.isEmpty()).mapToDouble(list -> impurityCalculationMethod.calculateImpurity(list)).average().getAsDouble();
            if (calculatedSplitImpurity < currentImpurity) {
                currentImpurity = calculatedSplitImpurity;
                bestSplitFeature = feature;
            }
        }

        return bestSplitFeature;
    }

    /**
     * Returns Label if data is homogeneous.
     */
    protected Label getLabel(List<DataSample> data) {
        // group by to map <Label, count>
        Map<Label, Long> labelCount = data.parallelStream().collect(groupingBy(DataSample::getLabel, counting()));
        long totalCount = data.size();
        for (Label label : labelCount.keySet()) {
            long nbOfLabels = labelCount.get(label);
            if (((double) nbOfLabels / (double) totalCount) >= homogenityPercentage) {
                return label;
            }
        }
        return null;
    }

    /**
     * Differs from getLabel() that it always return some label and does not look at homogenityPercentage parameter. It
     * is used when tree growth is stopped and everything what is left must be classified so it returns majority label for the data.
     */
    protected Label getMajorityLabel(List<DataSample> data) {
        // group by to map <Label, count> like in getLabels() but return Label with most counts
        return data.parallelStream().collect(groupingBy(DataSample::getLabel, counting())).entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
    }
    
    private static List<DataSample> readData(boolean training) throws IOException {
        List<DataSample> data = Lists.newArrayList();
        String filename = training ? "train.csv" : "test.csv";
        InputStreamReader stream = new InputStreamReader(DecisionTreeMain.class.getResourceAsStream(filename));
        try (ICsvListReader listReader = new CsvListReader(stream, CsvPreference.STANDARD_PREFERENCE);) {
            
            // the header elements are used to map the values to the bean (names must match)
            final String[] header = listReader.getHeader(true);
            
            List<Object> values;
            while ((values = listReader.read(getProcessors(training))) != null) {
                data.add(SimpleDataSample.newSimpleDataSample("Survived", header, values.toArray()));
            }
        }
        return data;
    }

    // -------------------------------- TREE PRINTING ------------------------------------

    public void printTree() {
        printSubtree(root);
    }

    public void printSubtree(Node node) {
        if (!node.getChildren().isEmpty() && node.getChildren().get(0) != null) {
            printTree(node.getChildren().get(0), true, "");
        }
        printNodeValue(node);
        if (node.getChildren().size() > 1 && node.getChildren().get(1) != null) {
            printTree(node.getChildren().get(1), false, "");
        }
    }

    private void printNodeValue(Node node) {
        if (node.isLeaf()) {
            System.out.print(node.getLabel());
        } else {
            System.out.print(node.getName());
        }
        System.out.println();
    }

    private void printTree(Node node, boolean isRight, String indent) {
        if (!node.getChildren().isEmpty() && node.getChildren().get(0) != null) {
            printTree(node.getChildren().get(0), true, indent + (isRight ? "        " : " |      "));
        }
        System.out.print(indent);
        if (isRight) {
            System.out.print(" /");
        } else {
            System.out.print(" \\");
        }
        System.out.print("----- ");
        printNodeValue(node);
        if (node.getChildren().size() > 1 && node.getChildren().get(1) != null) {
            printTree(node.getChildren().get(1), false, indent + (isRight ? " |      " : "        "));
        }
    }
    
    private static List<Feature> getFeatures() {
        Feature firstClassPassenger = newFeature("Pclass", 1);
        Feature secondClassPassenger = newFeature("Pclass", 2);
        Feature isMale = newFeature("Sex", "male");
        Feature isFemale = newFeature("Sex", "female");
        Feature ageLessThan10 = newFeature("Age", P.lessThanD(10.0), "less than 10");
        Feature ageBewteen10And30 = newFeature("Age", P.betweenD(10.0, 30.0), "between 10 and 30");
        Feature ageBewteen30And50 = newFeature("Age", P.betweenD(30.0, 50.0), "between 30 and 50");
        Feature ageMoreThan60 = newFeature("Age", P.moreThanD(60.0), "more than 60");
        Feature hasSiblings = newFeature("SibSp", P.moreThan(0), "more than 0");
        Feature moreThan2Siblings = newFeature("SibSp", P.moreThan(2), "more than 2");
        Feature hasParentsChildren = newFeature("Parch", P.moreThan(0), "more than 0");
        Feature moreThan2Children = newFeature("Parch", P.moreThan(2), "more than 2");
        Feature fareMoreThan7 = newFeature("Fare", P.lessThanD(7.89), "less than 7.89");
        Feature fareBetween7And15 = newFeature("Fare", P.betweenD(7.89, 15.78), "between 7.89 and 15.78");
        Feature fareBetween15And23 = newFeature("Fare", P.betweenD(15.78, 23.67), "between 15.78 and 23.67");
        Feature fareMoreThan71 = newFeature("Fare", P.moreThanD(71.01), "more than 71.01");
        Feature cabinA = newFeature("Cabin", P.startsWith("A"), "starts with A");
        Feature cabinB = newFeature("Cabin", P.startsWith("B"), "starts with B");
        Feature cabinC = newFeature("Cabin", P.startsWith("C"), "starts with C");
        Feature embarkedC = newFeature("Embarked", "C");
        Feature embarkedS = newFeature("Embarked", "S");
        Feature embarkedQ = newFeature("Embarked", "Q");
        
        return Arrays.asList(firstClassPassenger, secondClassPassenger, isMale, isFemale, hasSiblings, moreThan2Siblings,
                hasParentsChildren, moreThan2Children, ageLessThan10, ageBewteen10And30, ageBewteen30And50, ageMoreThan60,
                fareMoreThan7, fareBetween7And15, fareBetween15And23, fareMoreThan71, fareMoreThan71, cabinA, cabinB, cabinC,
                embarkedC, embarkedS, embarkedQ);
    }
    
    private static CellProcessor[] getProcessors(boolean training) {
        if (training) {
            final CellProcessor[] processors = new CellProcessor[] { 
                    new Optional(new ParseInt()),
                    new Optional(new ParseBooleanLabel()),
                    new Optional(new ParseInt()),
                    new Optional(),
                    new Optional(),
                    new Optional(new ParseDouble()),
                    new Optional(new ParseInt()),
                    new Optional(new ParseInt()),
                    new Optional(),
                    new Optional(new ParseDouble()),
                    new Optional(),
                    new Optional()
            };
            return processors;
        } else {
            final CellProcessor[] processors = new CellProcessor[] { 
                    new Optional(new ParseInt()),
                    new Optional(new ParseInt()),
                    new Optional(),
                    new Optional(),
                    new Optional(new ParseDouble()),
                    new Optional(new ParseInt()),
                    new Optional(new ParseInt()),
                    new Optional(),
                    new Optional(new ParseDouble()),
                    new Optional(),
                    new Optional()
            };
            return processors;
        }
    }

    private static class ParseBooleanLabel extends ParseBool {
        
        public Object execute(final Object value, final CsvContext context) {
            Boolean parsed = (Boolean)super.execute(value, context);
            return parsed ? BooleanLabel.TRUE_LABEL : BooleanLabel.FALSE_LABEL;
        }
        
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException {
        List<DataSample> trainingData = readData(true);
        DecisionTreeMain tree = new DecisionTreeMain();
         
        List<Feature> features = getFeatures();
         
        tree.train(trainingData, features);
         
        tree.printTree();
         
        List<DataSample> testingData = readData(false);
         
        for (DataSample dataSample : testingData) {
           tree.classify(dataSample);
        }
         
    }
}
