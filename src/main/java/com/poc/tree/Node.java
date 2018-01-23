package com.poc.tree;

import java.util.List;

import com.google.common.collect.Lists;
import com.poc.tree.feature.Feature;
import com.poc.tree.label.Label;

public class Node {
    
    private static final String LEAF_NODE_NAME = "Leaf";

    /** Node's feature used to split it further. */
    private Feature feature;
    
    private Label label;
    
    private List<Node> children = Lists.newArrayList();
    
    private Node(Feature feature) {
        this.feature = feature;
    }

    private Node(Feature feature, Label label) {
        this.label = label;
        this.feature = feature;
    }

    public static Node newNode(Feature feature) {
        return new Node(feature);
    }

    public static Node newLeafNode(Label label) {
        return new Node(null, label);
    }
    
    public void addChild(Node child) {
        children.add(child);
    }
    
    public List<Node> getChildren() {
        return children;
    }
    
    public Label getLabel() {
        return label;
    }

    public boolean isLeaf() {
        return label != null;
    }

    public Feature getFeature() {
        return feature;
    }

    public String getName() {
        return feature != null ? feature.toString() : LEAF_NODE_NAME;
    }
    
}
