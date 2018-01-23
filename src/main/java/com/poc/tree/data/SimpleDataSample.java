package com.poc.tree.data;

import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.poc.tree.label.Label;

public class SimpleDataSample implements DataSample {
    
    private Map<String, Object> values = Maps.newHashMap();
    
    /** Column name which contains data labels. */
    private String labelColumn;
    
    private SimpleDataSample(String labelColumn, String[] header, Object... dataValues) {
        super();
        this.labelColumn = labelColumn;
        for (int i = 0; i < header.length; i++) {
            this.values.put(header[i], dataValues[i]);
        }
    }

    @Override
    public Optional<Object> getValue(String column) {
        return Optional.ofNullable(values.get(column));
    }
    
    @Override
    public Label getLabel() {
        return (Label)values.get(labelColumn);
    }

    /**
     * Create data sample without labels which is used on trained tree.
     */
    public static SimpleDataSample newClassificationDataSample(String[] header, Object... values) {
        Preconditions.checkArgument(header.length == values.length);
        return new SimpleDataSample(null, header, values);
    }

    /**
     * @param labelColumn
     * @param header
     * @param values
     * @return
     */
    public static SimpleDataSample newSimpleDataSample(String labelColumn, String[] header, Object... values) {
        Preconditions.checkArgument(header.length == values.length);
        return new SimpleDataSample(labelColumn, header, values);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "SimpleDataSample [values=" + values + "]";
    }
    
}
