package com.poc.tree.data;

import java.util.Optional;

import com.poc.tree.feature.Feature;
import com.poc.tree.label.Label;

public interface DataSample {
    
    /**
     * Get sample data value from specified column.
     * 
     * @return Data value.
     */
    Optional<Object> getValue(String column);
    
    /**
     * Assigned label of training data.
     * 
     * @return Label.
     */
    Label getLabel();
    
    /**
     * Check if data has feature.
     * 
     * @param feature Feature.
     * 
     * @return True if data has feature and false otherwise.
     */
    default boolean has(Feature feature) {
        return feature.belongsTo(this);
    }
    
}
