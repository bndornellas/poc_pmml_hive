package com.poc.tree.feature;

import static java.util.stream.Collectors.partitioningBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import com.google.common.collect.Lists;
import com.poc.tree.data.DataSample;

public class PredicateFeature<T> implements Feature {
    
    /** Data column used by feature. */
    private String column; 

    /** Predicate used for splitting. */
    private Predicate<T> predicate;
    
    /** Feature Label used for visualization and testing the tree. */
    private String label;

    private PredicateFeature(String column, Predicate<T> predicate, String label) {
        super();
        this.column = column;
        this.predicate = predicate;
        this.label = label;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean belongsTo(DataSample dataSample) {
        Optional<Object> optionalValue = dataSample.getValue(column);
        return optionalValue.isPresent() ? predicate.test((T)optionalValue.get()) : false;
    }

    @Override
    public List<List<DataSample>> split(List<DataSample> data) {
        List<List<DataSample>> result = Lists.newArrayList();
        // 
        Map<Boolean, List<DataSample>> split = data.parallelStream().collect(partitioningBy(dataSample -> belongsTo(dataSample)));
        
        if (split.get(true).size() > 0) {
            result.add(split.get(true));
        } else {
            result.add(Lists.newArrayList());
        }
        if (split.get(false).size() > 0) {
            result.add(split.get(false));
        } else {
            result.add(Lists.newArrayList());
        }
        return result;
    }
    
    @Override
    public String toString() {
        return label;
    }
    
    /**
     * Default static factory method which creates a feature. Default feature splits data whose column value is equal provided feature value.
     */
    public static <T> Feature newFeature(String column, T featureValue) {
        return new PredicateFeature<T>(column, P.isEqual(featureValue), String.format("%s = %s", column, featureValue));
    }

    /**
     * Static factory method to create a new feature.
     */
    public static <T> Feature newFeature(String column, Predicate<T> predicate, String predicateString) {
        return new PredicateFeature<T>(column, predicate, String.format("%s %s", column, predicateString));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((column == null) ? 0 : column.hashCode());
        result = prime * result + ((label == null) ? 0 : label.hashCode());
        result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("rawtypes")
        PredicateFeature other = (PredicateFeature) obj;
        if (column == null) {
            if (other.column != null)
                return false;
        } else if (!column.equals(other.column))
            return false;
        if (label == null) {
            if (other.label != null)
                return false;
        } else if (!label.equals(other.label))
            return false;
        if (predicate == null) {
            if (other.predicate != null)
                return false;
        } else if (!predicate.equals(other.predicate))
            return false;
        return true;
    }
    
}
