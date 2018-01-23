package com.poc.tree.feature;

import java.util.List;

import com.poc.tree.data.DataSample;

public interface Feature {
    
    /**
     * Calculates and checks if data contains feature.
     * 
     * @param dataSample Data sample.
     * @return true if data has this feature and false otherwise.
     */
    boolean belongsTo(DataSample dataSample);

    /**
     * Split data according to if it has this feature.
     * 
     * @param data Data to by split by this feature.
     * @return Sublists of split data samples.
     */
    List<List<DataSample>> split(List<DataSample> data);

}
