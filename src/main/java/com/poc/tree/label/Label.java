package com.poc.tree.label;

public abstract class Label {
    
    /**
     * Label value used to print to predictions output.
     * 
     * @return Print label
     */
    public abstract String getPrintValue();
    
    /**
     * @return Label name
     */
    public abstract String getName();
    
    /**
     * Force overriding equals.
     */
    public abstract boolean equals(final Object o);

    /**
     * Force overriding hashCode.
     */
    public abstract int hashCode();

}
