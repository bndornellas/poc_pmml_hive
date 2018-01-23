package com.poc.tree.utils;

import org.apache.commons.math3.util.FastMath;

public class MathUtils {

    public static double log2(double x) {
        return FastMath.log(x) / FastMath.log(2);
    }
}
