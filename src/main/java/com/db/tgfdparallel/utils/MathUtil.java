package com.db.tgfdparallel.utils;

public class MathUtil {
    public static int computeCombinations(int lowerBound, int upperBound) {
        int n = upperBound - lowerBound + 1;
        int k = 2;

        return factorial(n) / (factorial(k) * factorial(n - k));
    }

    private static int factorial(int num) {
        int result = 1;
        for (int i = 1; i <= num; i++) {
            result *= i;
        }
        return result;
    }
}
