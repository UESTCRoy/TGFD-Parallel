package com.db.tgfdparallel.utils;

import java.util.List;

public class MathUtil {
    public static long computeCombinations(int lowerBound, int upperBound) {
        int n = upperBound - lowerBound + 1;
        int k = 2;
        return combination(n, k);
    }

    private static long combination(int n, int k) {
        long result = 1;
        for (int i = 1; i <= k; i++) {
            result *= n - i + 1;
            result /= i;
        }
        return result;
    }

    public static int countPairs(List<Integer> list) {
        long positiveCount = list.stream()
                .filter(number -> number > 0)
                .count();
        return (int) (positiveCount * (positiveCount - 1) / 2);
    }

    public static void main(String[] args) {
        System.out.println(computeCombinations(1, 6));
    }
}
