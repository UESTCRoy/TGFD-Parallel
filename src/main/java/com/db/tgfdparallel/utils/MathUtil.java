package com.db.tgfdparallel.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    public static <T> Set<List<T>> cartesianProduct(List<Set<T>> sets) {
        Set<List<T>> result = new HashSet<>();
        cartesianProduct(0, new ArrayList<>(), sets, result);
        return result;
    }

    private static  <T> void cartesianProduct(int index, List<T> current, List<Set<T>> sets, Set<List<T>> result) {
        if (index == sets.size()) {
            result.add(new ArrayList<>(current));
            return;
        }

        Set<T> currentSet = sets.get(index);
        for (T element : currentSet) {
            current.add(element);
            cartesianProduct(index + 1, current, sets, result);
            current.remove(current.size() - 1);
        }
    }

    public static void main(String[] args) {
        System.out.println(computeCombinations(1, 6));
    }
}
