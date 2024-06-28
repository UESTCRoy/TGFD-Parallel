package com.db.tgfdparallel.utils;

import com.db.tgfdparallel.domain.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TGFDComparison {
    private static final Pattern tgfdPatternParallel = Pattern.compile("TGFD\\{dependency=DataDependency\\{x=\\[(.*?)\\], y=\\[(.*?)\\]\\}, delta=Pair\\(min=(\\d+), max=(\\d+)\\), tgfdSupport=([\\d\\.E-]+), level=(\\d+)\\}");
    private static final Pattern tgfdPatternSeq = Pattern.compile("TGFD\\{dependency=DataDependency\\{x=\\[(.*?)\\], y=\\[(.*?)\\]}, delta=Delta\\{min=(P\\d+[YMD]), max=(P\\d+[YMD])\\}, support=([\\d\\.E-]+)\\}");

//    private static final Pattern literalPatternParallel = Pattern.compile("ConstantLiteral\\(vertexType=(.*?), attrName=(.*?), attrValue=(.*?)\\)");
    private static final Pattern literalPattern = Pattern.compile("ConstantLiteral\\{vertexType='(.*?)', attrName='(.*?)', attrValue='(.*?)'\\}");

    public static void main(String[] args) throws IOException {
        String path1 = "/Users/roy/Desktop/TGFD/datasets/imdb/Constant-TGFD.txt";
        String path2 = "/Users/roy/Desktop/TGFD/datasets/imdb/experiment-tgfdsG1m_imdb-t16-k2-theta8.0E-5-gamma20-freqSet10-fast-changefileAll-2024.06.20.13.26.20.txt";

        Set<TGFD> tgfdSet1 = new HashSet<>();
        Set<TGFD> tgfdSet2 = new HashSet<>();

        // 读取第一个文件并填充集合, 1为Parallel
        AtomicInteger parallelCount = new AtomicInteger();
        Files.lines(Paths.get(path1)).forEach(line -> {
            TGFD tgfd = parseTGFDFromString(line, 1);
            tgfdSet1.add(tgfd);
            parallelCount.getAndIncrement();
        });
        System.out.println("Parallel TGFDs: " + parallelCount.get());

        // 读取第二个文件并填充集合, 2为Sequential
        AtomicInteger sequentialCount = new AtomicInteger();
        Files.lines(Paths.get(path2)).forEach(line -> {
            TGFD tgfd = parseTGFDFromString(line, 2);
            tgfdSet2.add(tgfd);
            sequentialCount.getAndIncrement();
        });
        System.out.println("Sequential TGFDs: " + sequentialCount.get());

        // 比较两个集合中的重叠数量
        int overlapCount = 0;
//        for (TGFD tgfd1 : tgfdSet1) {
//            if (tgfdSet2.contains(tgfd1)) {
//                overlapCount++;
//            }
//        }
        int nonOverlapCount = 0;
        for (TGFD tgfd2 : tgfdSet2) {
            if (tgfdSet1.contains(tgfd2)) {
                overlapCount++;
            } else {
                nonOverlapCount++;
//                System.out.println(tgfd2);
            }
        }

        System.out.println("Total overlapping TGFDs: " + overlapCount + " Total non-overlapping TGFDs: " + nonOverlapCount);
    }

    public static TGFD parseTGFDFromString(String tgfdString, int type) {
        Pattern pattern = type == 1 ? tgfdPatternParallel : tgfdPatternSeq;
        Matcher matcher = pattern.matcher(tgfdString);
        if (matcher.find()) {
            List<ConstantLiteral> x = parseLiterals(matcher.group(1));
            // 对x进行排序
            x.sort((o1, o2) -> {
                int vertexTypeComparison = o1.getVertexType().compareTo(o2.getVertexType());
                if (vertexTypeComparison != 0) {
                    return vertexTypeComparison;
                }

                int attrNameComparison = o1.getAttrName().compareTo(o2.getAttrName());
                if (attrNameComparison != 0) {
                    return attrNameComparison;
                }

                return o1.getAttrValue().compareTo(o2.getAttrValue());
            });
            List<ConstantLiteral> y = parseLiterals(matcher.group(2));
            int min = type == 1 ? Integer.parseInt(matcher.group(3)) : parsePeriod(matcher.group(3));
            int max = type == 1 ? Integer.parseInt(matcher.group(4)) : parsePeriod(matcher.group(4));
            double support = Double.parseDouble(matcher.group(type == 1 ? 5 : 5));
            int level = type == 1 ? Integer.parseInt(matcher.group(6)) : 0;

            Pair delta = new Pair(min, max);
            DataDependency dependency = new DataDependency();
            dependency.getX().addAll(x);
            dependency.getY().addAll(y);
            return new TGFD(delta, dependency, support, level);
        }
        return null;
    }

    private static List<ConstantLiteral> parseLiterals(String literalsString) {
        List<ConstantLiteral> literals = new ArrayList<>();
        Matcher matcher = literalPattern.matcher(literalsString);
        while (matcher.find()) {
            String vertexType = matcher.group(1).trim();
            String attrName = matcher.group(2).trim();
            String attrValue = matcher.group(3).trim();
            literals.add(new ConstantLiteral(vertexType, attrName, attrValue));
        }
        return literals;
    }

    private static int parsePeriod(String period) {
        // Assuming format like "P2Y" for 2 years
        return Integer.parseInt(period.substring(1, period.length() - 1));
    }
}
