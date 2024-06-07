package com.db.tgfdparallel.service;

import com.db.tgfdparallel.domain.AttributeDependency;
import com.db.tgfdparallel.domain.Delta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DependencyService {
    private static final Logger logger = LoggerFactory.getLogger(DependencyService.class);

    public boolean isSuperSetOfPath(AttributeDependency dependency, List<AttributeDependency> prunedDependencies) {
        for (AttributeDependency prunedPath : prunedDependencies) {
            if (dependency.getRhs().equals(prunedPath.getRhs()) && dependency.getLhs().containsAll(prunedPath.getLhs())) {
//                logger.info("Candidate path {} is a superset of pruned path {}", dependency, prunedPath);
                return true;
            }
        }
        return false;
    }

    public boolean isSuperSetOfPathAndSubsetOfDelta(AttributeDependency dependency, List<AttributeDependency> minimalDependenciesOnThisPath) {
        for (AttributeDependency prunedPath : minimalDependenciesOnThisPath) {
            if (dependency.getRhs().equals(prunedPath.getRhs()) && dependency.getLhs().containsAll(prunedPath.getLhs())) {
//                logger.info("Candidate path {} is a superset of pruned path {}", dependency, prunedPath);
                if (subsetOf(dependency.getDelta(), prunedPath.getDelta())) {
//                    logger.info("Candidate path delta {}\n with pruned path delta {}.", dependency.getDelta(), prunedPath.getDelta());
                    return true;
                }
            }
        }
        return false;
    }

    private boolean subsetOf(Delta delta, Delta prunedPathDelta) {
        return delta.getMin().getYears() >= prunedPathDelta.getMin().getYears() && delta.getMax().getYears() <= prunedPathDelta.getMax().getYears();
    }

//    private boolean subsetOf(Delta delta, Delta prunedPathDelta) {
//        // Extracting years and months for the minimum period comparison
//        int deltaMinYears = delta.getMin().getYears();
//        int deltaMinMonths = delta.getMin().getMonths();
//        int prunedDeltaMinYears = prunedPathDelta.getMin().getYears();
//        int prunedDeltaMinMonths = prunedPathDelta.getMin().getMonths();
//
//        // Extracting years and months for the maximum period comparison
//        int deltaMaxYears = delta.getMax().getYears();
//        int deltaMaxMonths = delta.getMax().getMonths();
//        int prunedDeltaMaxYears = prunedPathDelta.getMax().getYears();
//        int prunedDeltaMaxMonths = prunedPathDelta.getMax().getMonths();
//
//        // Combine years and months into total months for easier comparison
//        int deltaMinTotalMonths = deltaMinYears * 12 + deltaMinMonths;
//        int prunedDeltaMinTotalMonths = prunedDeltaMinYears * 12 + prunedDeltaMinMonths;
//        int deltaMaxTotalMonths = deltaMaxYears * 12 + deltaMaxMonths;
//        int prunedDeltaMaxTotalMonths = prunedDeltaMaxYears * 12 + prunedDeltaMaxMonths;
//
//        // Compare the total months for min and max
//        return deltaMinTotalMonths >= prunedDeltaMinTotalMonths && deltaMaxTotalMonths <= prunedDeltaMaxTotalMonths;
//    }

}
