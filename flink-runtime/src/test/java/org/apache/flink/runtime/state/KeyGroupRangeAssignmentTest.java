package org.apache.flink.runtime.state;

import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;
import org.apache.flink.shaded.guava30.com.google.common.hash.Hashing;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex;

public class KeyGroupRangeAssignmentTest {

    @Test
    void test() {

        final int maxParallelism = (int) Math.pow(2, 15);
        final int oldParallelism = 12;
        final int newParallelism = 14;

        System.out.println("Max parallelism: " + maxParallelism);

        for (int idx = 0; idx < oldParallelism; idx++) {
            final KeyGroupRange oldRange = computeKeyGroupRangeForOperatorIndex(
                    maxParallelism,
                    oldParallelism,
                    idx);
            final KeyGroupRange newRange = computeKeyGroupRangeForOperatorIndex(
                    maxParallelism,
                    newParallelism,
                    idx);
            final KeyGroupRange intersection = oldRange.getIntersection(newRange);
            final boolean intersect = intersection.getStartKeyGroup() < intersection.getEndKeyGroup();
            System.out.printf(
                    "Old: %s, New: %s, Intersection: %s%n",
                    oldRange,
                    newRange,
                    intersection);
        }
    }

    @Test
    void testConsistentHashing() {
        final int maxParallelism = (int) Math.pow(2, 15);
        final int oldParallelism = 6;
        final int newParallelism = 32;

        final Map<Integer, Set<Integer>> oldAssignment = calculateAssignment(maxParallelism, oldParallelism);
        final Map<Integer, Set<Integer>> newAssignment = calculateAssignment(maxParallelism, newParallelism);

        for (int subtaskIdx = 0; subtaskIdx < Math.max(oldParallelism, newParallelism); subtaskIdx++) {
            final Set<Integer> oldSubtask = oldAssignment.getOrDefault(subtaskIdx, new HashSet<>());
            final Set<Integer> newSubtask = newAssignment.getOrDefault(subtaskIdx, new HashSet<>());
            final Sets.SetView<Integer> intersection = Sets.intersection(oldSubtask, newSubtask);
            System.out.printf("Idx: %d, Old: %d, New: %d, Intersection: %d%n", subtaskIdx, oldSubtask.size(), newSubtask.size(), intersection.size());
            if (intersection.size() == newSubtask.size()) {
                final Sets.SetView<Integer> removedGroups = Sets.difference(oldSubtask, intersection);
                System.out.printf("---> Removing %d key groups.%n", removedGroups.size());
            } else {
                final Sets.SetView<Integer> addedGroups = Sets.difference(newSubtask, intersection);
                final int[] counts = new int[Math.max(oldParallelism, newParallelism)];
                Arrays.fill(counts, 0);
                oldAssignment.forEach((k, groups) -> {
                    for (int group : groups) {
                        if (addedGroups.contains(group)) {
                            counts[k]++;
                        }
                    }
                });
                int fromSubtasksCount = 0;
                for (int count : counts) {
                    if (count > 0) {
                        fromSubtasksCount++;
                    }
                }
                System.out.printf("---> Adding %d key groups from %d subtasks.%n", addedGroups.size(), fromSubtasksCount);
            }
            System.out.println("=========================================");
        }
    }

    private static Map<Integer, Set<Integer>> calculateAssignment(int maxParallelism, int parallelism) {
        final Map<Integer, Set<Integer>> assignment = new HashMap<>();
        for (int keyGroup = 0; keyGroup < maxParallelism; keyGroup++) {
            final int subtask = Hashing.consistentHash(keyGroup, parallelism);
            assignment.computeIfAbsent(subtask, ignored -> new HashSet<>()).add(keyGroup);
        }
        return assignment;
    }
}
