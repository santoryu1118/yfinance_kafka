package org.santoryu.kafka.streams;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Deque;

public class ListPerformanceTest {

    private static long measureTime(Runnable operation) {
        long startTime = System.nanoTime();
        operation.run();
        return System.nanoTime() - startTime;
    }

    public static void main(String[] args) {
        int size = 50;
        int iterations = 1000; // Increase iterations for averaging

        // Initialize collections
        List<Integer> arrayList = new ArrayList<>(size);
        LinkedList<Integer> linkedList = new LinkedList<>();
        Deque<Integer> arrayDeque = new ArrayDeque<>(size);

        for (int i = 0; i < size; i++) {
            arrayList.add(i);
            linkedList.add(i);
            arrayDeque.addLast(i);
        }

        // Run benchmarks
        benchmark("ArrayList", arrayList, iterations);
        benchmark("LinkedList", linkedList, iterations);
        benchmark("ArrayDeque", arrayDeque, iterations);
    }

    private static <T> void benchmark(String type, T collection, int iterations) {
        long totalGetFirstTime = 0;
        long totalSubListTime = 0;
        long totalRemoveLastTime = 0;
        long totalAddFirstTime = 0;

        for (int i = 0; i < iterations; i++) {
            if (collection instanceof LinkedList) {
                LinkedList<Integer> tempList = new LinkedList<>((LinkedList<Integer>) collection);

                totalGetFirstTime += measureTime(tempList::getFirst);
                totalSubListTime += measureTime(() -> tempList.subList(0, 10));
                totalRemoveLastTime += measureTime(tempList::removeLast);
                totalAddFirstTime += measureTime(() -> tempList.addFirst(-1));
            } else if (collection instanceof Deque) {
                Deque<Integer> tempDeque = new ArrayDeque<>((Deque<Integer>) collection);

                totalGetFirstTime += measureTime(tempDeque::getFirst);
                totalSubListTime += measureTime(() -> new ArrayList<>(tempDeque).subList(0, 10));
                totalRemoveLastTime += measureTime(tempDeque::removeLast);
                totalAddFirstTime += measureTime(() -> tempDeque.addFirst(-1));
            } else if (collection instanceof List) {
                List<Integer> tempList = new ArrayList<>((List<Integer>) collection);

                totalGetFirstTime += measureTime(() -> tempList.get(0));
                totalSubListTime += measureTime(() -> tempList.subList(0, 10));
                totalRemoveLastTime += measureTime(() -> tempList.remove(tempList.size() - 1));
                totalAddFirstTime += measureTime(() -> tempList.add(0, -1));
            }
        }

        System.out.println(type + " getFirst(): " + (totalGetFirstTime / iterations) + " ns");
        System.out.println(type + " subList(0, 10): " + (totalSubListTime / iterations) + " ns");
        System.out.println(type + " removeLast(): " + (totalRemoveLastTime / iterations) + " ns");
        System.out.println(type + " addFirst(): " + (totalAddFirstTime / iterations) + " ns");
    }
}
