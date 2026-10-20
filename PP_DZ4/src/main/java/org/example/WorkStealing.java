package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class WorkStealing {

    enum TaskDistribution {
        UNIFORM,
        PERIODIC,
        PARETO
    }

    interface Shutdownable {
        void shutdown();
    }

    public interface ShutdownableExecutor extends Shutdownable, java.util.concurrent.Executor {}

    static class FixedThreadPoolExecutor implements ShutdownableExecutor {
        private final ExecutorService executor;

        public FixedThreadPoolExecutor(int threads) {
            this.executor = Executors.newFixedThreadPool(threads);
        }

        @Override
        public void execute(Runnable command) {
            executor.execute(command);
        }

        @Override
        public void shutdown() {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    static class ThreadPerTaskExecutor implements ShutdownableExecutor {
        List<Thread> threads = new ArrayList<>();

        @Override
        public void execute(Runnable command) {
            var t = new Thread(command);
            threads.add(t);
            t.start();
        }

        public void shutdown() {
            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    static class RoundRobinExecutor implements ShutdownableExecutor {

        protected static final Runnable EXIT_TASK = () -> {};

        AtomicInteger counter = new AtomicInteger(0);

        List<Thread> threads;
        Vector<BlockingDeque<Runnable>> tasks;
        volatile boolean isShuttingDown = false;

        RoundRobinExecutor(int threads) {
            Supplier<IntStream> stream = () -> IntStream.iterate(0, x -> x < threads, x -> x + 1);
            tasks = new Vector<>(
                    stream.get()
                            .mapToObj(x -> new LinkedBlockingDeque<Runnable>())
                            .toList());
            this.threads = stream.get()
                    .mapToObj(this::spawnThread)
                    .toList();
            this.threads.forEach(Thread::start);
        }

        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task;
                    try {
                        task = tasks.get(id).takeFirst();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (task == EXIT_TASK)
                        return;
                    task.run();
                }
            });
        }

        @Override
        public void shutdown() {
            synchronized (this) {
                if (!isShuttingDown) {
                    isShuttingDown = true;
                    tasks.forEach(queue -> queue.addLast(EXIT_TASK));
                }
            }
            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (isShuttingDown)
                throw new RejectedExecutionException("Executor is shutting down");
            tasks.get(counter.addAndGet(1) % threads.size()).addLast(command);
        }
    }

    static class WorkStealingExecutor extends RoundRobinExecutor {

        WorkStealingExecutor(int threads) {
            super(threads);
        }

        @Override
        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task = tasks.get(id).pollFirst();
                    if (task == null)
                        task = stealTask(id);

                    if (task == null) {
                        try {
                            task = tasks.get(id).pollFirst(1, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (task == null)
                            continue;
                    }

                    if (task == EXIT_TASK) {
                        task = stealTask(id);
                        if (task == null)
                            return;
                        tasks.get(id).addLast(EXIT_TASK);
                    }

                    task.run();
                }
            });
        }

        private Runnable stealTask(int thiefId) {
            for (int offset = 1; offset < tasks.size(); offset++) {
                var queue = tasks.get((thiefId + offset) % tasks.size());
                var task = queue.pollLast();

                if (task == EXIT_TASK) {
                    task = queue.pollLast();
                    queue.addLast(EXIT_TASK);
                }

                if (task != null)
                    return task;
            }
            return null;
        }
    }

    static final int THREAD_NUMBER = 10;
    static final int TASK_NUMBER = 100_000;
    static final int TARGET_OPTIMAL_FULL_TIME = 10_000; // ms = 10 s

    static final int MEAN_TASK_TIME = (int) Math.round((TARGET_OPTIMAL_FULL_TIME + 0d) / TASK_NUMBER * THREAD_NUMBER);
    static final int LOWER_TASK_TIME_BOUND = 0;
    static final int HIGHER_TASK_TIME_BOUND = MEAN_TASK_TIME * 2 + 1;
    static final int ESTIMATED_OPTIMAL_TIME = MEAN_TASK_TIME * TASK_NUMBER / THREAD_NUMBER;
    static final long TARGET_TOTAL_TASK_TIME = (long) TARGET_OPTIMAL_FULL_TIME * THREAD_NUMBER;
    static final long RANDOM_SEED = 42;
    static final double PARETO_SHAPE = 1.5;
    static final int PARETO_RAW_MAX_TASK_TIME = MEAN_TASK_TIME * 100;

    private static volatile long dummySink = 0;

    public static void blackHole(long difficulty) {
        long sum = 0;
        long iterations = difficulty * 200_000L;
        for (long i = 0; i < iterations; i++) {
            sum += i ^ (i << 1);
        }
        dummySink = sum;
    }

    static Runnable createTask(int duration, boolean useBlackHole) {
        return () -> {
            if (useBlackHole) {
                blackHole(duration);
            } else {
                try {
                    Thread.sleep(duration);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    static int createTaskDuration(TaskDistribution distribution, int taskId, Random random) {
        return switch (distribution) {
            case UNIFORM -> random.nextInt(LOWER_TASK_TIME_BOUND, HIGHER_TASK_TIME_BOUND);
            case PERIODIC -> taskId % THREAD_NUMBER == 0
                    ? MEAN_TASK_TIME * THREAD_NUMBER
                    : 0;
            case PARETO -> {
                var scale = MEAN_TASK_TIME * (PARETO_SHAPE - 1) / PARETO_SHAPE;
                var duration = scale / Math.pow(1 - random.nextDouble(), 1 / PARETO_SHAPE);
                yield (int) Math.min(Math.round(duration), PARETO_RAW_MAX_TASK_TIME);
            }
        };
    }

    static int[] createTaskDurations(TaskDistribution distribution) {
        var random = new Random(RANDOM_SEED);
        var durations = new int[TASK_NUMBER];

        long totalDuration = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            durations[taskId] = createTaskDuration(distribution, taskId, random);
            totalDuration += durations[taskId];
        }

        double scale = (double) TARGET_TOTAL_TASK_TIME / totalDuration;
        double remainder = 0;
        long normalizedTotal = 0;
        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            double scaledDuration = durations[taskId] * scale + remainder;
            durations[taskId] = (int) scaledDuration;
            remainder = scaledDuration - durations[taskId];
            normalizedTotal += durations[taskId];
        }

        for (int taskId = 0; normalizedTotal < TARGET_TOTAL_TASK_TIME; taskId++) {
            durations[taskId % TASK_NUMBER]++;
            normalizedTotal++;
        }
        for (int taskId = 0; normalizedTotal > TARGET_TOTAL_TASK_TIME; taskId++) {
            int index = taskId % TASK_NUMBER;
            if (durations[index] > 0) {
                durations[index]--;
                normalizedTotal--;
            }
        }

        return durations;
    }

    static Vector<Runnable> createTasks(TaskDistribution distribution, boolean useBlackHole) {
        var durations = createTaskDurations(distribution);
        return new Vector<>(
                IntStream
                        .iterate(0, x -> x < TASK_NUMBER, x -> x + 1)
                        .mapToObj(x -> createTask(durations[x], useBlackHole))
                        .toList()
        );
    }

    public record Pair<A, B>(A first, B second) {}

    public static Pair<Long, Long> measureExecutor(ShutdownableExecutor executor, TaskDistribution distribution, boolean useBlackHole) {
        var tasks = createTasks(distribution, useBlackHole);
        var start = System.nanoTime();
        tasks.forEach(executor::execute);
        var submissionEnd = System.nanoTime();
        executor.shutdown();
        var finish = System.nanoTime();
        return new Pair<>(submissionEnd - start, finish - start);
    }

    public static void main(String[] args) {
        System.out.println("Target optimal time: " + TARGET_OPTIMAL_FULL_TIME);
        System.out.println("Estimated optimal time: " + ESTIMATED_OPTIMAL_TIME);

        System.out.println("\n=== 1. TESTING WITH Thread.sleep() ===");
        runBenchmarkSuite(false);

        System.out.println("\n=== 2. TESTING WITH blackHole() ===");
        runBenchmarkSuite(true);
    }

    private static void runBenchmarkSuite(boolean useBlackHole) {
        for (var distribution : TaskDistribution.values()) {
            System.out.println("\n--- Task distribution: " + distribution + " ---");

            var fixedThreadPoolResult = measureExecutor(new FixedThreadPoolExecutor(THREAD_NUMBER), distribution, useBlackHole);
            printResult("FixedThreadPool Executor", fixedThreadPoolResult);

            var roundRobinExecutorResult = measureExecutor(new RoundRobinExecutor(THREAD_NUMBER), distribution, useBlackHole);
            printResult("Round Robin Executor", roundRobinExecutorResult);

            var workStealingExecutorResult = measureExecutor(new WorkStealingExecutor(THREAD_NUMBER), distribution, useBlackHole);
            printResult("Work Stealing Executor", workStealingExecutorResult);
        }
    }

    private static void printResult(String name, Pair<Long, Long> result) {
        System.out.printf("%-26s | Submission: %8.2f ms | Execution: %8.2f ms\n",
                name, result.first / 1_000_000d, result.second / 1_000_000d);
    }
}

/*
Target optimal time: 10000
Estimated optimal time: 10000

=== 1. TESTING WITH Thread.sleep() ===

--- Task distribution: UNIFORM ---
FixedThreadPool Executor   | Submission:    11,79 ms | Execution: 16214,86 ms
Round Robin Executor       | Submission:    12,48 ms | Execution: 16323,39 ms
Work Stealing Executor     | Submission:     9,00 ms | Execution: 16241,55 ms

--- Task distribution: PERIODIC ---
FixedThreadPool Executor   | Submission:    10,36 ms | Execution: 10747,64 ms
Round Robin Executor       | Submission:   116,46 ms | Execution: 105781,49 ms
Work Stealing Executor     | Submission:    22,58 ms | Execution: 10640,98 ms

--- Task distribution: PARETO ---
FixedThreadPool Executor   | Submission:     2,74 ms | Execution: 14849,00 ms
Round Robin Executor       | Submission:     5,31 ms | Execution: 15768,60 ms
Work Stealing Executor     | Submission:     5,58 ms | Execution: 20112,29 ms

=== 2. TESTING WITH blackHole() ===

--- Task distribution: UNIFORM ---
FixedThreadPool Executor   | Submission:     3,35 ms | Execution:   910,28 ms
Round Robin Executor       | Submission:     6,88 ms | Execution:   915,42 ms
Work Stealing Executor     | Submission:     6,24 ms | Execution:   921,45 ms

--- Task distribution: PERIODIC ---
FixedThreadPool Executor   | Submission:     3,66 ms | Execution:   906,68 ms
Round Robin Executor       | Submission:   143,46 ms | Execution:  5290,51 ms
Work Stealing Executor     | Submission:   104,39 ms | Execution:   971,68 ms

--- Task distribution: PARETO ---
FixedThreadPool Executor   | Submission:     3,38 ms | Execution:   939,07 ms
Round Robin Executor       | Submission:     6,23 ms | Execution:   912,86 ms
Work Stealing Executor     | Submission:     6,70 ms | Execution:   928,64 ms
*/