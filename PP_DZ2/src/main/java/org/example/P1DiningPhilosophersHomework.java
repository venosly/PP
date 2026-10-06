import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class P1DiningPhilosophersHomework {

    private static final class Fork {
        private final int id;
        private final Lock lock = new ReentrantLock(true);

        private Fork(int id) {
            this.id = id;
        }
    }

    private static final class Table {
        private final Fork[] forks;
        private final AtomicIntegerArray forkUsers;
        private final AtomicBoolean conflictDetected = new AtomicBoolean();

        private Table(int philosopherCount) {
            forks = new Fork[philosopherCount];
            for (int id = 0; id < philosopherCount; id++) {
                forks[id] = new Fork(id);
            }
            forkUsers = new AtomicIntegerArray(philosopherCount);
        }

        void eat(int philosopherId) {
            Fork left = forks[philosopherId];
            Fork right = forks[(philosopherId + 1) % forks.length];

            //вилки
            Fork first = left.id < right.id ? left : right;
            Fork second = left.id < right.id ? right : left;

            first.lock.lock();
            try {
                second.lock.lock();
                try {
                    eatWithBothForks(left, right);
                } finally {
                    second.lock.unlock();
                }
            } finally {
                first.lock.unlock();
            }
        }

        private void eatWithBothForks(Fork left, Fork right) {
            int leftUsers = forkUsers.incrementAndGet(left.id);
            int rightUsers = forkUsers.incrementAndGet(right.id);
            if (leftUsers != 1 || rightUsers != 1) {
                conflictDetected.set(true);
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                forkUsers.decrementAndGet(right.id);
                forkUsers.decrementAndGet(left.id);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int philosopherCount = 5;
        int mealsPerPhilosopher = 100;
        Table table = new Table(philosopherCount);
        ExecutorService pool = Executors.newFixedThreadPool(philosopherCount);
        List<Future<Integer>> results = new ArrayList<>();

        try {
            for (int philosopher = 0; philosopher < philosopherCount; philosopher++) {
                int philosopherId = philosopher;
                results.add(pool.submit(() -> {
                    int meals = 0;
                    while (meals < mealsPerPhilosopher) {
                        table.eat(philosopherId);
                        meals++;
                        Thread.yield();
                    }
                    return meals;
                }));
            }

            for (int philosopher = 0; philosopher < philosopherCount; philosopher++) {
                int meals = results.get(philosopher).get(10, TimeUnit.SECONDS);
                if (meals != mealsPerPhilosopher) {
                    throw new AssertionError(
                            "Philosopher " + philosopher + " ate " + meals + " times");
                }
            }
        } catch (TimeoutException e) {
            throw new AssertionError("Possible deadlock: philosophers did not finish", e);
        } finally {
            pool.shutdownNow();
        }

        if (table.conflictDetected.get()) {
            throw new AssertionError("Two philosophers used the same fork");
        }

        System.out.println("OK: every philosopher ate " + mealsPerPhilosopher
                + " times, no deadlock detected");
    }
}
/*
 * Задачка: Допустим, существует полный порядок ресурсов в программе и ресурсы блокируются в соответствие с этим порядком, а освобождаются в обратном порядке.
 * 1) Можно ли в таком случае организовать взаимную блокировку?
 *    — нет. если все потоки захватывают ресурсы в одном и том же
 *    порядке (например, по возрастанию id), условие кругового ожидания
 *    становится невыполнимым, что гарантирует отсутствие взаимной блокировки.
 *
 * 2) Можно ли организовать взаимную блокировку, если отдавать ресурсы в том же порядке,
 *    в каком мы их берём?
 *    — порядок освобождения ресурсов никак не влияет на возникновение взаимной блокировки.
 *    поэтому если захват происходит по единому порядку, взаимной блокировки всё равно не будет,
 *    даже если освобождать их в том же порядке.
 */
