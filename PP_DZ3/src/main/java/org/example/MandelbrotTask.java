import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class MandelbrotTask {

    //параметры сетки
    private static final int WIDTH = 800;
    private static final int HEIGHT = 600;
    private static final int MAX_ITERATIONS = 1000;

    //область плоскости
    private static final double X_MIN = -2.0;
    private static final double X_MAX = 1.0;
    private static final double Y_MIN = -1.0;
    private static final double Y_MAX = 1.0;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        //двумерный массив
        int[][] pixels = new int[HEIGHT][WIDTH];

        //определяем количество потоков
        int processors = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(processors);

        System.out.println("Запуск расчета множества Мандельброта в " + processors + " потоков...");
        long startTime = System.currentTimeMillis();

        List<Future<?>> futures = new ArrayList<>();

        int rowsPerTask = HEIGHT / processors;
        for (int i = 0; i < processors; i++) {
            final int startRow = i * rowsPerTask;
            final int endRow = (i == processors - 1) ? HEIGHT : (i + 1) * rowsPerTask;

            futures.add(pool.submit(() -> {
                for (int y = startRow; y < endRow; y++) {
                    for (int x = 0; x < WIDTH; x++) {
                        double c_re = X_MIN + (x * (X_MAX - X_MIN) / (WIDTH - 1));
                        double c_im = Y_MIN + (y * (Y_MAX - Y_MIN) / (HEIGHT - 1));

                        //расчет точки
                        pixels[y][x] = calculatePixel(c_re, c_im);
                    }
                }
            }));
        }

        //ожидание завершения всех потоков
        for (Future<?> future : futures) {
            future.get();
        }

        pool.shutdown();
        long endTime = System.currentTimeMillis();

        System.out.println("Расчет успешно завершен за " + (endTime - startTime) + " мс.");
    }

    //расчет количества итераций
    private static int calculatePixel(double c_re, double c_im) {
        double z_re = 0.0;
        double z_im = 0.0;
        int iteration = 0;

        while (z_re * z_re + z_im * z_im <= 4.0 && iteration < MAX_ITERATIONS) {
            double z_re_new = z_re * z_re - z_im * z_im + c_re;
            z_im = 2.0 * z_re * z_im + c_im;
            z_re = z_re_new;
            iteration++;
        }

        return iteration;
    }
}