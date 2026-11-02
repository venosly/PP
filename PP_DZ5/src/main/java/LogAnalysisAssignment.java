import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {
        public PathStatistics merge(PathStatistics other) {
            return new PathStatistics(
                    this.requests + other.requests,
                    this.successful + other.successful,
                    this.errors + other.errors,
                    this.responseBytes + other.responseBytes,
                    this.totalDurationMillis + other.totalDurationMillis,
                    Math.max(this.maxDurationMillis, other.maxDurationMillis)
            );
        }
    }

    public record Report(
            Map<String, PathStatistics> pathStats,
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis,
            int processedFiles,
            int failedFiles,
            long processedLines,
            long malformedLines,
            long readErrors
    ) {}

    //разбор строки
    public static LogEntry parseLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Line cannot be null");
        }
        String[] fields = line.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException("Invalid line format (expected 5 fields): " + line);
        }
        try {
            String method = fields[0];
            String path = fields[1];
            int status = Integer.parseInt(fields[2]);
            long responseBytes = Long.parseLong(fields[3]);
            long durationMillis = Long.parseLong(fields[4]);
            return new LogEntry(method, path, status, responseBytes, durationMillis);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in line: " + line, e);
        }
    }

    //аккумулятор статистики
    public static final class StatisticsAccumulator {
        private final ConcurrentHashMap<String, PathStatistics> statsMap = new ConcurrentHashMap<>();

        private final AtomicLong totalRequests = new AtomicLong();
        private final AtomicLong successfulRequests = new AtomicLong();
        private final AtomicLong errorRequests = new AtomicLong();
        private final AtomicLong totalBytes = new AtomicLong();
        private final AtomicLong maxDuration = new AtomicLong();

        public void addEntry(LogEntry entry) {
            boolean isSuccess = entry.status() >= 200 && entry.status() < 400;

            statsMap.compute(entry.path(), (k, v) -> {
                if (v == null) {
                    return new PathStatistics(
                            1,
                            isSuccess ? 1 : 0,
                            isSuccess ? 0 : 1,
                            entry.responseBytes(),
                            entry.durationMillis(),
                            entry.durationMillis()
                    );
                } else {
                    return new PathStatistics(
                            v.requests() + 1,
                            v.successful() + (isSuccess ? 1 : 0),
                            v.errors() + (isSuccess ? 0 : 1),
                            v.responseBytes() + entry.responseBytes(),
                            v.totalDurationMillis() + entry.durationMillis(),
                            Math.max(v.maxDurationMillis(), entry.durationMillis())
                    );
                }
            });

            totalRequests.incrementAndGet();
            if (isSuccess) {
                successfulRequests.incrementAndGet();
            } else {
                errorRequests.incrementAndGet();
            }
            totalBytes.addAndGet(entry.responseBytes());

            long currentMax;
            do {
                currentMax = maxDuration.get();
                if (entry.durationMillis() <= currentMax) {
                    break;
                }
            } while (!maxDuration.compareAndSet(currentMax, entry.durationMillis()));
        }

        public Report toReport(int processedFiles, int failedFiles, long processedLines, long malformedLines, long readErrors) {
            return new Report(
                    Collections.unmodifiableMap(new HashMap<>(statsMap)),
                    totalRequests.get(),
                    successfulRequests.get(),
                    errorRequests.get(),
                    totalBytes.get(),
                    maxDuration.get(),
                    processedFiles,
                    failedFiles,
                    processedLines,
                    malformedLines,
                    readErrors
            );
        }
    }

    //последовательный отчет
    public static Report analyzeSequential(Path directory) throws IOException {
        StatisticsAccumulator accumulator = new StatisticsAccumulator();
        int processedFiles = 0;
        int failedFiles = 0;
        long processedLines = 0;
        long malformedLines = 0;
        long readErrors = 0;

        try (Stream<Path> paths = Files.list(directory)) {
            List<Path> fileList = paths.filter(Files::isRegularFile).toList();
            for (Path file : fileList) {
                try {
                    Iterator<String> iterator = LogFileLines.iterator(file);
                    while (iterator.hasNext()) {
                        try {
                            String line = iterator.next();
                            LogEntry entry = parseLine(line);
                            accumulator.addEntry(entry);
                            processedLines++;
                        } catch (IllegalArgumentException e) {
                            malformedLines++;
                        }
                    }
                    processedFiles++;
                } catch (Exception e) {
                    failedFiles++;
                    readErrors++;
                }
            }
        }
        return accumulator.toReport(processedFiles, failedFiles, processedLines, malformedLines, readErrors);
    }

    //параллельный анализ
    public static Report analyzeParallel(
            Path directory,
            int poolSize,
            int semaphoreLimit,
            AtomicBoolean stopFlag
    ) throws Exception {
        StatisticsAccumulator accumulator = new StatisticsAccumulator();

        AtomicInteger processedFiles = new AtomicInteger();
        AtomicInteger failedFiles = new AtomicInteger();
        AtomicLong processedLines = new AtomicLong();
        AtomicLong malformedLines = new AtomicLong();
        AtomicLong readErrors = new AtomicLong();

        AtomicInteger activeFiles = new AtomicInteger();
        AtomicInteger maxActiveFiles = new AtomicInteger();

        Semaphore semaphore = new Semaphore(semaphoreLimit);
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);

        List<Path> files;
        try (Stream<Path> pathStream = Files.list(directory)) {
            files = pathStream.filter(Files::isRegularFile).toList();
        }

        CountDownLatch latch = new CountDownLatch(files.size());

        for (Path file : files) {
            executor.submit(() -> {
                if (stopFlag != null && stopFlag.get()) {
                    latch.countDown();
                    return;
                }

                boolean acquiredSemaphore = false;
                try {
                    semaphore.acquire();
                    acquiredSemaphore = true;

                    int currentActive = activeFiles.incrementAndGet();
                    updateMaxActive(maxActiveFiles, currentActive);

                    if (stopFlag != null && stopFlag.get()) {
                        return;
                    }

                    Iterator<String> iterator = LogFileLines.iterator(file);
                    while (iterator.hasNext()) {
                        if (stopFlag != null && stopFlag.get()) {
                            break;
                        }
                        try {
                            String line = iterator.next();
                            LogEntry entry = parseLine(line);
                            accumulator.addEntry(entry);
                            processedLines.incrementAndGet();
                        } catch (IllegalArgumentException e) {
                            malformedLines.incrementAndGet();
                        }
                    }
                    processedFiles.incrementAndGet();

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failedFiles.incrementAndGet();
                } catch (Exception e) {
                    failedFiles.incrementAndGet();
                    readErrors.incrementAndGet();
                } finally {
                    activeFiles.decrementAndGet();
                    if (acquiredSemaphore) {
                        semaphore.release();
                    }
                    latch.countDown();
                }
            });
        }

        latch.await();

        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        System.out.println("Max simultaneous active files observed: " + maxActiveFiles.get());

        return accumulator.toReport(
                processedFiles.get(),
                failedFiles.get(),
                processedLines.get(),
                malformedLines.get(),
                readErrors.get()
        );
    }

    private static void updateMaxActive(AtomicInteger maxActiveFiles, int currentActive) {
        int currentMax;
        do {
            currentMax = maxActiveFiles.get();
            if (currentActive <= currentMax) {
                break;
            }
        } while (!maxActiveFiles.compareAndSet(currentMax, currentActive));
    }

    public static final class LogFileLines {
        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
                return new Iterator<>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException("Cannot read " + path, exception);
                        }
                    }

                    private void close() {
                        if (closed) return;
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException("Cannot close " + path, exception);
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) throw new NoSuchElementException();
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException("Cannot open " + path, exception);
            }
        }
    }

    public static final class GenerateLogs {
        private static final String[] METHODS = {"GET", "POST", "PUT", "DELETE"};
        private static final String[] PATHS = {"/", "/index.html", "/login", "/api/users", "/api/orders", "/api/products", "/search", "/health"};
        private static final int[] STATUSES = {200, 200, 200, 201, 204, 301, 400, 401, 404, 500};

        private GenerateLogs() {}

        public static void generate(Path outputDirectory, int fileCount, int linesPerFile) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException("Counts must be non-negative");
            }
            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve("access-%03d.log".formatted(fileId));
                try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method = METHODS[random.nextInt(METHODS.length)];
                        String path = PATHS[random.nextInt(PATHS.length)];
                        int status = STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes = 100L + random.nextInt(100_000);
                        long durationMillis = 1L + random.nextInt(500);

                        writer.write("%s %s %d %d %d%n".formatted(method, path, status, responseBytes, durationMillis));
                    }
                }
            }
        }
    }

    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError("Unexpected line: " + line);
                    }
                    Integer.parseInt(fields[2]);
                    Long.parseLong(fields[3]);
                    Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }

        if (files != 3 || lines != 21) {
            throw new AssertionError("Expected 3 files and 21 lines, got " + files + " files and " + lines + " lines");
        }
        System.out.println("OK: generated " + files + " files and read " + lines + " lines");
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(args.length > 1 ? args[1] : "logs");
            int fileCount = args.length > 2 ? Integer.parseInt(args[2]) : 20;
            int linesPerFile = args.length > 3 ? Integer.parseInt(args[3]) : 1_000;

            GenerateLogs.generate(directory, fileCount, linesPerFile);
            System.out.println("Generated " + fileCount + " files in " + directory);
            return;
        }

        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }

        Path dir = Path.of("logs");
        if (!Files.exists(dir)) {
            GenerateLogs.generate(dir, 5, 100);
        }

        System.out.println("Running sequential analysis...");
        Report seqReport = analyzeSequential(dir);
        System.out.println("Sequential total requests: " + seqReport.totalRequests());

        System.out.println("Running parallel analysis...");
        AtomicBoolean stopFlag = new AtomicBoolean(false);
        Report parReport = analyzeParallel(dir, 4, 2, stopFlag);
        System.out.println("Parallel total requests: " + parReport.totalRequests());

        boolean match = seqReport.totalRequests() == parReport.totalRequests() &&
                seqReport.successfulRequests() == parReport.successfulRequests() &&
                seqReport.errorRequests() == parReport.errorRequests();
        System.out.println("Results match sequential baseline: " + match);
    }
}
/*
 * ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
 * 1. каждая строка полного запуска обработана ровно один раз;
 * - потому что итератор читает файл строка за строкой сверху вниз,
 * без перескоков и повторов.
 * 2. для каждого URL requests = successful + errors;
 * - это в аккумуляторе считается: статус либо в диапазоне 200-399 (успех),
 * либо нет (ошибка), и оба счетчика параллельно инкрементятся.
 * 3. maxDuration <= totalDuration при наличии запросов;
 * - самое большое время одного запроса не может быть больше общего времени.
 * 4. 0 <= activeFiles <= semaphoreLimit; после завершения анализа activeFiles равно нулю;
 * - за это отвечает семафор + декремент счетчика в блоке finally, поэтому
 * даже если что то упадет, все равно всё корректно сбросится.
 * 6. один повреждённый файл не мешает обработке остальных;
 * - поврежденные файлы ловятся через try-catch, улетают в счетчик ошибок,
 * но работу остальных файлов не стопают.
 * 7. в программе нет дедлока.
 * - дедлоков тут нет, потому что потоки не захватывают несколько ресурсов одновременно.
 * берем ровно одно разрешение у семафора перед чтением файла, а дальше работаем локально.
 * - поток не может ждать acquire(), удерживая synchronized-монитор, потому что семафор
 * запрашивается вообще вне всяких синхронизированных блоков.
 * - разрешение семафора возвращается, потому что оно вынесено в блок finally.
 * даже если файл вылетит с исключением, семафор все равно отпустит поток.
 * - volatile нужен для флага остановки, чтобы потоки не кешировали
 * значение у себя в процессоре, а реально видели, когда главный поток меняет его на true.
 */