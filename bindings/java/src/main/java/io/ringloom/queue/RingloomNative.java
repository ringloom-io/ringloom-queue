// SPDX-License-Identifier: Apache-2.0

package io.ringloom.queue;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

final class RingloomNative {
    static final int ABI_VERSION = 1;
    private static final String LIBRARY_BASE_NAME = "ringloom_queue";
    private static final String CLASSPATH_LIBRARY_ROOT = "/io/ringloom/queue/native";

    static final Linker LINKER = Linker.nativeLinker();
    static final Arena LIBRARY_ARENA = Arena.ofShared();
    static final AddressLayout ADDRESS = ValueLayout.ADDRESS;

    static final long QUEUE_OPTIONS_SIZE = 64L;
    static final long MESSAGE_VIEW_SIZE = 32L;

    static final long OPTIONS_SIZE_OFFSET = 0L;
    static final long OPTIONS_DIR_OFFSET = 8L;
    static final long OPTIONS_DIR_LEN_OFFSET = 16L;
    static final long OPTIONS_CREATE_OFFSET = 24L;
    static final long OPTIONS_ROLL_SCHEME_NAME_OFFSET = 32L;
    static final long OPTIONS_ROLL_SCHEME_NAME_LEN_OFFSET = 40L;
    static final long OPTIONS_ENABLE_PREFETCHER_OFFSET = 48L;
    static final long OPTIONS_ENABLE_CLEANER_OFFSET = 49L;
    static final long OPTIONS_SPAWN_HELPER_THREADS_OFFSET = 50L;
    static final long OPTIONS_RETENTION_CYCLES_OFFSET = 52L;
    static final long OPTIONS_RETENTION_CYCLES_SET_OFFSET = 56L;

    static final long MESSAGE_VIEW_SIZE_OFFSET = 0L;
    static final long MESSAGE_VIEW_INDEX_OFFSET = 8L;
    static final long MESSAGE_VIEW_DATA_OFFSET = 16L;
    static final long MESSAGE_VIEW_DATA_LEN_OFFSET = 24L;

    private static final SymbolLookup SYMBOLS;

    private static final MethodHandle ABI_VERSION_HANDLE;
    private static final MethodHandle STRERROR_HANDLE;
    private static final MethodHandle QUEUE_OPEN_HANDLE;
    private static final MethodHandle QUEUE_CLOSE_HANDLE;
    private static final MethodHandle APPENDER_OPEN_HANDLE;
    private static final MethodHandle APPENDER_APPEND_HANDLE;
    private static final MethodHandle APPENDER_CLOSE_HANDLE;
    private static final MethodHandle TAILER_OPEN_HANDLE;
    private static final MethodHandle TAILER_POLL_HANDLE;
    private static final MethodHandle TAILER_PREFETCH_POLL_HANDLE;
    private static final MethodHandle TAILER_CLOSE_HANDLE;
    private static final MethodHandle QUEUE_PREFETCH_POLL_HANDLE;
    private static final MethodHandle QUEUE_CLEANER_POLL_HANDLE;
    private static final MethodHandle QUEUE_MAINTENANCE_POLL_HANDLE;

    static {
        try {
            Path libraryPath = nativeLibraryPath();
            System.load(libraryPath.toAbsolutePath().toString());
            SYMBOLS = SymbolLookup.libraryLookup(libraryPath, LIBRARY_ARENA);

            ABI_VERSION_HANDLE = downcall("ringloom_abi_version", FunctionDescriptor.of(ValueLayout.JAVA_INT));
            STRERROR_HANDLE = downcall("ringloom_strerror", FunctionDescriptor.of(ADDRESS, ValueLayout.JAVA_INT));
            QUEUE_OPEN_HANDLE = downcall("ringloom_queue_open", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS));
            QUEUE_CLOSE_HANDLE = downcall("ringloom_queue_close", FunctionDescriptor.ofVoid(ADDRESS));
            APPENDER_OPEN_HANDLE = downcall("ringloom_appender_open", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS));
            APPENDER_APPEND_HANDLE = downcall("ringloom_appender_append", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS, ValueLayout.JAVA_LONG, ADDRESS));
            APPENDER_CLOSE_HANDLE = downcall("ringloom_appender_close", FunctionDescriptor.ofVoid(ADDRESS));
            TAILER_OPEN_HANDLE = downcall("ringloom_tailer_open", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ValueLayout.JAVA_LONG, ADDRESS));
            TAILER_POLL_HANDLE = downcall("ringloom_tailer_poll", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS));
            TAILER_PREFETCH_POLL_HANDLE = downcall("ringloom_tailer_prefetch_poll", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ValueLayout.JAVA_INT, ADDRESS));
            TAILER_CLOSE_HANDLE = downcall("ringloom_tailer_close", FunctionDescriptor.ofVoid(ADDRESS));
            QUEUE_PREFETCH_POLL_HANDLE = downcall("ringloom_queue_prefetch_poll", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ValueLayout.JAVA_INT, ADDRESS));
            QUEUE_CLEANER_POLL_HANDLE = downcall("ringloom_queue_cleaner_poll", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ValueLayout.JAVA_INT, ADDRESS));
            QUEUE_MAINTENANCE_POLL_HANDLE = downcall("ringloom_queue_maintenance_poll", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ValueLayout.JAVA_INT, ADDRESS));

            int abiVersion = abiVersion();
            if (abiVersion != ABI_VERSION) {
                throw new IllegalStateException("Unsupported ringloom-queue native ABI version " + abiVersion);
            }
        } catch (RuntimeException | Error ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new IllegalStateException("Failed to initialize ringloom-queue native bindings", ex);
        }
    }

    private RingloomNative() {
    }

    static Path nativeLibraryPath() {
        String libPath = System.getProperty("ringloom.queue.nativeLibPath");
        if (libPath != null && !libPath.isBlank()) {
            return requireExistingLibrary(Path.of(libPath));
        }

        String libDir = System.getProperty("ringloom.queue.nativeLibDir");
        if (libDir != null && !libDir.isBlank()) {
            return requireExistingLibrary(Path.of(libDir).resolve(System.mapLibraryName(LIBRARY_BASE_NAME)));
        }

        return extractClasspathLibrary();
    }

    private static Path requireExistingLibrary(Path path) {
        if (!Files.exists(path)) {
            throw new IllegalStateException("Native library not found at " + path);
        }
        return path;
    }

    private static Path extractClasspathLibrary() {
        String mappedLibraryName = System.mapLibraryName(LIBRARY_BASE_NAME);
        String resourcePath = CLASSPATH_LIBRARY_ROOT + "/" + platformIdentifier() + "/" + mappedLibraryName;

        try (var libraryStream = RingloomNative.class.getResourceAsStream(resourcePath)) {
            if (libraryStream == null) {
                throw new IllegalStateException(
                    "Embedded native library resource not found at " + resourcePath
                        + "; set ringloom.queue.nativeLibPath or ringloom.queue.nativeLibDir to load an external build"
                );
            }

            Path tempDir = Files.createTempDirectory("ringloom-queue-native-");
            Path extractedLibrary = tempDir.resolve(mappedLibraryName);
            Files.copy(libraryStream, extractedLibrary);
            extractedLibrary.toFile().deleteOnExit();
            tempDir.toFile().deleteOnExit();
            return extractedLibrary;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to extract embedded ringloom-queue native library", ex);
        }
    }

    private static String platformIdentifier() {
        return normalizeOsName(System.getProperty("os.name")) + "-" + normalizeArchName(System.getProperty("os.arch"));
    }

    private static String normalizeOsName(String osName) {
        String normalized = osName.toLowerCase();
        if (normalized.startsWith("linux")) {
            return "linux";
        }
        if (normalized.startsWith("mac os") || normalized.startsWith("darwin")) {
            return "macos";
        }
        throw new IllegalStateException("Unsupported operating system for embedded ringloom-queue native library: " + osName);
    }

    private static String normalizeArchName(String archName) {
        return switch (archName.toLowerCase()) {
            case "x86_64", "amd64" -> "x86_64";
            case "aarch64", "arm64" -> "aarch64";
            default -> throw new IllegalStateException("Unsupported architecture for embedded ringloom-queue native library: " + archName);
        };
    }

    static int abiVersion() {
        try {
            return (int) ABI_VERSION_HANDLE.invokeExact();
        } catch (Throwable throwable) {
            throw propagate("ringloom_abi_version", throwable);
        }
    }

    static int queueOpen(MemorySegment options, MemorySegment outQueue) {
        try {
            return (int) QUEUE_OPEN_HANDLE.invokeExact(options, outQueue);
        } catch (Throwable throwable) {
            throw propagate("ringloom_queue_open", throwable);
        }
    }

    static void queueClose(MemorySegment queue) {
        try {
            QUEUE_CLOSE_HANDLE.invokeExact(queue);
        } catch (Throwable throwable) {
            throw propagate("ringloom_queue_close", throwable);
        }
    }

    static int appenderOpen(MemorySegment queue, MemorySegment outAppender) {
        try {
            return (int) APPENDER_OPEN_HANDLE.invokeExact(queue, outAppender);
        } catch (Throwable throwable) {
            throw propagate("ringloom_appender_open", throwable);
        }
    }

    static int appenderAppend(MemorySegment appender, MemorySegment payload, long payloadLen, MemorySegment outIndex) {
        try {
            return (int) APPENDER_APPEND_HANDLE.invokeExact(appender, payload, payloadLen, outIndex);
        } catch (Throwable throwable) {
            throw propagate("ringloom_appender_append", throwable);
        }
    }

    static void appenderClose(MemorySegment appender) {
        try {
            APPENDER_CLOSE_HANDLE.invokeExact(appender);
        } catch (Throwable throwable) {
            throw propagate("ringloom_appender_close", throwable);
        }
    }

    static int tailerOpen(MemorySegment queue, long startIndex, MemorySegment outTailer) {
        try {
            return (int) TAILER_OPEN_HANDLE.invokeExact(queue, startIndex, outTailer);
        } catch (Throwable throwable) {
            throw propagate("ringloom_tailer_open", throwable);
        }
    }

    static int tailerPoll(MemorySegment tailer, MemorySegment outView) {
        try {
            return (int) TAILER_POLL_HANDLE.invokeExact(tailer, outView);
        } catch (Throwable throwable) {
            throw propagate("ringloom_tailer_poll", throwable);
        }
    }

    static int tailerPrefetchPoll(MemorySegment tailer, int maxWorkUnits, MemorySegment outStep) {
        try {
            return (int) TAILER_PREFETCH_POLL_HANDLE.invokeExact(tailer, maxWorkUnits, outStep);
        } catch (Throwable throwable) {
            throw propagate("ringloom_tailer_prefetch_poll", throwable);
        }
    }

    static void tailerClose(MemorySegment tailer) {
        try {
            TAILER_CLOSE_HANDLE.invokeExact(tailer);
        } catch (Throwable throwable) {
            throw propagate("ringloom_tailer_close", throwable);
        }
    }

    static int queuePrefetchPoll(MemorySegment queue, int maxWorkUnits, MemorySegment outStep) {
        try {
            return (int) QUEUE_PREFETCH_POLL_HANDLE.invokeExact(queue, maxWorkUnits, outStep);
        } catch (Throwable throwable) {
            throw propagate("ringloom_queue_prefetch_poll", throwable);
        }
    }

    static int queueCleanerPoll(MemorySegment queue, int maxWorkUnits, MemorySegment outStep) {
        try {
            return (int) QUEUE_CLEANER_POLL_HANDLE.invokeExact(queue, maxWorkUnits, outStep);
        } catch (Throwable throwable) {
            throw propagate("ringloom_queue_cleaner_poll", throwable);
        }
    }

    static int queueMaintenancePoll(MemorySegment queue, int maxWorkUnits, MemorySegment outStep) {
        try {
            return (int) QUEUE_MAINTENANCE_POLL_HANDLE.invokeExact(queue, maxWorkUnits, outStep);
        } catch (Throwable throwable) {
            throw propagate("ringloom_queue_maintenance_poll", throwable);
        }
    }

    static String statusName(int status) {
        try {
            return readCString((MemorySegment) STRERROR_HANDLE.invokeExact(status), 128);
        } catch (Throwable throwable) {
            throw propagate("ringloom_strerror", throwable);
        }
    }

    static void throwForStatus(String action, int status) {
        if (status == RingloomStatus.OK) {
            return;
        }
        if (status == RingloomStatus.INVALID_ARGUMENT) {
            throw new IllegalArgumentException(action + " failed: " + statusName(status));
        }
        if (status == RingloomStatus.OUT_OF_MEMORY) {
            throw new OutOfMemoryError(action + " failed: " + statusName(status));
        }
        throw new RingloomQueueException(status, statusName(status), action);
    }

    static StepResult readStepResult(MemorySegment outStep) {
        return StepResult.fromNative(outStep.get(ValueLayout.JAVA_INT, 0));
    }

    static String readCString(MemorySegment address, int maxBytes) {
        if (address == null || address.address() == 0) {
            return "";
        }

        MemorySegment bytes = address.reinterpret(maxBytes);
        int len = 0;
        while (len < maxBytes && bytes.get(ValueLayout.JAVA_BYTE, len) != 0) {
            len += 1;
        }

        byte[] copy = new byte[len];
        MemorySegment.copy(bytes, ValueLayout.JAVA_BYTE, 0, copy, 0, len);
        return new String(copy, StandardCharsets.UTF_8);
    }

    private static MethodHandle downcall(String symbol, FunctionDescriptor descriptor) {
        MemorySegment address = SYMBOLS.find(symbol)
            .orElseThrow(() -> new IllegalStateException("Missing native symbol " + symbol));
        return LINKER.downcallHandle(address, descriptor);
    }

    private static RuntimeException propagate(String action, Throwable throwable) {
        if (throwable instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (throwable instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("Native invocation failed for " + action, throwable);
    }
}
