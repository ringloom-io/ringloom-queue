# ringloom-queue Java bindings

These bindings expose the `ringloom_queue` native C ABI through the Java Foreign Function & Memory API on Java 25.

The Gradle build embeds the current platform's shared library into the jar under `io/ringloom/queue/native/<os>-<arch>/`. By default, the native library is built from this repository with:

```sh
zig build c-abi -Doptimize=ReleaseSmall
```

You can override native loading at runtime with:

- `-Dringloom.queue.nativeLibPath=/absolute/path/to/libringloom_queue.so`
- `-Dringloom.queue.nativeLibDir=/directory/containing/the/library`

## Usage

```java
try (RingloomQueue queue = RingloomQueue.open(
         QueueConfig.create("data/events").withRollSchemeName("FAST_DAILY"));
     RingloomAppender appender = queue.openAppender()) {

    long index = appender.appendString("hello ringloom");

    try (RingloomTailer tailer = queue.openTailer(0)) {
        RingloomMessageView view = new RingloomMessageView();
        if (tailer.poll(view)) {
            System.out.println(view.index() + ": " + view.payloadString());
        }
    }
}
```

`RingloomMessageView` is a borrowed mmap view. Its payload segment is valid until the next poll on the same tailer or until that tailer is closed. Use `pollCopy()` or `payloadBytes()` when bytes must outlive that borrowed window.

## Build

```sh
cd bindings/java
gradle jar
```

The resulting jar includes the ReleaseSmall shared library for the build host platform.

## Test

```sh
cd bindings/java
gradle test
```

Tests run with `--enable-native-access=ALL-UNNAMED` and use the embedded native library unless you set one of the native override properties above.
