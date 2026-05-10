plugins {
    java
}

fun normalizeOsName(osName: String): String = when {
    osName.startsWith("linux", ignoreCase = true) -> "linux"
    osName.startsWith("mac os", ignoreCase = true) || osName.startsWith("darwin", ignoreCase = true) -> "macos"
    else -> throw GradleException("Unsupported operating system for embedded ringloom-queue native library: $osName")
}

fun normalizeArchName(archName: String): String = when (archName.lowercase()) {
    "x86_64", "amd64" -> "x86_64"
    "aarch64", "arm64" -> "aarch64"
    else -> throw GradleException("Unsupported architecture for embedded ringloom-queue native library: $archName")
}

group = providers.gradleProperty("ringloomQueue.mavenGroup").orElse("io.ringloom").get()
version = providers.gradleProperty("ringloomQueue.version").orElse("0.0.0-SNAPSHOT").get()

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val repoRoot = layout.projectDirectory.dir("../..").asFile.canonicalFile
val nativeLibraryFileName = System.mapLibraryName("ringloom_queue")
val embeddedPlatform = "${normalizeOsName(System.getProperty("os.name"))}-${normalizeArchName(System.getProperty("os.arch"))}"
val embeddedNativeResourceDir = "io/ringloom/queue/native/$embeddedPlatform"
val configuredEmbeddedNativeLibDir = ((findProperty("ringloomQueue.embeddedNativeLibDir") as String?)
    ?: System.getProperty("ringloom.queue.nativeLibDir"))
    ?.takeIf { it.isNotBlank() }
val generatedResourcesDir = layout.buildDirectory.dir("generated/resources/main")
val embeddedNativeOutputDir = layout.buildDirectory.dir("generated/resources/main/$embeddedNativeResourceDir")
val embeddedNativeSourceFile = providers.provider {
    val nativeLibDir = configuredEmbeddedNativeLibDir?.let(::file) ?: repoRoot.resolve("zig-out/lib")
    nativeLibDir.resolve(nativeLibraryFileName)
}

sourceSets {
    main {
        resources.srcDir(generatedResourcesDir)
    }
}

val buildEmbeddedNativeLibrary = tasks.register<Exec>("buildEmbeddedNativeLibrary") {
    onlyIf { configuredEmbeddedNativeLibDir == null }
    workingDir = repoRoot
    commandLine("zig", "build", "c-abi", "-Doptimize=ReleaseSmall")
    inputs.files(
        repoRoot.resolve("build.zig"),
        repoRoot.resolve("build.zig.zon")
    )
    inputs.files(fileTree(repoRoot.resolve("src")))
    outputs.file(repoRoot.resolve("zig-out/lib").resolve(nativeLibraryFileName))
}

val stageEmbeddedNativeLibrary = tasks.register<Copy>("stageEmbeddedNativeLibrary") {
    dependsOn(buildEmbeddedNativeLibrary)
    from(embeddedNativeSourceFile)
    into(embeddedNativeOutputDir)
    doFirst {
        val sourceFile = embeddedNativeSourceFile.get()
        if (!sourceFile.exists()) {
            throw GradleException("Embedded ringloom-queue native library not found at $sourceFile")
        }
    }
}

tasks.named<ProcessResources>("processResources") {
    dependsOn(stageEmbeddedNativeLibrary)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    System.getProperty("ringloom.queue.nativeLibDir")?.takeIf { it.isNotBlank() }?.let {
        systemProperty("ringloom.queue.nativeLibDir", it)
    }
    System.getProperty("ringloom.queue.nativeLibPath")?.takeIf { it.isNotBlank() }?.let {
        systemProperty("ringloom.queue.nativeLibPath", it)
    }
}
