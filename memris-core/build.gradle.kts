import java.time.Duration

plugins {
    `java-library`
    jacoco
    alias(libs.plugins.pitest)
}

dependencies {
    api(libs.bytebuddy)
    api(libs.sparsebitset)
    api(libs.roaringbitmap)
    testImplementation(libs.archunit)
    testImplementation(libs.junit.params)
}

pitest {
    pitestVersion.set(libs.versions.pitest.asProvider())
    junit5PluginVersion.set(libs.versions.pitest.junit5.get())
    targetClasses.set(setOf(
        "io.memris.index.*",
        "io.memris.kernel.RowIdSetFactory",
        "io.memris.kernel.RowIdBitSet",
        "io.memris.kernel.RowIdArraySet",
        "io.memris.storage.heap.PageColumnString",
        "io.memris.storage.heap.PageColumnInt",
        "io.memris.storage.heap.PageColumnLong",
        "io.memris.storage.SelectionImpl",
    ))
    targetTests.set(setOf("io.memris.realworld.*", "io.memris.index.*", "io.memris.kernel.*", "io.memris.storage.*"))
    excludedGroups.set(setOf("benchmark"))
    mutationThreshold.set(85)
    coverageThreshold.set(90)
    threads.set(8)
    timeoutFactor.set(20.toBigDecimal())
    timeoutConstInMillis.set(30_000)
    jvmArgs.set(listOf("-Xms2g", "-Xmx4g", "-XX:+UseG1GC"))
    outputFormats.set(setOf("HTML", "XML"))
    timestampedReports.set(false)
}

val main = sourceSets.getByName("main")
val test = sourceSets.getByName("test")
val jmh = sourceSets.create("jmh") {
    compileClasspath += main.output + test.output
    runtimeClasspath += main.output + test.output
}

configurations[jmh.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
configurations[jmh.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    add(jmh.implementationConfigurationName, libs.jmh.core)
    add(jmh.annotationProcessorConfigurationName, libs.jmh.generator)
}

tasks.register<Test>("threadSafetyTest") {
    description = "Runs arena/codegen tests in parallel, isolated JVM forks."
    group = "verification"
    testClassesDirs = test.output.classesDirs
    classpath = test.runtimeClasspath
    include(
        "**/MemrisArenaConcurrencyTest.class",
        "**/RuntimeExecutorGeneratorConcurrencyTest.class",
        "**/ConditionRowEvaluatorGeneratorConcurrencyTest.class",
        "**/EntitySaverGeneratorCacheTest.class",
        "**/EntityMaterializerGeneratorCacheTest.class",
        "**/RuntimeExecutorGeneratorArenaIsolationTest.class",
    )
    maxParallelForks = 2
    forkEvery = 1
    timeout.set(Duration.ofMinutes(2))
    systemProperty("junit.jupiter.execution.parallel.enabled", "true")
    systemProperty("junit.jupiter.execution.parallel.mode.default", "same_thread")
    systemProperty("junit.jupiter.execution.parallel.mode.classes.default", "concurrent")
}

val benchmarkIncludes = providers.gradleProperty("jmhIncludes").orElse("io.memris.benchmarks.EmbeddedPathBenchmark.*")
val benchmarkResult = layout.buildDirectory.file(providers.gradleProperty("jmhResult").orElse("reports/jmh/embedded.json"))
val benchmarkWarmup = providers.gradleProperty("jmhWarmupIterations").orElse("2")
val benchmarkIterations = providers.gradleProperty("jmhIterations").orElse("2")
val benchmarkForks = providers.gradleProperty("jmhForks").orElse("1")

tasks.register<JavaExec>("jmh") {
    description = "Runs JMH; select benchmarks with -PjmhIncludes=..."
    group = "benchmark"
    classpath = jmh.runtimeClasspath
    mainClass.set("org.openjdk.jmh.Main")
    args(
        benchmarkIncludes.get(), "-wi", benchmarkWarmup.get(), "-i", benchmarkIterations.get(),
        "-f", benchmarkForks.get(), "-rf", "json", "-rff", benchmarkResult.get().asFile.absolutePath,
    )
    val resultDirectory = benchmarkResult.get().asFile.parentFile
    doFirst { resultDirectory.mkdirs() }
}

listOf(main, test).forEach { sourceSet ->
    val isTest = sourceSet.name == "test"
    tasks.register(if (isTest) "writeTestRuntimeClasspath" else "writeRuntimeClasspath") {
        description = "Writes the ${sourceSet.name} runtime classpath for JShell."
        group = "development"
        dependsOn(tasks.named(sourceSet.classesTaskName))
        val runtimeClasspath = sourceSet.runtimeClasspath
        val outputFile = layout.buildDirectory.file(if (isTest) "test-runtime-classpath.txt" else "runtime-classpath.txt")
        inputs.files(runtimeClasspath)
        outputs.file(outputFile)
        doLast { outputFile.get().asFile.writeText(runtimeClasspath.filter { it.exists() }.asPath) }
    }
}
