import com.github.spotbugs.snom.Confidence
import com.github.spotbugs.snom.Effort
import com.github.spotbugs.snom.SpotBugsExtension
import com.github.spotbugs.snom.SpotBugsTask
import com.vanniktech.maven.publish.MavenPublishBaseExtension

plugins {
    base
    alias(libs.plugins.maven.publish) apply false
    alias(libs.plugins.spotbugs) apply false
}

group = "io.github.thejuampi"
version = providers.gradleProperty("version").get()

val catalog = libs
val qualityChecks = providers.gradleProperty("quality").map(String::toBoolean).getOrElse(false)

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "jacoco")
    apply(plugin = "com.vanniktech.maven.publish")

    group = rootProject.group
    version = rootProject.version

    extensions.configure<JavaPluginExtension> {
        toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    }

    dependencies {
        "compileOnly"(catalog.lombok)
        "annotationProcessor"(catalog.lombok)
        "testCompileOnly"(catalog.lombok)
        "testAnnotationProcessor"(catalog.lombok)
        "testImplementation"(catalog.junit.api)
        "testImplementation"(catalog.assertj)
        "testRuntimeOnly"(catalog.junit.engine)
        "testRuntimeOnly"(catalog.junit.platform.launcher)
    }

    tasks.withType<JavaCompile>().configureEach {
        options.release.set(21)
        options.encoding = "UTF-8"
        options.compilerArgs.add("-parameters")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform {
            excludeTags("benchmark")
        }
        minHeapSize = "256m"
        maxHeapSize = "768m"
        jvmArgs("--enable-native-access=ALL-UNNAMED")
    }

    val testSourceSet = extensions.getByType<SourceSetContainer>().getByName("test")
    val realWorldVariants = listOf(
        Triple("realWorldBytecodeTest", "BYTECODE", true),
        Triple("realWorldBytecodeFallbackTest", "BYTECODE", false),
        Triple("realWorldMethodHandleTest", "METHOD_HANDLE", true),
        Triple("realWorldMethodHandleFallbackTest", "METHOD_HANDLE", false),
    ).map { (taskName, table, codegen) ->
        tasks.register<Test>(taskName) {
            description = "Runs real-world scenarios with $table tables and codegen=$codegen."
            group = "verification"
            testClassesDirs = testSourceSet.output.classesDirs
            classpath = testSourceSet.runtimeClasspath
            include("**/realworld/**")
            systemProperty("memris.test.table", table)
            systemProperty("memris.test.codegen", codegen.toString())
        }
    }
    tasks.register("realWorldTest") {
        description = "Runs the complete real-world table/codegen matrix."
        group = "verification"
        dependsOn(realWorldVariants)
    }
    tasks.named<Test>("test") {
        exclude("**/realworld/**")
        dependsOn(realWorldVariants)
    }

    tasks.withType<Javadoc>().configureEach {
        isFailOnError = false
        (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
    }

    extensions.configure<JacocoPluginExtension> {
        toolVersion = catalog.versions.jacoco.get()
    }
    tasks.withType<JacocoReport>().configureEach {
        dependsOn(tasks.named("test"))
        executionData.setFrom((listOf("test") + realWorldVariants.map { it.name }).map {
            layout.buildDirectory.file("jacoco/$it.exec")
        })
        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }

    // Keep analysis separate from the normal feedback loop, as Maven test/verify were.
    if (qualityChecks) {
        apply(plugin = "checkstyle")
        apply(plugin = "pmd")
        apply(plugin = "com.github.spotbugs")
        extensions.configure<CheckstyleExtension> {
            toolVersion = catalog.versions.checkstyle.get()
            configDirectory.set(rootProject.layout.projectDirectory.dir("config/checkstyle"))
            sourceSets = listOf(extensions.getByType<SourceSetContainer>().getByName("main"))
        }
        extensions.configure<PmdExtension> {
            toolVersion = catalog.versions.pmd.get()
            ruleSets = emptyList()
            ruleSetFiles = rootProject.files("config/pmd/rules.xml", "config/pmd/deadcode.xml", "config/pmd/excluded.xml")
            isIgnoreFailures = true
        }
        tasks.withType<Pmd>().configureEach {
            enabled = name == "pmdMain"
        }
        extensions.configure<SpotBugsExtension> {
            toolVersion.set(catalog.versions.spotbugs)
            effort.set(Effort.MAX)
            reportLevel.set(Confidence.MEDIUM)
            excludeFilter.set(rootProject.layout.projectDirectory.file("config/spotbugs/exclude.xml"))
        }
        tasks.withType<SpotBugsTask>().configureEach {
            enabled = name == "spotbugsMain"
        }
    }

    extensions.configure<MavenPublishBaseExtension> {
        coordinates(group.toString(), if (project.name == "memris-core") "memris" else project.name, version.toString())
        pom {
            name.set(project.name)
            description.set("Heap-based in-memory storage for Java 21 and Spring integration")
            url.set("https://github.com/Thejuampi/memris")
            licenses {
                license {
                    name.set("MIT License")
                    url.set("https://opensource.org/licenses/MIT")
                }
            }
            developers {
                developer {
                    id.set("thejuampi")
                    name.set("Juan Pablo Abelardo Lescano")
                    url.set("https://github.com/Thejuampi")
                }
            }
            scm {
                connection.set("scm:git:https://github.com/Thejuampi/memris.git")
                developerConnection.set("scm:git:https://github.com/Thejuampi/memris.git")
                url.set("https://github.com/Thejuampi/memris")
            }
        }
    }
    extensions.configure<PublishingExtension> {
        repositories.maven {
            name = "build"
            url = rootProject.layout.buildDirectory.dir("repository").get().asFile.toURI()
        }
    }
}

tasks.named("assemble") {
    dependsOn(subprojects.map { it.tasks.named("assemble") })
}
tasks.named("check") {
    dependsOn(subprojects.map { it.tasks.named("check") })
}

tasks.register("realWorldTest") {
    description = "Runs real-world scenarios in core and both Spring generations."
    group = "verification"
    dependsOn(subprojects.map { it.tasks.named("realWorldTest") })
}
