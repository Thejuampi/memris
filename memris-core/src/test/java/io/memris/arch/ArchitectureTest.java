package io.memris.arch;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

class ArchitectureTest {

    @Test
    void coreShouldNotDependOnSpring() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("io.memris.core..")
                .should().dependOnClassesThat().resideInAPackage("org.springframework..");
        rule.check(importedMainClasses());
    }

    @Test
    void storageShouldNotDependOnRuntime() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("io.memris.storage..")
                .should().dependOnClassesThat().resideInAPackage("io.memris.runtime..");
        rule.check(importedMainClasses());
    }

    @Test
    void coreShouldNotDependOnOtherModules() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("io.memris.core..")
                .should().dependOnClassesThat()
                .resideInAnyPackage(
                        "io.memris.runtime..",
                        "io.memris.repository..",
                        "io.memris.storage..",
                        "io.memris.index..",
                        "io.memris.kernel..",
                        "io.memris.query..");
        rule.check(importedMainClasses());
    }

    @Test
    void indexShouldNotDependOnStorage() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("io.memris.index..")
                .should().dependOnClassesThat().resideInAPackage("io.memris.storage..");
        rule.check(importedMainClasses());
    }

    @Test
    void kernelShouldNotDependOnRepositoryOrRuntime() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("io.memris.kernel..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("io.memris.repository..", "io.memris.runtime..");
        rule.check(importedMainClasses());
    }

    @Test
    void queryShouldNotDependOnRepositoryOrRuntime() {
        ArchRule rule = noClasses()
                .that().resideInAPackage("io.memris.query..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("io.memris.repository..", "io.memris.runtime..");
        rule.check(importedMainClasses());
    }

    @Test
    void mainCodeShouldNotDependOnTestPackages() {
        ArchRule rule = noClasses()
                .should().dependOnClassesThat().resideInAPackage("..test..");
        rule.check(importedMainClasses());
    }

    private static JavaClasses importedMainClasses() {
        return new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.memris..");
    }
}
