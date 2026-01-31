package io.memris.arch;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

class ArchitectureTest {

    @Test
    void coreShouldNotDependOnSpring() {
        JavaClasses importedClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.memris.core..");

        ArchRule rule = noClasses().should().dependOnClassesThat().resideInAPackage("org.springframework..");

        rule.check(importedClasses);
    }

    @Test
    void storageShouldNotDependOnRuntime() {
        JavaClasses importedClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.memris..");

        ArchRule rule = noClasses()
                .that().resideInAPackage("io.memris.storage..")
                .should().dependOnClassesThat().resideInAPackage("io.memris.runtime..");

        rule.check(importedClasses);
    }
}
