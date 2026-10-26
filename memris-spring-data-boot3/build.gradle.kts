plugins {
    `java-library`
}

dependencies {
    api(project(":memris-core"))
    api(libs.spring.boot3.autoconfigure)
    api(libs.spring.data3.commons)
    api(libs.spring6.context)
    implementation(libs.spring6.tx)
    implementation(libs.slf4j2)
    api(libs.jakarta.persistence)
    testImplementation(libs.spring.boot3.test)
    testImplementation(libs.spring.boot3.test.autoconfigure)
    testRuntimeOnly(libs.slf4j2.simple)
}
