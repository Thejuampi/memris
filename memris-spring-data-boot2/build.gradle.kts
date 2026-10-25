plugins {
    `java-library`
}

dependencies {
    api(project(":memris-core"))
    api(libs.spring.boot2.autoconfigure)
    api(libs.spring.data2.commons)
    api(libs.spring5.context)
    implementation(libs.slf4j1)
    compileOnly(libs.javax.persistence)
    testImplementation(libs.javax.persistence)
    testImplementation(libs.spring.boot2.test)
    testImplementation(libs.spring.boot2.test.autoconfigure)
    testRuntimeOnly(libs.slf4j1.simple)
}
