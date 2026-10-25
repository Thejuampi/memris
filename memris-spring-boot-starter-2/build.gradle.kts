plugins {
    `java-library`
}

dependencies {
    api(project(":memris-spring-data-boot2"))
    api(libs.spring.boot2.starter)
}
