plugins {
    `java-library`
}

dependencies {
    api(project(":memris-spring-data-boot3"))
    api(libs.spring.boot3.starter)
}
