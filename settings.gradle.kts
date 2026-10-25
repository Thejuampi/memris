rootProject.name = "memris"

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}

include(
    "memris-core",
    "memris-spring-data-boot2",
    "memris-spring-data-boot3",
    "memris-spring-boot-starter-2",
    "memris-spring-boot-starter-3",
)
