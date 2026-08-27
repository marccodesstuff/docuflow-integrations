rootProject.name = "docuflow-integrations"

enableFeaturePreview("VERSION_CATALOGS")

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            fromFiles(files("gradle/libs.versions.toml"))
        }
    }
}