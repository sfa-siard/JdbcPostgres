/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Java library project to get you started.
 * For more details take a look at the 'Building Java & JVM projects' chapter in the Gradle
 * User Manual available at https://docs.gradle.org/7.3.2/userguide/building_java_projects.html
 */

plugins {
    // Apply the java-library plugin for API and implementation separation.
    `java-library`
}

version = "2.2.2"

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

sourceSets {
    create("intTest") {
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
}

val intTestImplementation by configurations.getting {
    extendsFrom(configurations.implementation.get())
}

configurations["intTestRuntimeOnly"].extendsFrom(configurations.runtimeOnly.get())

dependencies {
    implementation("org.antlr:antlr4-runtime:4.5.2-1")

    // legacy dependencies -
    implementation(fileTree("lib") { include("*.jar") })

    // test dependencies
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testImplementation("org.junit.vintage:junit-vintage-engine")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")

    // integration tests
    intTestImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
    intTestImplementation("org.testcontainers:testcontainers:1.18.0")
    intTestImplementation("org.testcontainers:junit-jupiter:1.18.0")
    intTestImplementation("org.testcontainers:postgresql:1.18.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    archiveFileName.set("jdbcpostgres.jar")
}

tasks.getByName("assemble").dependsOn("testJar")

tasks.register<Jar>("testJar") {
    archiveFileName.set("jdbcpostgres-test.jar")
    from(project.the<SourceSetContainer>()["test"].output)
}

val integrationTest = task<Test>("integrationTest") {
    description = "Runs integration tests."
    group = "verification"

    testClassesDirs = sourceSets["intTest"].output.classesDirs
    classpath = sourceSets["intTest"].runtimeClasspath
    shouldRunAfter("test")

    useJUnitPlatform()

    testLogging {
        events("passed")
    }
}

tasks.check { dependsOn(integrationTest) }