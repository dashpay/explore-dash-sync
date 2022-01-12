import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.30"
    id("com.github.johnrengelman.shadow") version "6.1.0"
    application
}

group = "org.dash.mobile.explore.sync"
version = "1.0-SNAPSHOT"

val outputArchive = "${project.name}-${project.version}.jar"

// Required by the 'shadowJar' task
project.setProperty("mainClassName", "org.dash.mobile.explore.sync.Function")

repositories {
    mavenCentral()
}

dependencies {

    implementation("io.github.microutils:kotlin-logging:1.11.5")
    implementation("org.slf4j:slf4j-log4j12:1.7.32")

    implementation("com.google.apis:google-api-services-sheets:v4-rev612-1.25.0")
    implementation("com.google.auth:google-auth-library-oauth2-http:1.3.0")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2")

    implementation("com.squareup.retrofit2:retrofit:2.9.0")
    implementation("com.squareup.retrofit2:converter-gson:2.9.0")

    implementation("com.google.cloud.functions:functions-framework-api:1.0.1")

    testImplementation(kotlin("test-junit"))
}

tasks.test {
    useJUnit()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<ShadowJar> {
    manifest.attributes.apply {
        put("Implementation-Title", "Dash Explore Sync")
        put("Implementation-Version", project.version)
        put("Main-Class", "org.dash.mobile.explore.sync.Function")
    }
    archiveFileName.set(outputArchive)
    archiveClassifier.set("")
    archiveVersion.set("")
}

tasks {
    build {
        dependsOn(shadowJar)
        copy {
            from("build/libs/$outputArchive")
            into("build/deploy")
        }
    }
}

application {
    mainClass.set("org.dash.mobile.explore.sync.MainAppKt")
}