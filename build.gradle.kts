import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.30"
    id("com.github.johnrengelman.shadow") version "6.1.0"
    application
}

group = "org.dash.mobile.explore.sync"
version = "1.0-SNAPSHOT"

val outputArchive = "${project.name}-all.jar"

repositories {
    mavenCentral()
}

dependencies {

    implementation("net.lingala.zip4j:zip4j:2.9.1")

    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("net.logstash.logback:logstash-logback-encoder:7.0.1")
    implementation("ch.qos.logback:logback-classic:1.2.11")

    implementation("com.google.apis:google-api-services-sheets:v4-rev20220221-1.32.1")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0")

    implementation("com.squareup.retrofit2:retrofit:2.9.0")
    implementation("com.squareup.retrofit2:converter-gson:2.9.0")
    implementation("com.squareup.okhttp3:logging-interceptor:4.9.1")

    implementation("com.google.cloud.functions:functions-framework-api:1.0.4")

    implementation("com.google.cloud:google-cloud-storage:2.4.4")

    implementation("org.xerial:sqlite-jdbc:3.36.0.3")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.8.2")
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.8.2")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<ShadowJar> {
    manifest.attributes.apply {
        put("Implementation-Title", "Dash Explore Sync")
        put("Implementation-Version", project.version)
    }
    archiveFileName.set(outputArchive)
    archiveClassifier.set("")
    archiveVersion.set("")
}

application {
    mainClass.set("org.dash.mobile.explore.sync.MainAppKt")
}

tasks.register<ShadowJar>("shadowJarFun") {
    from(sourceSets.main.get().output)
    configurations = listOf(project.configurations.runtimeClasspath.get())
    manifest.attributes.apply {
        put("Implementation-Title", "Dash Explore Sync Function")
        put("Implementation-Version", project.version)
        put("Main-Class", "org.dash.mobile.explore.sync.Function")
    }
    archiveFileName.set("${project.name}-fun.jar")
    archiveClassifier.set("fun")
    archiveVersion.set("")
}

tasks.register<ShadowJar>("shadowJarApp") {
    from(sourceSets.main.get().output)
    configurations = listOf(project.configurations.runtimeClasspath.get())
    manifest.attributes.apply {
        put("Implementation-Title", "Dash Explore Sync App")
        put("Implementation-Version", project.version)
        put("Main-Class", "org.dash.mobile.explore.sync.MainAppKt")
    }
    archiveFileName.set("${project.name}-app.jar")
    archiveClassifier.set("app")
    archiveVersion.set("")
}

tasks.register("buildFun") {
    group = "build"
    description = "Build Google Cloud Function JAR"
    dependsOn("shadowJarFun")
    doLast {
        copy {
            from("build/libs/${project.name}-fun.jar")
            into("build/deploy/")
        }
    }
}

tasks.register("buildApp") {
    group = "build"
    description = "Build standalone application JAR"
    dependsOn("shadowJarApp")
    doLast {
        copy {
            from("build/libs/${project.name}-app.jar")
            into("build/deploy/")
        }
    }
}
