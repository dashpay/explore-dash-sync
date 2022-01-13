import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.google.protobuf.gradle.*

plugins {
    kotlin("jvm") version "1.5.30"
    id("com.github.johnrengelman.shadow") version "6.1.0"
    id("com.google.protobuf") version "0.8.18"
    application
}

group = "org.dash.mobile.explore.sync"
version = "1.0-SNAPSHOT"

val outputArchive = "${project.name}-all.jar"

repositories {
    mavenCentral()
}


dependencies {

    implementation("com.google.protobuf:protobuf-javalite:3.14.0")
    implementation("net.lingala.zip4j:zip4j:2.9.1")

    implementation("org.slf4j:slf4j-api:1.7.32")
    implementation("net.logstash.logback:logstash-logback-encoder:7.0.1")
    implementation("ch.qos.logback:logback-classic:1.2.10")

    implementation("com.google.apis:google-api-services-sheets:v4-rev612-1.25.0")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2")

    implementation("com.squareup.retrofit2:retrofit:2.9.0")
    implementation("com.squareup.retrofit2:converter-gson:2.9.0")

    implementation("com.google.cloud.functions:functions-framework-api:1.0.1")

    implementation("com.google.cloud:google-cloud-storage:2.3.0")

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
    }
    archiveFileName.set(outputArchive)
    archiveClassifier.set("")
    archiveVersion.set("")
}

//https://github.com/google/protobuf-gradle-plugin/issues/518
protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.14.0"
    }
    generateProtoTasks {
        ofSourceSet("main").forEach { task ->
            task.builtins {
                getByName("java") {
                    option("lite")
                }
            }
        }
    }
    generatedFilesBaseDir = File(projectDir, "/src").toString()
}

application {
    mainClass.set("org.dash.mobile.explore.sync.MainAppKt")
}

tasks.register("buildFun") {
    project.setProperty("mainClassName", "org.dash.mobile.explore.sync.Function")
    dependsOn("build")
    copy {
        from("build/libs/$outputArchive")
        into("build/deploy/")
        rename { "${rootProject.name}-fun.jar" }
    }
}

tasks.register("buildApp") {
    project.setProperty("mainClassName", "org.dash.mobile.explore.sync.MainAppKt")
    dependsOn("build")
    copy {
        from("build/libs/$outputArchive")
        into("build/deploy/")
        rename { "${rootProject.name}-app.jar" }
    }
}

