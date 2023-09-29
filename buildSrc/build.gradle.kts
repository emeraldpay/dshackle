plugins {
    `kotlin-dsl`
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("org.yaml:snakeyaml:1.24")
    implementation("dshackle:foundation:1.0.0")
    implementation("com.squareup:kotlinpoet:1.14.2")
}
