plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("com.google.gms.google-services")
}

// Version number comes from GitHub (every build gets a higher number, which Play Store needs)
val buildNumber = (System.getenv("GITHUB_RUN_NUMBER") ?: "1").toInt()
val keystorePath: String? = System.getenv("MAATA_KEYSTORE")

android {
    namespace = "com.maataapp.app"
    compileSdk = 36

    defaultConfig {
        applicationId = "com.maataapp.app"
        minSdk = 26
        targetSdk = 36
        versionCode = buildNumber
        versionName = "1.0.$buildNumber"
        buildConfigField("String", "APP_URL", "\"https://maataapp.com\"")
    }

    // Two versions from the same code:
    //  web  = the APK on maataapp.com/download (updates itself from the website, full team location)
    //  play = the Google Play version (Play updates it; no self-update, no "Allow all the time" location)
    flavorDimensions += "store"
    productFlavors {
        create("web") { dimension = "store"; buildConfigField("String", "STORE", "\"web\"") }
        create("play") { dimension = "store"; buildConfigField("String", "STORE", "\"play\"") }
    }

    signingConfigs {
        if (keystorePath != null && file(keystorePath).exists()) {
            create("release") {
                storeFile = file(keystorePath)
                storePassword = System.getenv("MAATA_KEYSTORE_PASSWORD")
                keyAlias = System.getenv("MAATA_KEY_ALIAS")
                keyPassword = System.getenv("MAATA_KEY_PASSWORD")
            }
        }
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            if (keystorePath != null && file(keystorePath).exists()) signingConfig = signingConfigs.getByName("release")
        }
    }
    buildFeatures { buildConfig = true }
    lint { abortOnError = false; checkReleaseBuilds = false }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }
}

dependencies {
    implementation(platform("com.google.firebase:firebase-bom:34.19.0"))
    implementation("com.google.firebase:firebase-messaging")
    implementation("androidx.core:core-ktx:1.16.0")
}
