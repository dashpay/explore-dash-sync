# Explore Dash Sync
```
git clone https://github.com/dash-mobile-team/explore-dash-sync.git
cd explore-dash-sync
./gradlew shadowJar
cd build/libs
java -jar explore-dash-sync.jar
```

# Google API credentials (credentials.json) not found.
Google Cloud Console -> (dash-explore-sync) -> Credentials -> Download OAuth client
https://console.cloud.google.com/apis/credentials?project=dash-explore-sync

save file as src/main/resources/credentials.json
