# Explore Dash Sync

## Build

### Recommended environments

Ubuntu 20.04.3 LTS or higher
  * Windows Subsystem for Linux
IntelliJ IDEA Community 2020.3 or higher

### Init

```
git clone https://github.com/dashpay/explore-dash-sync.git
cd explore-dash-sync
```

### Standalone app

#### Build

Generate executable [fat JAR](https://github.com/johnrengelman/shadow) `./build/deploy/explore-dash-sync-app.jar`) which
can be launched locally

```
./gradlew buildApp
```

#### Run

```
java -jar ./build/deploy/explore-dash-sync-app.jar
```

This command will generate `explore.dat` file in the current directory Supported arguments:

- `-upload` - upload data to GC Storage
- `-dev` - load data from dev servers
- `-quiet` - quiet mode: no notifications are pushed to Slack

### Google Cloud Function

#### Build

Generate [fat JAR](https://github.com/johnrengelman/shadow) `./build/deploy/explore-dash-sync-fun.jar` which can be
deployed to Google Cloud Platform ([BackgroundFunction](https://cloud.google.com/functions/docs/writing/background))

```
./gradlew buildFun
```

#### Deploy function to Google Cloud Platform

Base on [Deploying Cloud Functions](https://cloud.google.com/functions/docs/deploying)

```
# create pubsub topic
gcloud pubsub topics create explore-dash-sync-trigger-4

# deploy the function
gcloud functions deploy explore-dash-sync-4 \
 --runtime=java17 \
 --entry-point=org.dash.mobile.explore.sync.Function \
 --source=build/deploy \
 --trigger-topic explore-dash-sync-trigger-4 \
 --allow-unauthenticated --memory=1024MB --timeout=300s
``` 

Can be deployed manually
from https://console.cloud.google.com/functions/list?authuser=0&project=dash-wallet-firebase `CREATE FUNCTION`

#### Trigger function

Function can be triggered by publishing `explore-dash-sync-trigger-4` PubSub topic

```
gcloud pubsub topics publish explore-dash-sync-trigger-4
```

Or for the testnet mode:

```
gcloud pubsub topics publish explore-dash-sync-trigger-4 --attribute="mode=testnet"
```

#### After deploying

Check the first run's PiggyCards line in Slack (`PiggyCards: ...`). No response of the PiggyCards API has been
recorded, so the sync cannot know how the `currency` of a card is written. The rule is:

- the `country` property the brands are fetched with is not US: every PiggyCards merchant is left out
- the currency of the chosen card, trimmed and in upper case, is `USD`: kept as US/USD
- it is another well-formed currency code (three letters): left out of v4 and v5 and listed with the value
- it is absent, blank or anything else (`US Dollar`, `$`, `840`): kept as US/USD, which is what every PiggyCards
  merchant was taken for before, and listed under "unrecognised currency, treated as USD as before"

A `!!! WARNING` line follows when more than half of the US merchants are left out, or more than half are
unrecognised.

### Generating Protocol Buffer messages (`javalite`)

- src: `./src/main/proto/`
- dst: `./src/main/java/`

```
./gradlew generateProto
```

# Google API credentials (credentials.json) not found.

Google Cloud Console -> (dash-explore-sync) -> Credentials -> Download OAuth client
https://console.cloud.google.com/apis/credentials?project=dash-explore-sync

save file as src/main/resources/credentials.json

# Editing the database
Suppose new columns need to be added to the explore database.
1. Add the columns to the Room entities in Dash Wallet (`features/exploredash`), increase the version of
   `ExploreDatabase` and build, so that Room exports `features/exploredash/schemas/.../<version>.json`
2. Copy that file to `schemas/<version>.json` in this repository
3. Generate the empty database from it. This creates the tables, indices and FTS triggers of the schema and sets
   the Room `identity_hash` and the User Version, which must match those of `ExploreDatabase`:
    ```
    ./gradlew generateTemplate --args="schemas/5.json src/main/resources/explore-empty-v5.db"
    ```
4. Add code in `MerchantData` or `AtmData` to support those new columns
5. Depending on the data source, code must be added the `convert` methods

A wallet only opens a file with the User Version it was built for, so a new version is published as a new file
next to the old ones (see `ExploreVersion`). Each version has its own lock: when another run holds the lock of one
version, this run publishes only the other version and says so in Slack.

`schemas/5.json` must be regenerated from the wallet schema that is merged, and `explore-empty-v5.db` from it,
before any wallet release reads v5: Room checks its identity hash.

`explore-empty.db` (version 4) was made by hand before the generator existed and is kept as it is.
`./gradlew generateTemplate --args="schemas/4.json <file>"` gives the same tables, columns, indices, triggers,
identity hash and User Version, except for two declarations in `gift_card_providers`:

| | shipped `explore-empty.db` (v4) | Room schema, and so `explore-empty-v5.db` |
|---|---|---|
| `sourceId` | `INTEGER NOT NULL` | `TEXT NOT NULL` |
| `id` | `INTEGER PRIMARY KEY` | `INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL` |

A numeric PiggyCards `sourceId` is therefore stored as an integer in v4 and as text in v5.

Example of adding new columns to `MerchantData`:
```kotlin
data class MerchantData(
    // ...
    var minCardPurchase: Double? = 0.0,
    var maxCardPurchase: Double? = 0.0,
    
    // add new column variables here...
) {
    companion object {
        // Add each new column and a ? for it to the statement returned by insertStatement(version)
        
        // ...
        const val MIN_CARD_PURCHASE_COL = 26
        const val MAX_CARD_PURCHASE_COL = 27

        // add new column number constance here...
    }

    override fun transferInto(statement: PreparedStatement): PreparedStatement {
        return statement.apply {
            // ...
            setDouble(MIN_CARD_PURCHASE_COL, minCardPurchase ?: 0.0)
            setDouble(MAX_CARD_PURCHASE_COL, maxCardPurchase ?: 0.0)

            // add new statements here to set the values of the new columns
        }
    }
}
```

# Database Versions

1. DCG Merchants and ATM's for DashPay (android) < 11.1.2 and Dash Wallet (iOS) < 8.3
2. Beta version for DashSpend  for DashPay (android) = 11.2.1-2
3. Production for DashSpend (CTX) for DashPay (android) >= 11.2.2 and Dash Wallet (iOS) >= 8.4 
4. Production for DashSpend (CTX + PiggyCards)
5. DashSpend gift cards of every country: adds `merchant.country`, `merchant.currency` and
   `gift_card_providers.currency`. Each sync publishes it as `explore/explore-v5.db` next to
   `explore/explore-v4.db`, which keeps US gift card merchants only because the wallets reading it assume USD
   (CTX merchants with country US and currency USD, and US PiggyCards merchants unless their card names another
   currency code, see "After deploying"). While CTX returns US/USD merchants only and no PiggyCards card names
   another currency, v4 is what it was before v5 existed. Before the country filter, a US merchant sharing its
   name with a CTX merchant of another country that came first in the feed would have been given that
   merchant's id; it now always gets the id of the US merchant
