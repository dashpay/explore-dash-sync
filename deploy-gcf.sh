#!/bin/bash

# Deploy to Google Cloud Functions Script
# This script ensures clean builds and proper deployment

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
FUNCTION_NAME="explore-dash-sync-4"
TOPIC_NAME="explore-dash-sync-trigger-4"
PROJECT_ID="dash-wallet-firebase"
RUNTIME="java17"
ENTRY_POINT="org.dash.mobile.explore.sync.Function"
MEMORY="1024MB"
TIMEOUT="600s"

echo -e "${GREEN}=== Dash Explore Sync - Google Cloud Function Deployment ===${NC}\n"

# Step 1: Clean build directory
echo -e "${YELLOW}[1/5]${NC} Cleaning build directory..."
./gradlew clean
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC} Clean completed\n"
else
    echo -e "${RED}✗${NC} Clean failed"
    exit 1
fi

# Step 2: Build function JAR
echo -e "${YELLOW}[2/5]${NC} Building function JAR..."
./gradlew buildFun
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC} Build completed\n"
else
    echo -e "${RED}✗${NC} Build failed"
    exit 1
fi

# Step 3: Verify build artifacts
echo -e "${YELLOW}[3/5]${NC} Verifying build artifacts..."
if [ -f "build/deploy/explore-dash-sync-fun.jar" ]; then
    JAR_SIZE=$(ls -lh build/deploy/explore-dash-sync-fun.jar | awk '{print $5}')
    echo -e "${GREEN}✓${NC} JAR file found: explore-dash-sync-fun.jar (${JAR_SIZE})"

    # Check for unwanted directories
    if [ -d "build/deploy/explore-dash-sync-fun" ]; then
        echo -e "${YELLOW}⚠${NC} Warning: Found extracted JAR directory, removing..."
        rm -rf build/deploy/explore-dash-sync-fun
    fi

    echo -e "\nContents of build/deploy/:"
    ls -lh build/deploy/
    echo ""
else
    echo -e "${RED}✗${NC} JAR file not found!"
    exit 1
fi

# Step 4: Deploy to Google Cloud Functions
echo -e "${YELLOW}[4/5]${NC} Deploying to Google Cloud Functions..."
echo -e "  Function: ${FUNCTION_NAME}"
echo -e "  Project: ${PROJECT_ID}"
echo -e "  Runtime: ${RUNTIME}\n"

gcloud functions deploy ${FUNCTION_NAME} \
  --project=${PROJECT_ID} \
  --runtime=${RUNTIME} \
  --entry-point=${ENTRY_POINT} \
  --source=build/deploy \
  --trigger-topic ${TOPIC_NAME} \
  --allow-unauthenticated \
  --memory=${MEMORY} \
  --timeout=${TIMEOUT}

if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}✓${NC} Deployment completed successfully!\n"
else
    echo -e "\n${RED}✗${NC} Deployment failed"
    exit 1
fi

# Step 5: Verify deployment
echo -e "${YELLOW}[5/5]${NC} Verifying deployment..."
UPDATE_TIME=$(gcloud functions describe ${FUNCTION_NAME} --project=${PROJECT_ID} --format="value(updateTime)" 2>/dev/null)
STATUS=$(gcloud functions describe ${FUNCTION_NAME} --project=${PROJECT_ID} --format="value(status)" 2>/dev/null)

if [ -n "$UPDATE_TIME" ]; then
    echo -e "${GREEN}✓${NC} Function verified:"
    echo -e "  Status: ${STATUS}"
    echo -e "  Last Updated: ${UPDATE_TIME}\n"
else
    echo -e "${YELLOW}⚠${NC} Could not verify function status\n"
fi

echo -e "${GREEN}=== Deployment Complete ===${NC}\n"
echo -e "To trigger the function manually, run:"
echo -e "  ${YELLOW}Production:${NC} gcloud pubsub topics publish ${TOPIC_NAME} --project=${PROJECT_ID}"
echo -e "  ${YELLOW}Testnet:${NC}    gcloud pubsub topics publish ${TOPIC_NAME} --project=${PROJECT_ID} --attribute=\"mode=testnet\""
echo ""