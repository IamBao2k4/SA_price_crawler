#!/bin/bash

# Script to build Docker images for Linux platform on local machine
# Usage: ./build_images.sh [--push]

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
IMAGE_NAME="binance-price-crawler"
VERSION="latest"
PLATFORM="linux/amd64"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Building Docker Images for Linux${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running!${NC}"
    exit 1
fi

# Build crawler (producer) image
echo -e "${YELLOW}[1/2] Building crawler image...${NC}"
docker build \
    --platform ${PLATFORM} \
    --no-cache \
    -t ${IMAGE_NAME}:crawler-${VERSION} \
    -f Dockerfile \
    .

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Crawler image built successfully${NC}"
else
    echo -e "${RED}✗ Failed to build crawler image${NC}"
    exit 1
fi

# Build consumer image
echo -e "${YELLOW}[2/2] Building consumer image...${NC}"
docker build \
    --platform ${PLATFORM} \
    --no-cache \
    -t ${IMAGE_NAME}:consumer-${VERSION} \
    -f Dockerfile \
    .

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Consumer image built successfully${NC}"
else
    echo -e "${RED}✗ Failed to build consumer image${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Build Summary${NC}"
echo -e "${GREEN}========================================${NC}"
docker images | grep ${IMAGE_NAME}

echo ""
echo -e "${GREEN}✓ All images built successfully!${NC}"
echo ""

# Optional: Push to registry
if [ "$1" == "--push" ]; then
    echo -e "${YELLOW}Pushing images to registry...${NC}"
    echo -e "${RED}Note: Update registry URL before pushing!${NC}"

    # Uncomment and update these lines with your registry URL
    # REGISTRY="your-registry.com"
    # docker tag ${IMAGE_NAME}:crawler-${VERSION} ${REGISTRY}/${IMAGE_NAME}:crawler-${VERSION}
    # docker tag ${IMAGE_NAME}:consumer-${VERSION} ${REGISTRY}/${IMAGE_NAME}:consumer-${VERSION}
    # docker push ${REGISTRY}/${IMAGE_NAME}:crawler-${VERSION}
    # docker push ${REGISTRY}/${IMAGE_NAME}:consumer-${VERSION}

    echo -e "${YELLOW}Push functionality not configured. Edit script to enable.${NC}"
fi

echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Save images to tar files:"
echo "   docker save ${IMAGE_NAME}:crawler-${VERSION} | gzip > crawler-image.tar.gz"
echo "   docker save ${IMAGE_NAME}:consumer-${VERSION} | gzip > consumer-image.tar.gz"
echo ""
echo "2. Copy to server:"
echo "   scp crawler-image.tar.gz consumer-image.tar.gz user@server:/path/to/upload/"
echo ""
echo "3. Load images on server:"
echo "   docker load < crawler-image.tar.gz"
echo "   docker load < consumer-image.tar.gz"
echo ""
echo "4. Update docker-compose.remote.yml to use these image names"
echo ""
