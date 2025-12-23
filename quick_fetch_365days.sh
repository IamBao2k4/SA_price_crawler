#!/bin/bash
# Quick Start Script for Fetching 365 Days Historical Data
# Usage: ./quick_fetch_365days.sh

set -e  # Exit on error

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "${BOLD}${BLUE}================================${NC}"
echo -e "${BOLD}${BLUE}🚀 365 Days Data Fetcher${NC}"
echo -e "${BOLD}${BLUE}================================${NC}\n"

# Step 1: Check if .env exists
echo -e "${BOLD}[Step 1/5]${NC} Checking environment..."
if [ ! -f .env ]; then
    echo -e "${RED}❌ .env file not found!${NC}"
    echo -e "${YELLOW}Creating from .env.example...${NC}"

    if [ -f .env.example ]; then
        cp .env.example .env
        echo -e "${GREEN}✅ .env created. Please edit it with your settings.${NC}"
        exit 1
    else
        echo -e "${RED}❌ .env.example not found either!${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✅ .env file found${NC}"
fi

# Step 2: Check if MongoDB is running
echo -e "\n${BOLD}[Step 2/5]${NC} Checking MongoDB..."
if docker ps | grep -q binance_mongodb; then
    echo -e "${GREEN}✅ MongoDB is running${NC}"
else
    echo -e "${YELLOW}⚠️ MongoDB not running. Starting...${NC}"
    docker-compose up -d mongodb

    echo -e "${CYAN}⏳ Waiting for MongoDB to be ready...${NC}"
    sleep 5

    if docker ps | grep -q binance_mongodb; then
        echo -e "${GREEN}✅ MongoDB started successfully${NC}"
    else
        echo -e "${RED}❌ Failed to start MongoDB${NC}"
        exit 1
    fi
fi

# Step 3: Check if Python dependencies installed
echo -e "\n${BOLD}[Step 3/5]${NC} Checking Python dependencies..."
if python3 -c "import pymongo" 2>/dev/null; then
    echo -e "${GREEN}✅ Dependencies installed${NC}"
else
    echo -e "${YELLOW}⚠️ Installing dependencies...${NC}"
    pip install -r requirements.txt
    echo -e "${GREEN}✅ Dependencies installed${NC}"
fi

# Step 4: Ask user for options
echo -e "\n${BOLD}[Step 4/5]${NC} Fetch options:"
echo -e "  ${CYAN}1)${NC} Fetch 365 days (4h interval) - Recommended"
echo -e "  ${CYAN}2)${NC} Fetch 180 days (4h interval) - Faster test"
echo -e "  ${CYAN}3)${NC} Fetch 90 days (4h interval) - Quick test"
echo -e "  ${CYAN}4)${NC} Custom"
echo -e "  ${CYAN}5)${NC} Resume previous fetch"

read -p "Select option [1-5]: " option

case $option in
    1)
        DAYS=365
        INTERVAL=4h
        ;;
    2)
        DAYS=180
        INTERVAL=4h
        ;;
    3)
        DAYS=90
        INTERVAL=4h
        ;;
    4)
        read -p "Enter number of days: " DAYS
        read -p "Enter interval (1m/5m/15m/1h/4h/1d): " INTERVAL
        ;;
    5)
        DAYS=365
        INTERVAL=4h
        RESUME="--resume"
        ;;
    *)
        echo -e "${RED}Invalid option${NC}"
        exit 1
        ;;
esac

# Step 5: Start fetch
echo -e "\n${BOLD}[Step 5/5]${NC} Starting fetch..."
echo -e "${CYAN}Days:${NC} $DAYS"
echo -e "${CYAN}Interval:${NC} $INTERVAL"

if [ -z "$RESUME" ]; then
    echo -e "${YELLOW}📊 This will take approximately:${NC}"

    if [ "$DAYS" -eq 365 ]; then
        echo -e "  - With 4h interval: ~15-20 minutes"
        echo -e "  - Total data: ~43,800 candles"
    elif [ "$DAYS" -eq 180 ]; then
        echo -e "  - With 4h interval: ~8-10 minutes"
        echo -e "  - Total data: ~21,600 candles"
    elif [ "$DAYS" -eq 90 ]; then
        echo -e "  - With 4h interval: ~5 minutes"
        echo -e "  - Total data: ~10,800 candles"
    fi

    echo -e "\n${YELLOW}Press Ctrl+C anytime to stop (progress will be saved)${NC}"
    sleep 3
fi

echo -e "\n${GREEN}${BOLD}🚀 Starting fetch...${NC}\n"

# Run the fetch
python3 fetch_historical_365days.py --days $DAYS --interval $INTERVAL $RESUME

# Check if successful
if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}${BOLD}✅ Fetch completed successfully!${NC}\n"

    # Ask if user wants to verify
    read -p "Do you want to verify data quality? (y/n): " verify

    if [ "$verify" = "y" ] || [ "$verify" = "Y" ]; then
        echo -e "\n${CYAN}🔍 Running data verification...${NC}\n"
        python3 verify_data_quality.py --days $DAYS --interval $INTERVAL --detailed
    fi

    echo -e "\n${GREEN}${BOLD}🎉 All done!${NC}"
    echo -e "\n${CYAN}Next steps:${NC}"
    echo -e "  1. Verify data: ${BOLD}python3 verify_data_quality.py --days $DAYS --interval $INTERVAL${NC}"
    echo -e "  2. Train model: ${BOLD}cd ../SA_Prediction && python -m training.train_xgboost --days $DAYS${NC}"
    echo -e "  3. Make predictions: ${BOLD}python -m inference.predict --all${NC}"
    echo -e ""
else
    echo -e "\n${RED}❌ Fetch failed or was interrupted${NC}"
    echo -e "${YELLOW}💡 Tip: Run with --resume to continue${NC}"
    echo -e "  ${BOLD}./quick_fetch_365days.sh${NC} and select option 5\n"
    exit 1
fi
