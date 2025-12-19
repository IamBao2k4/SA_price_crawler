"""
Main entry point for Producer (Crawler)
"""
from src.config.settings import Settings
from src.producers.binance import BinanceDataSource
from src.storage.kafka_broker import KafkaMessageBroker
from src.consumers.crawler_service import CrawlerService
from src.utils.logger import setup_logger
from src.utils.validation import check_env_or_exit

def main():
    """Main function"""
    # Validate environment variables
    check_env_or_exit()

    # Setup
    settings = Settings()
    logger = setup_logger(__name__, settings.log_level)

    logger.info("Initializing Producer...")

    # Dependencies
    logger.info("Creating BinanceDataSource...")
    data_source = BinanceDataSource(settings.binance)
    logger.info("BinanceDataSource created")

    logger.info("Creating KafkaMessageBroker...")
    message_broker = KafkaMessageBroker(settings.kafka, mode='producer')
    logger.info("KafkaMessageBroker created")

    # Service
    logger.info("Creating CrawlerService...")
    service = CrawlerService(data_source, message_broker, settings)
    logger.info("CrawlerService created")

    # Run
    logger.info("Starting crawler service...")
    service.run(intervals=settings.active_intervals)


if __name__ == "__main__":
    main()
