"""
Main entry point for Consumer (Storage)
"""
from src.config.settings import Settings
from src.storage.kafka_broker import KafkaMessageBroker
from src.storage.mongodb_storage import MongoDBStorage
from src.storage.nats_publisher import NatsPublisher
from src.consumers.storage_service import StorageService
from src.utils.logger import setup_logger, setup_loguru_file_logging
from src.utils.validation import check_env_or_exit


def main():
    """Main function"""
    # Validate environment variables
    check_env_or_exit()

    # Setup
    settings = Settings()
    logger = setup_logger(__name__, settings.log_level)

    # setup_loguru_file_logging(log_dir="logs", level="DEBUG")

    logger.info("Initializing Consumer...")

    # Dependencies
    message_broker = KafkaMessageBroker(settings.kafka, mode='consumer')
    storage = MongoDBStorage(settings.mongodb)
    
    nats_publisher = NatsPublisher(settings.nats.url)
    # logger.info(f"NATS publisher initialized for: {settings.nats.url}")

    # Service
    service = StorageService(message_broker, storage, settings, nats_publisher)

    service.run(intervals=settings.active_intervals)


if __name__ == "__main__":
    main()
