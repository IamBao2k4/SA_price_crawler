"""
Main entry point for Consumer (Storage)
"""
from src.config.settings import Settings
from src.storage.kafka_broker import KafkaMessageBroker
from src.storage.mongodb_storage import MongoDBStorage
from src.consumers.storage_service import StorageService
from src.utils.logger import setup_logger


def main():
    """Main function"""
    # Setup
    settings = Settings()
    logger = setup_logger(__name__, settings.log_level)

    logger.info("Initializing Consumer...")

    # Dependencies
    message_broker = KafkaMessageBroker(settings.kafka, mode='consumer')
    storage = MongoDBStorage(settings.mongodb)

    # Service
    service = StorageService(message_broker, storage, settings)

    # Run
    service.run(intervals=settings.active_intervals)


if __name__ == "__main__":
    main()
