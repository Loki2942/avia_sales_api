import os
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class ETLConfig:
    # Пути к файлам
    INPUT_DIR: str = "data/input"
    OUTPUT_DIR: str = "data/output"
    PROCESSED_DIR: str = "data/processed"
    ERRORS_DIR: str = "data/errors"

    # Настройки обработки
    CHUNK_SIZE: int = 1000
    MAX_ERRORS: int = 100

    # Маппинги для трансформации
    DOCUMENT_TYPE_MAPPING: Dict[str, str] = None
    BOOKING_STATUS_MAPPING: Dict[str, str] = None
    FARE_CLASS_MAPPING: Dict[str, str] = None

    def __post_init__(self):
        if self.DOCUMENT_TYPE_MAPPING is None:
            self.DOCUMENT_TYPE_MAPPING = {
                'PASSPORT_RF': 'PASSPORT_RF',
                'INTERNATIONAL_PASSPORT': 'INTERNATIONAL_PASSPORT',
                'BIRTH_CERTIFICATE': 'BIRTH_CERTIFICATE',
                'MILITARY_ID': 'MILITARY_ID',
                'SEAMAN_ID': 'SEAMAN_ID',
                'PASSPORT': 'INTERNATIONAL_PASSPORT',  # Автоматическое преобразование
                'PASSPORT_INTERNATIONAL': 'INTERNATIONAL_PASSPORT'
            }

        if self.BOOKING_STATUS_MAPPING is None:
            self.BOOKING_STATUS_MAPPING = {
                'PENDING': 'PENDING',
                'CONFIRMED': 'CONFIRMED',
                'PAID': 'PAID',
                'CANCELLED': 'CANCELLED',
                'REFUNDED': 'REFUNDED',
                'EXPIRED': 'EXPIRED'
            }

        if self.FARE_CLASS_MAPPING is None:
            self.FARE_CLASS_MAPPING = {
                'ECONOMY': 'ECONOMY',
                'BUSINESS': 'BUSINESS',
                'FIRST': 'FIRST',
                'ECON': 'ECONOMY',
                'BUS': 'BUSINESS'
            }

    def ensure_directories(self):
        """Создает необходимые директории"""
        for directory in [self.INPUT_DIR, self.OUTPUT_DIR, self.PROCESSED_DIR, self.ERRORS_DIR]:
            os.makedirs(directory, exist_ok=True)


# Глобальная конфигурация
etl_config = ETLConfig()
etl_config.ensure_directories()