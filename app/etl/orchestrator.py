import os
import sys
import logging
from datetime import datetime
import json
from typing import Dict, Any

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.etl.extractors import PassengerDataExtractor, FlightDataExtractor
from app.etl.transformers import PassengerDataTransformer, FlightDataTransformer, FareDataTransformer
from app.etl.loaders import PassengerDataLoader, FlightDataLoader, FareDataLoader, VisualizationEngine
from config.etl_config import etl_config

logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """Оркестратор ETL-процессов"""

    def __init__(self, db: Session):
        self.db = db
        self.stats = {}

        # Инициализация компонентов
        self.extractors = {
            'passengers': PassengerDataExtractor(),
            'flights': FlightDataExtractor()
        }

        self.transformers = {
            'passengers': PassengerDataTransformer(),
            'flights': FlightDataTransformer(),
            'fares': FareDataTransformer()
        }

        self.loaders = {
            'passengers': PassengerDataLoader(db),
            'flights': FlightDataLoader(db),
            'fares': FareDataLoader(db)
        }

    def process_passengers_file(self, file_path: str):
        """Обработка файла с данными пассажиров"""
        logger.info(f"Начало обработки файла пассажиров: {file_path}")

        try:
            # Extract
            extractor = self.extractors['passengers']
            raw_data = extractor.extract_passengers_data(file_path)

            # Transform
            transformer = self.transformers['passengers']
            valid_data, errors_data = transformer.transform_passenger_data(raw_data)

            # Load
            loader = self.loaders['passengers']
            load_stats = loader.load_passenger_data(valid_data, file_path)

            # Сохранение ошибок
            if not errors_data.empty:
                loader.save_errors_report(errors_data, file_path, 'passengers')

            # Статистика
            self.stats['passengers'] = {
                'total_processed': len(raw_data),
                'valid_records': len(valid_data),
                'error_records': len(errors_data),
                **load_stats
            }

            logger.info(f"Обработка пассажиров завершена: {len(valid_data)} успешно, {len(errors_data)} с ошибками")

        except Exception as e:
            logger.error(f"Ошибка при обработке файла пассажиров {file_path}: {str(e)}")
            self.stats['passengers'] = {'error': str(e)}

    def process_flights_file(self, file_path: str):
        """Обработка файла с данными рейсов и тарифов"""
        logger.info(f"Начало обработки файла рейсов: {file_path}")

        try:
            extractor = self.extractors['flights']

            # Обработка рейсов
            raw_flights = extractor.extract_flights_data(file_path, 'Flights')
            transformer = self.transformers['flights']
            valid_flights, errors_flights = transformer.transform_flight_data(raw_flights)

            loader = self.loaders['flights']
            load_stats_flights, flight_mapping = loader.load_flight_data(valid_flights, file_path)

            if not errors_flights.empty:
                loader.save_errors_report(errors_flights, file_path, 'flights')

            # Обработка тарифов
            raw_fares = extractor.extract_fares_data(file_path, 'Fares')
            transformer_fares = self.transformers['fares']
            valid_fares, errors_fares = transformer_fares.transform_fare_data(raw_fares, flight_mapping)

            loader_fares = self.loaders['fares']
            load_stats_fares = loader_fares.load_fare_data(valid_fares, flight_mapping, file_path)

            if not errors_fares.empty:
                loader_fares.save_errors_report(errors_fares, file_path, 'fares')

            # Статистика
            self.stats['flights'] = {
                'total_processed': len(raw_flights),
                'valid_records': len(valid_flights),
                'error_records': len(errors_flights),
                **load_stats_flights
            }

            self.stats['fares'] = {
                'total_processed': len(raw_fares),
                'valid_records': len(valid_fares),
                'error_records': len(errors_fares),
                **load_stats_fares
            }

            logger.info(f"Обработка рейсов завершена: {len(valid_flights)} рейсов, {len(valid_fares)} тарифов")

        except Exception as e:
            logger.error(f"Ошибка при обработке файла рейсов {file_path}: {str(e)}")
            self.stats['flights'] = {'error': str(e)}
            self.stats['fares'] = {'error': str(e)}

    def run_etl_pipeline(self):
        """Запуск полного ETL-конвейера"""
        logger.info("Запуск ETL-конвейера")

        # Получаем список файлов
        extractor = self.extractors['passengers']
        files = extractor.list_available_files()

        if not files:
            logger.warning("Нет файлов для обработки")
            return

        # Обрабатываем каждый файл
        for file in files:
            logger.info(f"Обработка файла: {file}")

            if 'passenger' in file.lower():
                self.process_passengers_file(file)
            elif 'flight' in file.lower():
                self.process_flights_file(file)
            else:
                logger.warning(f"Неизвестный тип файла: {file}")

        # Визуализация результатов
        self._create_visualizations()

        # Сохранение итогового отчета
        self._save_final_report()

        logger.info("ETL-конвейер завершен")

    def _create_visualizations(self):
        """Создание визуализаций"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dashboard_path = os.path.join(etl_config.OUTPUT_DIR, f"etl_dashboard_{timestamp}.png")

            VisualizationEngine.create_etl_dashboard(self.stats, dashboard_path)

        except Exception as e:
            logger.error(f"Ошибка при создании визуализаций: {str(e)}")

    def _save_final_report(self):
        """Сохранение итогового отчета"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = os.path.join(etl_config.OUTPUT_DIR, f"etl_report_{timestamp}.json")

            report = {
                'timestamp': timestamp,
                'statistics': self.stats,
                'summary': self._generate_summary()
            }

            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)

            logger.info(f"Итоговый отчет сохранен: {report_path}")

        except Exception as e:
            logger.error(f"Ошибка при сохранении отчета: {str(e)}")

    def _generate_summary(self) -> Dict[str, Any]:
        """Генерация сводки по процессу"""
        total_processed = 0
        total_valid = 0
        total_errors = 0

        for process_stats in self.stats.values():
            total_processed += process_stats.get('total_processed', 0)
            total_valid += process_stats.get('valid_records', 0)
            total_errors += process_stats.get('error_records', 0)

        success_rate = (total_valid / total_processed * 100) if total_processed > 0 else 0

        return {
            'total_processed': total_processed,
            'total_valid': total_valid,
            'total_errors': total_errors,
            'success_rate': round(success_rate, 2)
        }