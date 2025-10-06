import pandas as pd
import os
import json
from typing import Dict, List, Any, Tuple
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

from sqlalchemy.orm import Session
from app.database import get_db
from app import crud, schemas, models
from config.etl_config import etl_config

logger = logging.getLogger(__name__)


class DataLoader:
    """Базовый класс для загрузки данных"""

    def __init__(self, db: Session):
        self.db = db

    def save_errors_report(self, errors_df: pd.DataFrame, source_file: str, process_type: str):
        """Сохранение отчета об ошибках"""
        if errors_df.empty:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = f"{process_type}_errors_{timestamp}.csv"
        error_path = os.path.join(etl_config.ERRORS_DIR, error_file)

        # Сохраняем ошибки в CSV
        errors_df.to_csv(error_path, index=False, encoding='utf-8')

        # Сохраняем сводку в JSON
        summary = {
            'source_file': source_file,
            'process_type': process_type,
            'timestamp': timestamp,
            'total_errors': len(errors_df),
            'error_categories': self._categorize_errors(errors_df)
        }

        summary_file = f"{process_type}_summary_{timestamp}.json"
        summary_path = os.path.join(etl_config.ERRORS_DIR, summary_file)

        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        logger.info(f"Отчет об ошибках сохранен: {error_path}")

    def _categorize_errors(self, errors_df: pd.DataFrame) -> Dict[str, int]:
        """Категоризация ошибок"""
        categories = {}
        for errors in errors_df['_errors']:
            for error in errors:
                category = error.split(':')[0] if ':' in error else 'Общие ошибки'
                categories[category] = categories.get(category, 0) + 1
        return categories


class PassengerDataLoader(DataLoader):
    """Загрузчик данных пассажиров"""

    def load_passenger_data(self, valid_df: pd.DataFrame, source_file: str) -> Dict[str, Any]:
        """Загрузка данных пассажиров в БД"""
        stats = {
            'total_processed': len(valid_df),
            'passengers_created': 0,
            'documents_created': 0,
            'errors': []
        }

        # Получаем маппинг типов документов из БД
        doc_types = self.db.query(models.Dictionary_DocumentType).all()
        doc_type_mapping = {dt.type_code: dt.document_type_id for dt in doc_types}

        for _, row in valid_df.iterrows():
            try:
                # Создаем пассажира
                passenger_data = schemas.PassengerCreate(
                    first_name=row['first_name'],
                    last_name=row['last_name'],
                    date_of_birth=row['date_of_birth'],
                    email=row.get('email'),
                    phone_number=row.get('phone_number')
                )

                passenger = crud.create_passenger(self.db, passenger_data)
                stats['passengers_created'] += 1

                # Создаем документ пассажира
                document_type_id = doc_type_mapping.get(row['document_type'])
                if not document_type_id:
                    stats['errors'].append(f"Неизвестный тип документа: {row['document_type']}")
                    continue

                document_data = schemas.PassengerDocumentCreate(
                    passenger_id=passenger.passenger_id,
                    document_type_id=document_type_id,
                    document_number=row['document_number'],
                    expiry_date=row.get('expiry_date'),
                    country_of_issue=row['country_of_issue']
                )

                crud.create_passenger_document(self.db, document_data)
                stats['documents_created'] += 1

            except Exception as e:
                stats['errors'].append(f"Ошибка при создании пассажира {row.get('first_name', '')}: {str(e)}")

        self.db.commit()
        logger.info(f"Загрузка пассажиров завершена: {stats}")
        return stats


class FlightDataLoader(DataLoader):
    """Загрузчик данных рейсов"""

    def load_flight_data(self, valid_df: pd.DataFrame, source_file: str) -> Dict[str, Any]:
        """Загрузка данных рейсов в БД"""
        stats = {
            'total_processed': len(valid_df),
            'flights_created': 0,
            'errors': []
        }

        flight_mapping = {}  # flight_number -> flight_id

        for _, row in valid_df.iterrows():
            try:
                flight_data = schemas.FlightCreate(
                    flight_number=row['flight_number'],
                    departure_airport_code=row['departure_airport_code'],
                    arrival_airport_code=row['arrival_airport_code'],
                    scheduled_departure=row['scheduled_departure'],
                    scheduled_arrival=row['scheduled_arrival'],
                    aircraft_type=row.get('aircraft_type'),
                    total_seats=row['total_seats']
                )

                flight = crud.create_flight(self.db, flight_data)
                flight_mapping[row['flight_number']] = flight.flight_id
                stats['flights_created'] += 1

            except Exception as e:
                stats['errors'].append(f"Ошибка при создании рейса {row.get('flight_number', '')}: {str(e)}")

        self.db.commit()
        logger.info(f"Загрузка рейсов завершена: {stats}")
        return stats, flight_mapping


class FareDataLoader(DataLoader):
    """Загрузчик данных тарифов"""

    def load_fare_data(self, valid_df: pd.DataFrame, flight_mapping: Dict[str, str], source_file: str) -> Dict[
        str, Any]:
        """Загрузка данных тарифов в БД"""
        stats = {
            'total_processed': len(valid_df),
            'fares_created': 0,
            'errors': []
        }

        for _, row in valid_df.iterrows():
            try:
                fare_data = schemas.FareCreate(
                    flight_id=row['flight_id'],
                    fare_class=row['fare_class'],
                    price=row['price'],
                    fare_conditions=row.get('fare_conditions'),
                    available_seats=row['available_seats']
                )

                crud.create_fare(self.db, fare_data)
                stats['fares_created'] += 1

            except Exception as e:
                stats['errors'].append(f"Ошибка при создании тарифа: {str(e)}")

        self.db.commit()
        logger.info(f"Загрузка тарифов завершена: {stats}")
        return stats


class VisualizationEngine:
    """Движок для визуализации результатов ETL"""

    @staticmethod
    def create_etl_dashboard(stats: Dict[str, Any], output_path: str):
        """Создание дашборда с результатами ETL"""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            fig.suptitle('ETL Process Dashboard', fontsize=16)

            # 1. Общая статистика
            success_rates = []
            labels = []
            for process, data in stats.items():
                if 'total_processed' in data and data['total_processed'] > 0:
                    success_rate = (data.get('passengers_created', 0) +
                                    data.get('flights_created', 0) +
                                    data.get('fares_created', 0)) / data['total_processed'] * 100
                    success_rates.append(success_rate)
                    labels.append(process)

            axes[0, 0].bar(labels, success_rates, color=['green', 'blue', 'orange'])
            axes[0, 0].set_title('Success Rate by Process')
            axes[0, 0].set_ylabel('Success Rate (%)')

            # 2. Количество обработанных записей
            processed_data = [data.get('total_processed', 0) for data in stats.values()]
            axes[0, 1].pie(processed_data, labels=stats.keys(), autopct='%1.1f%%')
            axes[0, 1].set_title('Records Distribution')

            # 3. Ошибки по типам
            error_counts = [len(data.get('errors', [])) for data in stats.values()]
            axes[1, 0].bar(stats.keys(), error_counts, color='red')
            axes[1, 0].set_title('Errors by Process')
            axes[1, 0].set_ylabel('Error Count')

            # 4. Временная шкала (заглушка)
            axes[1, 1].text(0.5, 0.5, 'Timeline Visualization\n(Would show processing timeline)',
                            ha='center', va='center', transform=axes[1, 1].transAxes)
            axes[1, 1].set_title('Processing Timeline')

            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()

            logger.info(f"Дашборд сохранен: {output_path}")

        except Exception as e:
            logger.error(f"Ошибка при создании дашборда: {str(e)}")