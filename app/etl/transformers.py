import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Any
import logging
from datetime import datetime
import uuid

from app.etl.validators import PassengerValidator, FlightValidator
from config.etl_config import etl_config

logger = logging.getLogger(__name__)


class DataTransformer:
    """Базовый класс для трансформации данных"""

    def __init__(self):
        self.validator = None

    def clean_text(self, text: str) -> str:
        """Очистка текстовых данных"""
        if pd.isna(text):
            return ''
        return str(text).strip()

    def generate_uuid(self) -> str:
        """Генерация UUID"""
        return str(uuid.uuid4())


class PassengerDataTransformer(DataTransformer):
    """Трансформатор данных пассажиров"""

    def __init__(self):
        super().__init__()
        self.validator = PassengerValidator()

    def transform_passenger_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Трансформация данных пассажиров
        Возвращает: (валидные данные, данные с ошибками)
        """
        valid_rows = []
        error_rows = []

        for index, row in df.iterrows():
            # Очистка текстовых полей
            cleaned_row = row.copy()
            for col in ['first_name', 'last_name', 'email', 'phone_number',
                        'document_type', 'document_number', 'country_of_issue']:
                if col in cleaned_row:
                    cleaned_row[col] = self.clean_text(cleaned_row[col])

            # Валидация
            is_valid, errors = self.validator.validate_passenger_row(cleaned_row)

            if is_valid:
                # Трансформация валидных данных
                transformed_row = self._transform_valid_passenger(cleaned_row)
                valid_rows.append(transformed_row)
            else:
                # Сохранение ошибок
                error_row = cleaned_row.to_dict()
                error_row['_errors'] = errors
                error_row['_original_index'] = index
                error_rows.append(error_row)

        valid_df = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame()
        errors_df = pd.DataFrame(error_rows) if error_rows else pd.DataFrame()

        logger.info(f"Трансформация пассажиров завершена: {len(valid_df)} валидных, {len(errors_df)} с ошибками")
        return valid_df, errors_df

    def _transform_valid_passenger(self, row: pd.Series) -> Dict[str, Any]:
        """Трансформация валидной строки пассажира"""
        transformed = {}

        # Генерация ID если не указан
        if 'passenger_id' in row and not pd.isna(row['passenger_id']):
            transformed['passenger_id'] = row['passenger_id']
        else:
            transformed['passenger_id'] = self.generate_uuid()

        # Базовые поля
        transformed['first_name'] = row['first_name']
        transformed['last_name'] = row['last_name']
        transformed['date_of_birth'] = row['date_of_birth']
        transformed['email'] = row.get('email')
        transformed['phone_number'] = row.get('phone_number')

        # Трансформация типа документа
        doc_type = self.clean_text(row.get('document_type', ''))
        transformed['document_type'] = etl_config.DOCUMENT_TYPE_MAPPING.get(
            doc_type.upper(), doc_type.upper()
        )

        # Данные документа
        transformed['document_number'] = row['document_number']
        transformed['expiry_date'] = row.get('expiry_date')
        transformed['country_of_issue'] = row['country_of_issue']

        # Дополнительные поля для связывания
        if 'flight_number' in row:
            transformed['flight_number'] = row['flight_number']
        if 'fare_class' in row:
            transformed['fare_class'] = etl_config.FARE_CLASS_MAPPING.get(
                row['fare_class'].upper(), row['fare_class'].upper()
            )
        if 'booking_status' in row:
            transformed['booking_status'] = etl_config.BOOKING_STATUS_MAPPING.get(
                row['booking_status'].upper(), row['booking_status'].upper()
            )

        return transformed


class FlightDataTransformer(DataTransformer):
    """Трансформатор данных рейсов"""

    def __init__(self):
        super().__init__()
        self.validator = FlightValidator()

    def transform_flight_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Трансформация данных рейсов"""
        valid_rows = []
        error_rows = []

        for index, row in df.iterrows():
            # Очистка данных
            cleaned_row = row.copy()
            for col in ['flight_number', 'departure_airport_code', 'arrival_airport_code', 'aircraft_type']:
                if col in cleaned_row:
                    cleaned_row[col] = self.clean_text(cleaned_row[col])

            # Валидация
            is_valid, errors = self.validator.validate_flight_row(cleaned_row)

            if is_valid:
                transformed_row = self._transform_valid_flight(cleaned_row)
                valid_rows.append(transformed_row)
            else:
                error_row = cleaned_row.to_dict()
                error_row['_errors'] = errors
                error_row['_original_index'] = index
                error_rows.append(error_row)

        valid_df = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame()
        errors_df = pd.DataFrame(error_rows) if error_rows else pd.DataFrame()

        logger.info(f"Трансформация рейсов завершена: {len(valid_df)} валидных, {len(errors_df)} с ошибками")
        return valid_df, errors_df

    def _transform_valid_flight(self, row: pd.Series) -> Dict[str, Any]:
        """Трансформация валидной строки рейса"""
        transformed = {}

        # Генерация ID
        transformed['flight_id'] = self.generate_uuid()

        # Базовые поля
        transformed['flight_number'] = row['flight_number'].upper()
        transformed['departure_airport_code'] = row['departure_airport_code'].upper()
        transformed['arrival_airport_code'] = row['arrival_airport_code'].upper()
        transformed['scheduled_departure'] = row['scheduled_departure']
        transformed['scheduled_arrival'] = row['scheduled_arrival']
        transformed['aircraft_type'] = row.get('aircraft_type')
        transformed['total_seats'] = int(row['total_seats'])

        return transformed


class FareDataTransformer(DataTransformer):
    """Трансформатор данных тарифов"""

    def transform_fare_data(self, df: pd.DataFrame, flight_mapping: Dict[str, str]) -> Tuple[
        pd.DataFrame, pd.DataFrame]:
        """Трансформация данных тарифов"""
        valid_rows = []
        error_rows = []

        for index, row in df.iterrows():
            cleaned_row = row.copy()

            # Очистка
            for col in ['fare_class', 'fare_conditions']:
                if col in cleaned_row:
                    cleaned_row[col] = self.clean_text(cleaned_row[col])

            errors = self._validate_fare_row(cleaned_row, flight_mapping)

            if not errors:
                transformed_row = self._transform_valid_fare(cleaned_row, flight_mapping)
                valid_rows.append(transformed_row)
            else:
                error_row = cleaned_row.to_dict()
                error_row['_errors'] = errors
                error_row['_original_index'] = index
                error_rows.append(error_row)

        valid_df = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame()
        errors_df = pd.DataFrame(error_rows) if error_rows else pd.DataFrame()

        logger.info(f"Трансформация тарифов завершена: {len(valid_df)} валидных, {len(errors_df)} с ошибками")
        return valid_df, errors_df

    def _validate_fare_row(self, row: pd.Series, flight_mapping: Dict[str, str]) -> List[str]:
        """Валидация строки тарифа"""
        errors = []

        required_fields = ['flight_number', 'fare_class', 'price', 'available_seats']
        for field in required_fields:
            if field not in row or pd.isna(row[field]):
                errors.append(f"Отсутствует обязательное поле: {field}")

        # Проверка существования рейса
        flight_number = self.clean_text(row.get('flight_number', ''))
        if flight_number and flight_number not in flight_mapping:
            errors.append(f"Рейс не найден: {flight_number}")

        # Проверка цены
        try:
            price = float(row.get('price', 0))
            if price < 0:
                errors.append("Цена не может быть отрицательной")
        except (ValueError, TypeError):
            errors.append("Неверный формат цены")

        # Проверка количества мест
        try:
            seats = int(row.get('available_seats', 0))
            if seats < 0:
                errors.append("Количество мест не может быть отрицательным")
        except (ValueError, TypeError):
            errors.append("Неверный формат количества мест")

        return errors

    def _transform_valid_fare(self, row: pd.Series, flight_mapping: Dict[str, str]) -> Dict[str, Any]:
        """Трансформация валидной строки тарифа"""
        transformed = {}

        transformed['fare_id'] = self.generate_uuid()
        transformed['flight_id'] = flight_mapping[self.clean_text(row['flight_number']).upper()]
        transformed['fare_class'] = etl_config.FARE_CLASS_MAPPING.get(
            row['fare_class'].upper(), row['fare_class'].upper()
        )
        transformed['price'] = float(row['price'])
        transformed['fare_conditions'] = row.get('fare_conditions')
        transformed['available_seats'] = int(row['available_seats'])

        return transformed