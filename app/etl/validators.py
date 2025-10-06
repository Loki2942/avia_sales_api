import pandas as pd
import re
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional
import logging
from uuid import UUID

logger = logging.getLogger(__name__)


class DataValidator:
    """Класс для валидации данных"""

    @staticmethod
    def validate_email(email: str) -> bool:
        """Валидация email"""
        if pd.isna(email):
            return True  # Email опциональный
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, str(email)))

    @staticmethod
    def validate_phone(phone: str) -> bool:
        """Валидация номера телефона"""
        if pd.isna(phone):
            return True  # Телефон опциональный для пассажира
        pattern = r'^[\+]?[0-9\s\-\(\)]{10,15}$'
        return bool(re.match(pattern, str(phone)))

    @staticmethod
    def validate_date(date_str: str, date_format: str = '%Y-%m-%d') -> bool:
        """Валидация даты"""
        if pd.isna(date_str):
            return True
        try:
            datetime.strptime(str(date_str), date_format)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def validate_future_date(date_str: str, date_format: str = '%Y-%m-%d') -> bool:
        """Проверка что дата в будущем (для expiry_date)"""
        if pd.isna(date_str):
            return True
        try:
            parsed_date = datetime.strptime(str(date_str), date_format).date()
            return parsed_date > date.today()
        except (ValueError, TypeError):
            return False

    @staticmethod
    def validate_uuid(uuid_str: str) -> bool:
        """Валидация UUID"""
        if pd.isna(uuid_str):
            return True
        try:
            UUID(str(uuid_str))
            return True
        except ValueError:
            return False

    @staticmethod
    def validate_required_fields(row: pd.Series, required_fields: List[str]) -> List[str]:
        """Проверка обязательных полей"""
        missing_fields = []
        for field in required_fields:
            if field not in row or pd.isna(row[field]) or str(row[field]).strip() == '':
                missing_fields.append(field)
        return missing_fields


class PassengerValidator(DataValidator):
    """Специализированный валидатор для данных пассажиров"""

    REQUIRED_FIELDS = ['first_name', 'last_name', 'date_of_birth', 'document_type', 'document_number',
                       'country_of_issue']

    def validate_passenger_row(self, row: pd.Series) -> Tuple[bool, List[str]]:
        """Валидация строки с данными пассажира"""
        errors = []

        # Проверка обязательных полей
        missing_fields = self.validate_required_fields(row, self.REQUIRED_FIELDS)
        if missing_fields:
            errors.append(f"Отсутствуют обязательные поля: {', '.join(missing_fields)}")

        # Валидация даты рождения
        if not self.validate_date(row.get('date_of_birth')):
            errors.append("Неверный формат даты рождения")

        # Валидация email
        if 'email' in row and not self.validate_email(row['email']):
            errors.append("Неверный формат email")

        # Валидация телефона
        if 'phone_number' in row and not self.validate_phone(row['phone_number']):
            errors.append("Неверный формат номера телефона")

        # Валидация даты истечения документа
        if 'expiry_date' in row and row['expiry_date'] and not self.validate_date(row['expiry_date']):
            errors.append("Неверный формат даты истечения документа")

        # Проверка что документ не просрочен
        if 'expiry_date' in row and row['expiry_date'] and not self.validate_future_date(row['expiry_date']):
            errors.append("Документ просрочен")

        return len(errors) == 0, errors


class FlightValidator(DataValidator):
    """Валидатор для данных рейсов"""

    REQUIRED_FIELDS = ['flight_number', 'departure_airport_code', 'arrival_airport_code',
                       'scheduled_departure', 'scheduled_arrival', 'total_seats']

    def validate_flight_row(self, row: pd.Series) -> Tuple[bool, List[str]]:
        """Валидация строки с данными рейса"""
        errors = []

        # Проверка обязательных полей
        missing_fields = self.validate_required_fields(row, self.REQUIRED_FIELDS)
        if missing_fields:
            errors.append(f"Отсутствуют обязательные поля: {', '.join(missing_fields)}")

        # Валидация кодов аэропортов (3 символа)
        if 'departure_airport_code' in row and len(str(row['departure_airport_code'])) != 3:
            errors.append("Код аэропорта вылета должен содержать 3 символа")

        if 'arrival_airport_code' in row and len(str(row['arrival_airport_code'])) != 3:
            errors.append("Код аэропорта прилета должен содержать 3 символа")

        # Валидация дат
        if not self.validate_date(row.get('scheduled_departure'), '%Y-%m-%d %H:%M:%S'):
            errors.append("Неверный формат даты вылета")

        if not self.validate_date(row.get('scheduled_arrival'), '%Y-%m-%d %H:%M:%S'):
            errors.append("Неверный формат даты прилета")

        # Проверка что дата прилета после даты вылета
        try:
            dep_time = datetime.strptime(str(row['scheduled_departure']), '%Y-%m-%d %H:%M:%S')
            arr_time = datetime.strptime(str(row['scheduled_arrival']), '%Y-%m-%d %H:%M:%S')
            if arr_time <= dep_time:
                errors.append("Время прилета должно быть после времени вылета")
        except (ValueError, TypeError):
            pass  # Ошибки уже будут в предыдущих проверках

        # Валидация количества мест
        try:
            seats = int(row['total_seats'])
            if seats <= 0:
                errors.append("Количество мест должно быть положительным числом")
        except (ValueError, TypeError):
            errors.append("Неверный формат количества мест")

        return len(errors) == 0, errors