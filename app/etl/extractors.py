import pandas as pd
import os
from typing import List, Dict, Any, Optional
import logging
from config.etl_config import etl_config

logger = logging.getLogger(__name__)


class DataExtractor:
    """Базовый класс для извлечения данных из файлов"""

    def __init__(self):
        self.supported_formats = ['.csv', '.xlsx', '.xls']

    def list_available_files(self) -> List[str]:
        """Получить список доступных файлов для обработки"""
        files = []
        for file in os.listdir(etl_config.INPUT_DIR):
            if any(file.lower().endswith(fmt) for fmt in self.supported_formats):
                files.append(file)
        logger.info(f"Найдено файлов для обработки: {files}")
        return files

    def extract_from_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Извлечение данных из CSV файла"""
        try:
            df = pd.read_csv(file_path, **kwargs)
            logger.info(f"Успешно извлечено {len(df)} записей из CSV: {file_path}")
            return df
        except Exception as e:
            logger.error(f"Ошибка при чтении CSV файла {file_path}: {str(e)}")
            raise

    def extract_from_excel(self, file_path: str, sheet_name: str = 0, **kwargs) -> pd.DataFrame:
        """Извлечение данных из Excel файла"""
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
            logger.info(f"Успешно извлечено {len(df)} записей из Excel: {file_path}")
            return df
        except Exception as e:
            logger.error(f"Ошибка при чтении Excel файла {file_path}: {str(e)}")
            raise

    def extract_data(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Универсальный метод извлечения данных"""
        file_path = os.path.join(etl_config.INPUT_DIR, file_path)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")

        if file_path.lower().endswith('.csv'):
            return self.extract_from_csv(file_path, **kwargs)
        elif file_path.lower().endswith(('.xlsx', '.xls')):
            return self.extract_from_excel(file_path, **kwargs)
        else:
            raise ValueError(f"Неподдерживаемый формат файла: {file_path}")

    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Получить информацию о файле"""
        full_path = os.path.join(etl_config.INPUT_DIR, file_path)
        df = self.extract_data(file_path, nrows=5)  # Читаем только первые 5 строк для анализа

        return {
            'file_name': file_path,
            'columns': list(df.columns),
            'sample_data': df.head(3).to_dict('records'),
            'total_rows_preview': len(df)
        }


class PassengerDataExtractor(DataExtractor):
    """Специализированный экстрактор для данных пассажиров"""

    def extract_passengers_data(self, file_path: str) -> pd.DataFrame:
        """Извлечение данных пассажиров с базовой очисткой"""
        df = self.extract_data(file_path)

        # Базовая очистка названий колонок
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

        # Удаление полностью пустых строк
        df = df.dropna(how='all')

        logger.info(f"Извлечены данные пассажиров: {len(df)} записей, колонки: {list(df.columns)}")
        return df


class FlightDataExtractor(DataExtractor):
    """Специализированный экстрактор для данных рейсов"""

    def extract_flights_data(self, file_path: str, sheet_name: str = 'Flights') -> pd.DataFrame:
        """Извлечение данных рейсов"""
        df = self.extract_data(file_path, sheet_name=sheet_name)

        # Базовая очистка
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        df = df.dropna(how='all')

        logger.info(f"Извлечены данные рейсов: {len(df)} записей")
        return df

    def extract_fares_data(self, file_path: str, sheet_name: str = 'Fares') -> pd.DataFrame:
        """Извлечение данных тарифов"""
        df = self.extract_data(file_path, sheet_name=sheet_name)

        # Базовая очистка
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        df = df.dropna(how='all')

        logger.info(f"Извлечены данные тарифов: {len(df)} записей")
        return df