from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.orm import Session
import logging
import uvicorn
import os
import uuid
from typing import Dict, Any

from app.database import engine, get_db, SessionLocal
from app import models
from app.routers import (
    passengers, flights, bookings, fares,
    passenger_documents, tickets, payments,
    booking_statuses, document_types
)
from app.etl import ETLOrchestrator

# Настройка логгера для main.py
logger = logging.getLogger(__name__)

# Create database tables
models.Base.metadata.create_all(bind=engine)

# Create FastAPI app
app = FastAPI(
    title="AviaSales API",
    description="REST API for AviaSales booking system with ETL capabilities",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(passengers.router, prefix="/api/passengers", tags=["passengers"])
app.include_router(flights.router, prefix="/api/flights", tags=["flights"])
app.include_router(bookings.router, prefix="/api/bookings", tags=["bookings"])
app.include_router(fares.router, prefix="/api/fares", tags=["fares"])
app.include_router(passenger_documents.router, prefix="/api/passenger-documents", tags=["passenger-documents"])
app.include_router(tickets.router, prefix="/api/tickets", tags=["tickets"])
app.include_router(payments.router, prefix="/api/payments", tags=["payments"])
app.include_router(booking_statuses.router, prefix="/api/booking-statuses", tags=["booking-statuses"])
app.include_router(document_types.router, prefix="/api/document-types", tags=["document-types"])


# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "AviaSales API with ETL",
        "version": "2.0.0",
        "docs": "/docs",
        "redoc": "/redoc"
    }


# Health check endpoint
@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    try:
        # Try to execute a simple query to check database connection
        db.execute(text('SELECT 1'))
        return {
            "status": "healthy",
            "database": "connected"
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Database connection failed")


# ETL endpoints
@app.post("/api/etl/upload-file")
async def upload_file(
        background_tasks: BackgroundTasks,
        file: UploadFile = File(...),
        file_type: str = "auto"
):
    """Эндпоинт для загрузки файла и запуска ETL"""
    try:
        # Проверяем расширение файла
        allowed_extensions = ['.csv', '.xlsx', '.xls']
        file_extension = os.path.splitext(file.filename)[1].lower()

        if file_extension not in allowed_extensions:
            raise HTTPException(
                status_code=400,
                detail=f"Неподдерживаемый формат файла. Разрешены: {', '.join(allowed_extensions)}"
            )

        # Создаем директорию input если не существует
        os.makedirs("data/input", exist_ok=True)

        # Сохраняем файл
        file_id = str(uuid.uuid4())
        file_path = os.path.join("data/input", f"{file_id}_{file.filename}")

        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)

        # Запускаем ETL в фоне
        background_tasks.add_task(process_uploaded_file, file_path, file_type)

        return {
            "message": "Файл загружен и находится в обработке",
            "file_id": file_id,
            "filename": file.filename
        }

    except Exception as e:
        logger.error(f"Ошибка загрузки файла: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки файла: {str(e)}")


@app.get("/api/etl/status")
async def get_etl_status():
    """Получить статус ETL-процессов"""
    return {"status": "running", "message": "ETL system is operational"}


def process_uploaded_file(file_path: str, file_type: str):
    """Фоновая задача обработки файла"""
    db = SessionLocal()
    try:
        orchestrator = ETLOrchestrator(db)

        if file_type == "passengers" or "passenger" in file_path.lower():
            orchestrator.process_passengers_file(file_path)
        elif file_type == "flights" or "flight" in file_path.lower():
            orchestrator.process_flights_file(file_path)
        else:
            # Автоопределение
            if "passenger" in file_path.lower():
                orchestrator.process_passengers_file(file_path)
            elif "flight" in file_path.lower():
                orchestrator.process_flights_file(file_path)

    except Exception as e:
        # Используем логгер, объявленный в начале файла
        logger.error(f"Ошибка обработки файла {file_path}: {str(e)}")
    finally:
        db.close()


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        log_level="info"
    )