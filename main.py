from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import logging
import uvicorn

from app.database import engine, get_db
from app import models
from app.routers import (
    passengers, flights, bookings, fares,
    passenger_documents, tickets, payments,
    booking_statuses, document_types
)

# Create database tables
models.Base.metadata.create_all(bind=engine)

# Create FastAPI app
app = FastAPI(
    title="AviaSales API",
    description="REST API for AviaSales booking system",
    version="1.0.0"
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
        "message": "AviaSales API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc"
    }

# Health check endpoint
@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    try:
        # Try to execute a simple query to check database connection
        db.execute("SELECT 1")
        return {
            "status": "healthy",
            "database": "connected"
        }
    except Exception as e:
        logging.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Database connection failed")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )