from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID
import logging

from app.database import get_db
from app import crud, schemas

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/", response_model=schemas.Flight, status_code=status.HTTP_201_CREATED)
def create_flight(flight: schemas.FlightCreate, db: Session = Depends(get_db)):
    try:
        return crud.create_flight(db=db, flight=flight)
    except Exception as e:
        logger.error(f"Error creating flight: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create flight"
        )

@router.get("/", response_model=List[schemas.Flight])
def read_flights(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.get_flights(db, skip=skip, limit=limit)

@router.get("/{flight_id}", response_model=schemas.Flight)
def read_flight(flight_id: UUID, db: Session = Depends(get_db)):
    db_flight = crud.get_flight(db, flight_id=flight_id)
    if db_flight is None:
        logger.warning(f"Flight with id {flight_id} not found")
        raise HTTPException(status_code=404, detail="Flight not found")
    return db_flight

@router.put("/{flight_id}", response_model=schemas.Flight)
def update_flight(flight_id: UUID, flight: schemas.FlightUpdate, db: Session = Depends(get_db)):
    db_flight = crud.update_flight(db, flight_id=flight_id, flight_update=flight)
    if db_flight is None:
        raise HTTPException(status_code=404, detail="Flight not found")
    return db_flight

@router.delete("/{flight_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_flight(flight_id: UUID, db: Session = Depends(get_db)):
    success = crud.delete_flight(db, flight_id=flight_id)
    if not success:
        raise HTTPException(status_code=404, detail="Flight not found")
    return None