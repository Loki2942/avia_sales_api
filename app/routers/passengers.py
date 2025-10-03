from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID
import logging

from app.database import get_db
from app import crud, schemas

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/", response_model=schemas.Passenger, status_code=status.HTTP_201_CREATED)
def create_passenger(passenger: schemas.PassengerCreate, db: Session = Depends(get_db)):
    try:
        return crud.create_passenger(db=db, passenger=passenger)
    except Exception as e:
        logger.error(f"Error creating passenger: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create passenger"
        )

@router.get("/", response_model=List[schemas.Passenger])
def read_passengers(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.get_passengers(db, skip=skip, limit=limit)

@router.get("/{passenger_id}", response_model=schemas.Passenger)
def read_passenger(passenger_id: UUID, db: Session = Depends(get_db)):
    db_passenger = crud.get_passenger(db, passenger_id=passenger_id)
    if db_passenger is None:
        logger.warning(f"Passenger with id {passenger_id} not found")
        raise HTTPException(status_code=404, detail="Passenger not found")
    return db_passenger

@router.put("/{passenger_id}", response_model=schemas.Passenger)
def update_passenger(passenger_id: UUID, passenger: schemas.PassengerUpdate, db: Session = Depends(get_db)):
    db_passenger = crud.update_passenger(db, passenger_id=passenger_id, passenger_update=passenger)
    if db_passenger is None:
        raise HTTPException(status_code=404, detail="Passenger not found")
    return db_passenger

@router.delete("/{passenger_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_passenger(passenger_id: UUID, db: Session = Depends(get_db)):
    success = crud.delete_passenger(db, passenger_id=passenger_id)
    if not success:
        raise HTTPException(status_code=404, detail="Passenger not found")
    return None