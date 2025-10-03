from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID
import logging

from app.database import get_db
from app import crud, schemas

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/", response_model=schemas.Booking, status_code=status.HTTP_201_CREATED)
def create_booking(booking: schemas.BookingCreate, db: Session = Depends(get_db)):
    try:
        return crud.create_booking(db=db, booking=booking)
    except Exception as e:
        logger.error(f"Error creating booking: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create booking"
        )

@router.get("/", response_model=List[schemas.Booking])
def read_bookings(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.get_bookings(db, skip=skip, limit=limit)

@router.get("/{booking_id}", response_model=schemas.Booking)
def read_booking(booking_id: UUID, db: Session = Depends(get_db)):
    db_booking = crud.get_booking(db, booking_id=booking_id)
    if db_booking is None:
        logger.warning(f"Booking with id {booking_id} not found")
        raise HTTPException(status_code=404, detail="Booking not found")
    return db_booking

@router.put("/{booking_id}", response_model=schemas.Booking)
def update_booking(booking_id: UUID, booking: schemas.BookingUpdate, db: Session = Depends(get_db)):
    db_booking = crud.update_booking(db, booking_id=booking_id, booking_update=booking)
    if db_booking is None:
        raise HTTPException(status_code=404, detail="Booking not found")
    return db_booking

@router.delete("/{booking_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_booking(booking_id: UUID, db: Session = Depends(get_db)):
    success = crud.delete_booking(db, booking_id=booking_id)
    if not success:
        raise HTTPException(status_code=404, detail="Booking not found")
    return None