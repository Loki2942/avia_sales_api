from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID
import logging

from app.database import get_db
from app import crud, schemas

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/", response_model=schemas.Fare, status_code=status.HTTP_201_CREATED)
def create_fare(fare: schemas.FareCreate, db: Session = Depends(get_db)):
    try:
        return crud.create_fare(db=db, fare=fare)
    except Exception as e:
        logger.error(f"Error creating fare: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create fare"
        )

@router.get("/", response_model=List[schemas.Fare])
def read_fares(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.get_fares(db, skip=skip, limit=limit)

@router.get("/{fare_id}", response_model=schemas.Fare)
def read_fare(fare_id: UUID, db: Session = Depends(get_db)):
    db_fare = crud.get_fare(db, fare_id=fare_id)
    if db_fare is None:
        logger.warning(f"Fare with id {fare_id} not found")
        raise HTTPException(status_code=404, detail="Fare not found")
    return db_fare

@router.put("/{fare_id}", response_model=schemas.Fare)
def update_fare(fare_id: UUID, fare: schemas.FareUpdate, db: Session = Depends(get_db)):
    db_fare = crud.update_fare(db, fare_id=fare_id, fare_update=fare)
    if db_fare is None:
        raise HTTPException(status_code=404, detail="Fare not found")
    return db_fare

@router.delete("/{fare_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_fare(fare_id: UUID, db: Session = Depends(get_db)):
    success = crud.delete_fare(db, fare_id=fare_id)
    if not success:
        raise HTTPException(status_code=404, detail="Fare not found")
    return None