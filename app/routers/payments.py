from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID
import logging

from app.database import get_db
from app import crud, schemas

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/", response_model=schemas.Payment, status_code=status.HTTP_201_CREATED)
def create_payment(payment: schemas.PaymentCreate, db: Session = Depends(get_db)):
    try:
        return crud.create_payment(db=db, payment=payment)
    except Exception as e:
        logger.error(f"Error creating payment: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create payment"
        )

@router.get("/", response_model=List[schemas.Payment])
def read_payments(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.get_payments(db, skip=skip, limit=limit)

@router.get("/{payment_id}", response_model=schemas.Payment)
def read_payment(payment_id: UUID, db: Session = Depends(get_db)):
    db_payment = crud.get_payment(db, payment_id=payment_id)
    if db_payment is None:
        logger.warning(f"Payment with id {payment_id} not found")
        raise HTTPException(status_code=404, detail="Payment not found")
    return db_payment

@router.put("/{payment_id}", response_model=schemas.Payment)
def update_payment(payment_id: UUID, payment: schemas.PaymentUpdate, db: Session = Depends(get_db)):
    db_payment = crud.update_payment(db, payment_id=payment_id, payment_update=payment)
    if db_payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return db_payment

@router.delete("/{payment_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_payment(payment_id: UUID, db: Session = Depends(get_db)):
    success = crud.delete_payment(db, payment_id=payment_id)
    if not success:
        raise HTTPException(status_code=404, detail="Payment not found")
    return None