from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID
import logging

from app.database import get_db
from app import crud, schemas

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/", response_model=schemas.PassengerDocument, status_code=status.HTTP_201_CREATED)
def create_passenger_document(document: schemas.PassengerDocumentCreate, db: Session = Depends(get_db)):
    try:
        return crud.create_passenger_document(db=db, document=document)
    except Exception as e:
        logger.error(f"Error creating passenger document: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create passenger document"
        )

@router.get("/", response_model=List[schemas.PassengerDocument])
def read_passenger_documents(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.get_passenger_documents(db, skip=skip, limit=limit)

@router.get("/{document_id}", response_model=schemas.PassengerDocument)
def read_passenger_document(document_id: UUID, db: Session = Depends(get_db)):
    db_document = crud.get_passenger_document(db, document_id=document_id)
    if db_document is None:
        logger.warning(f"Passenger document with id {document_id} not found")
        raise HTTPException(status_code=404, detail="Passenger document not found")
    return db_document

@router.put("/{document_id}", response_model=schemas.PassengerDocument)
def update_passenger_document(document_id: UUID, document: schemas.PassengerDocumentUpdate, db: Session = Depends(get_db)):
    db_document = crud.update_passenger_document(db, document_id=document_id, document_update=document)
    if db_document is None:
        raise HTTPException(status_code=404, detail="Passenger document not found")
    return db_document

@router.delete("/{document_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_passenger_document(document_id: UUID, db: Session = Depends(get_db)):
    success = crud.delete_passenger_document(db, document_id=document_id)
    if not success:
        raise HTTPException(status_code=404, detail="Passenger document not found")
    return None