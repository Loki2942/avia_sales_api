from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
import logging

from app.database import get_db
from app import crud, schemas

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/", response_model=List[schemas.DocumentType])
def read_document_types(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.get_document_types(db, skip=skip, limit=limit)