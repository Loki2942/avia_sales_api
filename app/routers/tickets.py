from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID
import logging

from app.database import get_db
from app import crud, schemas

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/", response_model=schemas.Ticket, status_code=status.HTTP_201_CREATED)
def create_ticket(ticket: schemas.TicketCreate, db: Session = Depends(get_db)):
    try:
        return crud.create_ticket(db=db, ticket=ticket)
    except Exception as e:
        logger.error(f"Error creating ticket: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not create ticket"
        )

@router.get("/", response_model=List[schemas.Ticket])
def read_tickets(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.get_tickets(db, skip=skip, limit=limit)

@router.get("/{ticket_id}", response_model=schemas.Ticket)
def read_ticket(ticket_id: UUID, db: Session = Depends(get_db)):
    db_ticket = crud.get_ticket(db, ticket_id=ticket_id)
    if db_ticket is None:
        logger.warning(f"Ticket with id {ticket_id} not found")
        raise HTTPException(status_code=404, detail="Ticket not found")
    return db_ticket

@router.put("/{ticket_id}", response_model=schemas.Ticket)
def update_ticket(ticket_id: UUID, ticket: schemas.TicketUpdate, db: Session = Depends(get_db)):
    db_ticket = crud.update_ticket(db, ticket_id=ticket_id, ticket_update=ticket)
    if db_ticket is None:
        raise HTTPException(status_code=404, detail="Ticket not found")
    return db_ticket

@router.delete("/{ticket_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_ticket(ticket_id: UUID, db: Session = Depends(get_db)):
    success = crud.delete_ticket(db, ticket_id=ticket_id)
    if not success:
        raise HTTPException(status_code=404, detail="Ticket not found")
    return None