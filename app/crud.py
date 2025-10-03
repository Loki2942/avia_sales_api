from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from typing import List, Optional
from uuid import UUID
import logging
from app import models, schemas

logger = logging.getLogger(__name__)

# Passenger CRUD
def get_passenger(db: Session, passenger_id: UUID) -> Optional[models.Passenger]:
    logger.info(f"Fetching passenger with ID: {passenger_id}")
    return db.query(models.Passenger).filter(models.Passenger.passenger_id == passenger_id).first()

def get_passengers(db: Session, skip: int = 0, limit: int = 100) -> List[models.Passenger]:
    logger.info(f"Fetching passengers with skip: {skip}, limit: {limit}")
    return db.query(models.Passenger).offset(skip).limit(limit).all()

def create_passenger(db: Session, passenger: schemas.PassengerCreate) -> models.Passenger:
    logger.info(f"Creating passenger: {passenger.first_name} {passenger.last_name}")
    db_passenger = models.Passenger(**passenger.dict())
    db.add(db_passenger)
    db.commit()
    db.refresh(db_passenger)
    logger.info(f"Passenger created with ID: {db_passenger.passenger_id}")
    return db_passenger

def update_passenger(db: Session, passenger_id: UUID, passenger_update: schemas.PassengerUpdate) -> Optional[models.Passenger]:
    logger.info(f"Updating passenger with ID: {passenger_id}")
    db_passenger = get_passenger(db, passenger_id)
    if db_passenger:
        update_data = passenger_update.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_passenger, field, value)
        db.commit()
        db.refresh(db_passenger)
        logger.info(f"Passenger with ID {passenger_id} updated successfully")
    else:
        logger.warning(f"Passenger with ID {passenger_id} not found for update")
    return db_passenger

def delete_passenger(db: Session, passenger_id: UUID) -> bool:
    logger.info(f"Deleting passenger with ID: {passenger_id}")
    db_passenger = get_passenger(db, passenger_id)
    if db_passenger:
        db.delete(db_passenger)
        db.commit()
        logger.info(f"Passenger with ID {passenger_id} deleted successfully")
        return True
    logger.warning(f"Passenger with ID {passenger_id} not found for deletion")
    return False

# Flight CRUD
def get_flight(db: Session, flight_id: UUID) -> Optional[models.Flight]:
    logger.info(f"Fetching flight with ID: {flight_id}")
    return db.query(models.Flight).filter(models.Flight.flight_id == flight_id).first()

def get_flights(db: Session, skip: int = 0, limit: int = 100) -> List[models.Flight]:
    logger.info(f"Fetching flights with skip: {skip}, limit: {limit}")
    return db.query(models.Flight).offset(skip).limit(limit).all()

def create_flight(db: Session, flight: schemas.FlightCreate) -> models.Flight:
    logger.info(f"Creating flight: {flight.flight_number}")
    db_flight = models.Flight(**flight.dict())
    db.add(db_flight)
    db.commit()
    db.refresh(db_flight)
    logger.info(f"Flight created with ID: {db_flight.flight_id}")
    return db_flight

def update_flight(db: Session, flight_id: UUID, flight_update: schemas.FlightUpdate) -> Optional[models.Flight]:
    logger.info(f"Updating flight with ID: {flight_id}")
    db_flight = get_flight(db, flight_id)
    if db_flight:
        update_data = flight_update.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_flight, field, value)
        db.commit()
        db.refresh(db_flight)
        logger.info(f"Flight with ID {flight_id} updated successfully")
    else:
        logger.warning(f"Flight with ID {flight_id} not found for update")
    return db_flight

def delete_flight(db: Session, flight_id: UUID) -> bool:
    logger.info(f"Deleting flight with ID: {flight_id}")
    db_flight = get_flight(db, flight_id)
    if db_flight:
        db.delete(db_flight)
        db.commit()
        logger.info(f"Flight with ID {flight_id} deleted successfully")
        return True
    logger.warning(f"Flight with ID {flight_id} not found for deletion")
    return False

# Booking CRUD
def get_booking(db: Session, booking_id: UUID) -> Optional[models.Booking]:
    logger.info(f"Fetching booking with ID: {booking_id}")
    return db.query(models.Booking).filter(models.Booking.booking_id == booking_id).first()

def get_bookings(db: Session, skip: int = 0, limit: int = 100) -> List[models.Booking]:
    logger.info(f"Fetching bookings with skip: {skip}, limit: {limit}")
    return db.query(models.Booking).offset(skip).limit(limit).all()

def create_booking(db: Session, booking: schemas.BookingCreate) -> models.Booking:
    logger.info(f"Creating booking for contact: {booking.contact_email}")
    db_booking = models.Booking(**booking.dict())
    db.add(db_booking)
    db.commit()
    db.refresh(db_booking)
    logger.info(f"Booking created with ID: {db_booking.booking_id}")
    return db_booking

def update_booking(db: Session, booking_id: UUID, booking_update: schemas.BookingUpdate) -> Optional[models.Booking]:
    logger.info(f"Updating booking with ID: {booking_id}")
    db_booking = get_booking(db, booking_id)
    if db_booking:
        update_data = booking_update.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_booking, field, value)
        db.commit()
        db.refresh(db_booking)
        logger.info(f"Booking with ID {booking_id} updated successfully")
    else:
        logger.warning(f"Booking with ID {booking_id} not found for update")
    return db_booking

def delete_booking(db: Session, booking_id: UUID) -> bool:
    logger.info(f"Deleting booking with ID: {booking_id}")
    db_booking = get_booking(db, booking_id)
    if db_booking:
        db.delete(db_booking)
        db.commit()
        logger.info(f"Booking with ID {booking_id} deleted successfully")
        return True
    logger.warning(f"Booking with ID {booking_id} not found for deletion")
    return False

# Fare CRUD
def get_fare(db: Session, fare_id: UUID) -> Optional[models.Fare]:
    logger.info(f"Fetching fare with ID: {fare_id}")
    return db.query(models.Fare).filter(models.Fare.fare_id == fare_id).first()

def get_fares(db: Session, skip: int = 0, limit: int = 100) -> List[models.Fare]:
    logger.info(f"Fetching fares with skip: {skip}, limit: {limit}")
    return db.query(models.Fare).offset(skip).limit(limit).all()

def create_fare(db: Session, fare: schemas.FareCreate) -> models.Fare:
    logger.info(f"Creating fare for flight: {fare.flight_id}")
    db_fare = models.Fare(**fare.dict())
    db.add(db_fare)
    db.commit()
    db.refresh(db_fare)
    logger.info(f"Fare created with ID: {db_fare.fare_id}")
    return db_fare

def update_fare(db: Session, fare_id: UUID, fare_update: schemas.FareUpdate) -> Optional[models.Fare]:
    logger.info(f"Updating fare with ID: {fare_id}")
    db_fare = get_fare(db, fare_id)
    if db_fare:
        update_data = fare_update.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_fare, field, value)
        db.commit()
        db.refresh(db_fare)
        logger.info(f"Fare with ID {fare_id} updated successfully")
    else:
        logger.warning(f"Fare with ID {fare_id} not found for update")
    return db_fare

def delete_fare(db: Session, fare_id: UUID) -> bool:
    logger.info(f"Deleting fare with ID: {fare_id}")
    db_fare = get_fare(db, fare_id)
    if db_fare:
        db.delete(db_fare)
        db.commit()
        logger.info(f"Fare with ID {fare_id} deleted successfully")
        return True
    logger.warning(f"Fare with ID {fare_id} not found for deletion")
    return False

# Passenger Document CRUD
def get_passenger_document(db: Session, document_id: UUID) -> Optional[models.Passenger_Document]:
    logger.info(f"Fetching passenger document with ID: {document_id}")
    return db.query(models.Passenger_Document).filter(models.Passenger_Document.document_id == document_id).first()

def get_passenger_documents(db: Session, skip: int = 0, limit: int = 100) -> List[models.Passenger_Document]:
    logger.info(f"Fetching passenger documents with skip: {skip}, limit: {limit}")
    return db.query(models.Passenger_Document).offset(skip).limit(limit).all()

def create_passenger_document(db: Session, document: schemas.PassengerDocumentCreate) -> models.Passenger_Document:
    logger.info(f"Creating passenger document for passenger: {document.passenger_id}")
    db_document = models.Passenger_Document(**document.dict())
    db.add(db_document)
    db.commit()
    db.refresh(db_document)
    logger.info(f"Passenger document created with ID: {db_document.document_id}")
    return db_document

def update_passenger_document(db: Session, document_id: UUID, document_update: schemas.PassengerDocumentUpdate) -> Optional[models.Passenger_Document]:
    logger.info(f"Updating passenger document with ID: {document_id}")
    db_document = get_passenger_document(db, document_id)
    if db_document:
        update_data = document_update.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_document, field, value)
        db.commit()
        db.refresh(db_document)
        logger.info(f"Passenger document with ID {document_id} updated successfully")
    else:
        logger.warning(f"Passenger document with ID {document_id} not found for update")
    return db_document

def delete_passenger_document(db: Session, document_id: UUID) -> bool:
    logger.info(f"Deleting passenger document with ID: {document_id}")
    db_document = get_passenger_document(db, document_id)
    if db_document:
        db.delete(db_document)
        db.commit()
        logger.info(f"Passenger document with ID {document_id} deleted successfully")
        return True
    logger.warning(f"Passenger document with ID {document_id} not found for deletion")
    return False

# Ticket CRUD
def get_ticket(db: Session, ticket_id: UUID) -> Optional[models.Ticket]:
    logger.info(f"Fetching ticket with ID: {ticket_id}")
    return db.query(models.Ticket).filter(models.Ticket.ticket_id == ticket_id).first()

def get_tickets(db: Session, skip: int = 0, limit: int = 100) -> List[models.Ticket]:
    logger.info(f"Fetching tickets with skip: {skip}, limit: {limit}")
    return db.query(models.Ticket).offset(skip).limit(limit).all()

def create_ticket(db: Session, ticket: schemas.TicketCreate) -> models.Ticket:
    logger.info(f"Creating ticket with number: {ticket.ticket_number}")
    db_ticket = models.Ticket(**ticket.dict())
    db.add(db_ticket)
    db.commit()
    db.refresh(db_ticket)
    logger.info(f"Ticket created with ID: {db_ticket.ticket_id}")
    return db_ticket

def update_ticket(db: Session, ticket_id: UUID, ticket_update: schemas.TicketUpdate) -> Optional[models.Ticket]:
    logger.info(f"Updating ticket with ID: {ticket_id}")
    db_ticket = get_ticket(db, ticket_id)
    if db_ticket:
        update_data = ticket_update.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_ticket, field, value)
        db.commit()
        db.refresh(db_ticket)
        logger.info(f"Ticket with ID {ticket_id} updated successfully")
    else:
        logger.warning(f"Ticket with ID {ticket_id} not found for update")
    return db_ticket

def delete_ticket(db: Session, ticket_id: UUID) -> bool:
    logger.info(f"Deleting ticket with ID: {ticket_id}")
    db_ticket = get_ticket(db, ticket_id)
    if db_ticket:
        db.delete(db_ticket)
        db.commit()
        logger.info(f"Ticket with ID {ticket_id} deleted successfully")
        return True
    logger.warning(f"Ticket with ID {ticket_id} not found for deletion")
    return False

# Payment CRUD
def get_payment(db: Session, payment_id: UUID) -> Optional[models.Payment]:
    logger.info(f"Fetching payment with ID: {payment_id}")
    return db.query(models.Payment).filter(models.Payment.payment_id == payment_id).first()

def get_payments(db: Session, skip: int = 0, limit: int = 100) -> List[models.Payment]:
    logger.info(f"Fetching payments with skip: {skip}, limit: {limit}")
    return db.query(models.Payment).offset(skip).limit(limit).all()

def create_payment(db: Session, payment: schemas.PaymentCreate) -> models.Payment:
    logger.info(f"Creating payment for booking: {payment.booking_id}")
    db_payment = models.Payment(**payment.dict())
    db.add(db_payment)
    db.commit()
    db.refresh(db_payment)
    logger.info(f"Payment created with ID: {db_payment.payment_id}")
    return db_payment

def update_payment(db: Session, payment_id: UUID, payment_update: schemas.PaymentUpdate) -> Optional[models.Payment]:
    logger.info(f"Updating payment with ID: {payment_id}")
    db_payment = get_payment(db, payment_id)
    if db_payment:
        update_data = payment_update.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_payment, field, value)
        db.commit()
        db.refresh(db_payment)
        logger.info(f"Payment with ID {payment_id} updated successfully")
    else:
        logger.warning(f"Payment with ID {payment_id} not found for update")
    return db_payment

def delete_payment(db: Session, payment_id: UUID) -> bool:
    logger.info(f"Deleting payment with ID: {payment_id}")
    db_payment = get_payment(db, payment_id)
    if db_payment:
        db.delete(db_payment)
        db.commit()
        logger.info(f"Payment with ID {payment_id} deleted successfully")
        return True
    logger.warning(f"Payment with ID {payment_id} not found for deletion")
    return False

# Dictionary CRUD operations
def get_booking_statuses(db: Session, skip: int = 0, limit: int = 100) -> List[models.Dictionary_BookingStatus]:
    return db.query(models.Dictionary_BookingStatus).offset(skip).limit(limit).all()

def get_document_types(db: Session, skip: int = 0, limit: int = 100) -> List[models.Dictionary_DocumentType]:
    return db.query(models.Dictionary_DocumentType).offset(skip).limit(limit).all()