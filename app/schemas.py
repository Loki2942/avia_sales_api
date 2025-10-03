from pydantic import BaseModel, EmailStr, validator
from typing import Optional, List
from datetime import datetime, date
from uuid import UUID

# Base schemas
class PassengerBase(BaseModel):
    first_name: str
    last_name: str
    date_of_birth: date
    email: Optional[EmailStr] = None
    phone_number: Optional[str] = None

class PassengerCreate(PassengerBase):
    pass

class PassengerUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    date_of_birth: Optional[date] = None
    email: Optional[EmailStr] = None
    phone_number: Optional[str] = None

class Passenger(PassengerBase):
    passenger_id: UUID
    created_datetime: datetime

    class Config:
        from_attributes = True

# Flight schemas
class FlightBase(BaseModel):
    flight_number: str
    departure_airport_code: str
    arrival_airport_code: str
    scheduled_departure: datetime
    scheduled_arrival: datetime
    aircraft_type: Optional[str] = None
    total_seats: int

    @validator('scheduled_arrival')
    def validate_dates(cls, v, values):
        if 'scheduled_departure' in values and v <= values['scheduled_departure']:
            raise ValueError('Arrival date must be after departure date')
        return v

class FlightCreate(FlightBase):
    pass

class FlightUpdate(BaseModel):
    flight_number: Optional[str] = None
    departure_airport_code: Optional[str] = None
    arrival_airport_code: Optional[str] = None
    scheduled_departure: Optional[datetime] = None
    scheduled_arrival: Optional[datetime] = None
    aircraft_type: Optional[str] = None
    total_seats: Optional[int] = None

class Flight(FlightBase):
    flight_id: UUID
    created_datetime: datetime

    class Config:
        from_attributes = True

# Booking Status schemas
class BookingStatusBase(BaseModel):
    status_code: str
    status_name: str

class BookingStatusCreate(BookingStatusBase):
    pass

class BookingStatus(BookingStatusBase):
    status_id: int

    class Config:
        from_attributes = True

# Document Type schemas
class DocumentTypeBase(BaseModel):
    type_code: str
    type_name: str

class DocumentTypeCreate(DocumentTypeBase):
    pass

class DocumentType(DocumentTypeBase):
    document_type_id: int

    class Config:
        from_attributes = True

# Booking schemas
class BookingBase(BaseModel):
    booking_status_id: int
    total_amount: float
    contact_email: EmailStr
    contact_phone: str

    @validator('total_amount')
    def validate_amount(cls, v):
        if v < 0:
            raise ValueError('Total amount cannot be negative')
        return v

class BookingCreate(BookingBase):
    pass

class BookingUpdate(BaseModel):
    booking_status_id: Optional[int] = None
    total_amount: Optional[float] = None
    contact_email: Optional[EmailStr] = None
    contact_phone: Optional[str] = None

class Booking(BookingBase):
    booking_id: UUID
    booking_date: datetime
    created_datetime: datetime

    class Config:
        from_attributes = True

# Fare schemas
class FareBase(BaseModel):
    flight_id: UUID
    fare_class: str
    price: float
    fare_conditions: Optional[str] = None
    available_seats: int

    @validator('price')
    def validate_price(cls, v):
        if v < 0:
            raise ValueError('Price cannot be negative')
        return v

    @validator('available_seats')
    def validate_seats(cls, v):
        if v < 0:
            raise ValueError('Available seats cannot be negative')
        return v

class FareCreate(FareBase):
    pass

class FareUpdate(BaseModel):
    fare_class: Optional[str] = None
    price: Optional[float] = None
    fare_conditions: Optional[str] = None
    available_seats: Optional[int] = None

class Fare(FareBase):
    fare_id: UUID
    created_datetime: datetime

    class Config:
        from_attributes = True

# Passenger Document schemas
class PassengerDocumentBase(BaseModel):
    passenger_id: UUID
    document_type_id: int
    document_number: str
    expiry_date: Optional[date] = None
    country_of_issue: str

class PassengerDocumentCreate(PassengerDocumentBase):
    pass

class PassengerDocumentUpdate(BaseModel):
    document_type_id: Optional[int] = None
    document_number: Optional[str] = None
    expiry_date: Optional[date] = None
    country_of_issue: Optional[str] = None

class PassengerDocument(PassengerDocumentBase):
    document_id: UUID
    created_datetime: datetime

    class Config:
        from_attributes = True

# Ticket schemas
class TicketBase(BaseModel):
    booking_id: UUID
    passenger_id: UUID
    fare_id: UUID
    passenger_document_id: UUID
    seat_number: Optional[str] = None
    ticket_number: str

class TicketCreate(TicketBase):
    pass

class TicketUpdate(BaseModel):
    seat_number: Optional[str] = None
    ticket_number: Optional[str] = None

class Ticket(TicketBase):
    ticket_id: UUID
    created_datetime: datetime

    class Config:
        from_attributes = True

# Payment schemas
class PaymentBase(BaseModel):
    booking_id: UUID
    payment_amount: float
    payment_method: str
    transaction_id: Optional[str] = None
    payment_status: str

    @validator('payment_amount')
    def validate_amount(cls, v):
        if v <= 0:
            raise ValueError('Payment amount must be positive')
        return v

class PaymentCreate(PaymentBase):
    pass

class PaymentUpdate(BaseModel):
    payment_amount: Optional[float] = None
    payment_method: Optional[str] = None
    transaction_id: Optional[str] = None
    payment_status: Optional[str] = None

class Payment(PaymentBase):
    payment_id: UUID
    payment_date: datetime
    created_datetime: datetime

    class Config:
        from_attributes = True

# Response schemas with relationships
class PassengerWithDocuments(Passenger):
    documents: List[PassengerDocument] = []

class FlightWithFares(Flight):
    fares: List[Fare] = []

class BookingWithDetails(Booking):
    tickets: List[Ticket] = []
    payments: List[Payment] = []
    booking_status: Optional[BookingStatus] = None

class FareWithFlight(Fare):
    flight: Optional[Flight] = None

class TicketWithDetails(Ticket):
    passenger: Optional[Passenger] = None
    booking: Optional[Booking] = None
    fare: Optional[Fare] = None
    passenger_document: Optional[PassengerDocument] = None