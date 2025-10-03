from sqlalchemy import Column, String, Integer, DateTime, Date, Numeric, SmallInteger, Text, ForeignKey, CheckConstraint
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


class Dictionary_BookingStatus(Base):
    __tablename__ = 'Dictionary_BookingStatus'

    status_id = Column(SmallInteger, primary_key=True, autoincrement=True)
    status_code = Column(String(30), nullable=False, unique=True)
    status_name = Column(String(100), nullable=False)

    # Relationship
    bookings = relationship("Booking", back_populates="booking_status")


class Dictionary_DocumentType(Base):
    __tablename__ = 'Dictionary_DocumentType'

    document_type_id = Column(SmallInteger, primary_key=True, autoincrement=True)
    type_code = Column(String(30), nullable=False, unique=True)
    type_name = Column(String(100), nullable=False)

    # Relationship
    passenger_documents = relationship("Passenger_Document", back_populates="document_type")


class Passenger(Base):
    __tablename__ = 'Passenger'

    passenger_id = Column(UNIQUEIDENTIFIER, primary_key=True, server_default=func.newid())
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    date_of_birth = Column(Date, nullable=False)
    email = Column(String(255))
    phone_number = Column(String(20))
    created_datetime = Column(DateTime, nullable=False, server_default=func.getutcdate())

    # Relationships
    documents = relationship("Passenger_Document", back_populates="passenger")
    tickets = relationship("Ticket", back_populates="passenger")


class Flight(Base):
    __tablename__ = 'Flight'

    flight_id = Column(UNIQUEIDENTIFIER, primary_key=True, server_default=func.newid())
    flight_number = Column(String(10), nullable=False)
    departure_airport_code = Column(String(3), nullable=False)
    arrival_airport_code = Column(String(3), nullable=False)
    scheduled_departure = Column(DateTime, nullable=False)
    scheduled_arrival = Column(DateTime, nullable=False)
    aircraft_type = Column(String(50))
    total_seats = Column(SmallInteger, nullable=False)
    created_datetime = Column(DateTime, nullable=False, server_default=func.getutcdate())

    # Relationships
    fares = relationship("Fare", back_populates="flight")

    __table_args__ = (
        CheckConstraint('scheduled_arrival > scheduled_departure', name='CHK_Flight_Dates'),
        CheckConstraint('total_seats > 0', name='CHK_Flight_Seats'),
    )


class Booking(Base):
    __tablename__ = 'Booking'

    booking_id = Column(UNIQUEIDENTIFIER, primary_key=True, server_default=func.newid())
    booking_date = Column(DateTime, nullable=False, server_default=func.getutcdate())
    booking_status_id = Column(SmallInteger, ForeignKey('Dictionary_BookingStatus.status_id'), nullable=False)
    total_amount = Column(Numeric(10, 2), nullable=False)
    contact_email = Column(String(255), nullable=False)
    contact_phone = Column(String(20), nullable=False)
    created_datetime = Column(DateTime, nullable=False, server_default=func.getutcdate())

    # Relationships
    booking_status = relationship("Dictionary_BookingStatus", back_populates="bookings")
    tickets = relationship("Ticket", back_populates="booking")
    payments = relationship("Payment", back_populates="booking")

    __table_args__ = (
        CheckConstraint('total_amount >= 0', name='CHK_Booking_TotalAmount'),
    )


class Fare(Base):
    __tablename__ = 'Fare'

    fare_id = Column(UNIQUEIDENTIFIER, primary_key=True, server_default=func.newid())
    flight_id = Column(UNIQUEIDENTIFIER, ForeignKey('Flight.flight_id'), nullable=False)
    fare_class = Column(String(30), nullable=False)
    price = Column(Numeric(10, 2), nullable=False)
    fare_conditions = Column(String(500))
    available_seats = Column(SmallInteger, nullable=False)
    created_datetime = Column(DateTime, nullable=False, server_default=func.getutcdate())

    # Relationships
    flight = relationship("Flight", back_populates="fares")
    tickets = relationship("Ticket", back_populates="fare")

    __table_args__ = (
        CheckConstraint('price >= 0', name='CHK_Fare_Price'),
        CheckConstraint('available_seats >= 0', name='CHK_Fare_AvailableSeats'),
    )


class Passenger_Document(Base):
    __tablename__ = 'Passenger_Document'

    document_id = Column(UNIQUEIDENTIFIER, primary_key=True, server_default=func.newid())
    passenger_id = Column(UNIQUEIDENTIFIER, ForeignKey('Passenger.passenger_id'), nullable=False)
    document_type_id = Column(SmallInteger, ForeignKey('Dictionary_DocumentType.document_type_id'), nullable=False)
    document_number = Column(String(50), nullable=False)
    expiry_date = Column(Date)
    country_of_issue = Column(String(3), nullable=False)
    created_datetime = Column(DateTime, nullable=False, server_default=func.getutcdate())

    # Relationships
    passenger = relationship("Passenger", back_populates="documents")
    document_type = relationship("Dictionary_DocumentType", back_populates="passenger_documents")
    tickets = relationship("Ticket", back_populates="passenger_document")


class Ticket(Base):
    __tablename__ = 'Ticket'

    ticket_id = Column(UNIQUEIDENTIFIER, primary_key=True, server_default=func.newid())
    booking_id = Column(UNIQUEIDENTIFIER, ForeignKey('Booking.booking_id'), nullable=False)
    passenger_id = Column(UNIQUEIDENTIFIER, ForeignKey('Passenger.passenger_id'), nullable=False)
    fare_id = Column(UNIQUEIDENTIFIER, ForeignKey('Fare.fare_id'), nullable=False)
    passenger_document_id = Column(UNIQUEIDENTIFIER, ForeignKey('Passenger_Document.document_id'), nullable=False)
    seat_number = Column(String(5))
    ticket_number = Column(String(13), nullable=False, unique=True)
    created_datetime = Column(DateTime, nullable=False, server_default=func.getutcdate())

    # Relationships
    booking = relationship("Booking", back_populates="tickets")
    passenger = relationship("Passenger", back_populates="tickets")
    fare = relationship("Fare", back_populates="tickets")
    passenger_document = relationship("Passenger_Document", back_populates="tickets")


class Payment(Base):
    __tablename__ = 'Payment'

    payment_id = Column(UNIQUEIDENTIFIER, primary_key=True, server_default=func.newid())
    booking_id = Column(UNIQUEIDENTIFIER, ForeignKey('Booking.booking_id'), nullable=False)
    payment_amount = Column(Numeric(10, 2), nullable=False)
    payment_date = Column(DateTime, nullable=False, server_default=func.getutcdate())
    payment_method = Column(String(50), nullable=False)
    transaction_id = Column(String(100), unique=True)
    payment_status = Column(String(30), nullable=False)
    created_datetime = Column(DateTime, nullable=False, server_default=func.getutcdate())

    # Relationships
    booking = relationship("Booking", back_populates="payments")

    __table_args__ = (
        CheckConstraint('payment_amount > 0', name='CHK_Payment_Amount'),
    )