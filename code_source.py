from sqlalchemy import create_engine, Column, Integer, String, DateTime, Numeric, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from datetime import datetime 
import pandas as pd
from sqlalchemy.exc import IntegrityError
import os
 
datab = os.getenv("DATABASE_URL", "postgresql://postgres:suivant.@localhost:5432/postgres")
engine = create_engine(datab)

Base = declarative_base()


class City(Base):
    __tablename__ = 'City'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    annonces = relationship("Annonce", back_populates="city")

class Annonce(Base):
    __tablename__ = 'Annonce'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    price = Column(String)
    datetime = Column(DateTime, default=datetime.utcnow)
    nb_rooms = Column(String)
    nb_baths = Column(Integer)
    surface_area = Column(Numeric)
    link = Column(String)
    city_id = Column(Integer, ForeignKey('City.id'))
    
    city = relationship("City", back_populates="annonces")
    equipments = relationship("Table_associative", back_populates="annonce")

class Equipement(Base):
    __tablename__ = 'Equipement'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    annonces = relationship("Table_associative", back_populates="equipement")

class Table_associative(Base):
    __tablename__ = 'Table_associative'
    annonce_id = Column(Integer, ForeignKey('Annonce.id'), primary_key=True)
    equipment_id = Column(Integer, ForeignKey('Equipement.id'), primary_key=True)
    
    annonce = relationship("Annonce", back_populates="equipments")
    equipement = relationship("Equipement", back_populates="annonces")

Base.metadata.create_all(engine)

# Session setup
Session = sessionmaker(bind=engine)
session = Session()


# Read the CSV file
df = pd.read_csv('data_final.csv')

columns_to_extract = [
    'Ascenseur', 'Balcon', 'Chauffage', 'Climatisation', 
    'Concierge', 'Cuisine equipee', 'Duplex', 'Meuble', 
    'Parking', 'Securite', 'Terrasse', 'Date'
]

# Insert unique equipment into the Equipement table
for element in columns_to_extract:
    # Check if the equipment already exists
    existing_equipment = session.query(Equipement).filter_by(name=element).first()
    if not existing_equipment:
        row = Equipement(name=element)  # No need to set id
        session.add(row)
        session.commit()

# Insert announcements from the DataFrame
for index, row in df.iterrows():
    try:
        city_name = row['Localisation']
        city = session.query(City).filter_by(name=city_name).first()
        if not city:
            city = City(name=city_name)
            session.add(city)
            session.commit()  # Save the city to get its ID

        annonce = Annonce(
            title=row['Title'],
            price=row['Price'],
            nb_rooms=row['Chambre'],
            nb_baths=row['Salle de bain'],
            surface_area=row['Surface habitable'],
            link=row['EquipementURL'],
            city_id=city.id  # Reference the city's ID
        )
        session.add(annonce)
        session.commit()

        # Now associate the equipment with the annonce
        for column in columns_to_extract:
            if row[column] == True:  # Check if the equipment is available
                # Get the existing equipment
                equipment = session.query(Equipement).filter_by(name=column).first()
                if equipment:
                    # Create a new entry in the associative table
                    association = Table_associative(annonce_id=annonce.id, equipment_id=equipment.id)
                    session.add(association)
                    session.commit()

    except IntegrityError:
        session.rollback()  # Rollback on integrity error
        print(f"Erreur d'intégrité pour l'annonce {row['Title']}. Peut-être une clé dupliquée.")
    except Exception as e:
        session.rollback()  # Rollback on other errors
        print(f"Erreur lors de l'importation de l'annonce {row['Title']}: {e}")

session.close()
