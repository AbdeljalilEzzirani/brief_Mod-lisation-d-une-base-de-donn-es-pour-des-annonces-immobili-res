from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Numeric, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


DATABASE_URL = "postgresql://postgres:suivant@localhost:5432/real_estate"
engine = create_engine(DATABASE_URL)

Base = declarative_base()

# Define the City model
class City(Base):
    __tablename__ = 'city'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    
Base.metadata.create_all(engine)

print(Base.metadata.tables.keys())
            