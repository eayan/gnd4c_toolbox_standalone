from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from passlib.hash import bcrypt
from db.database import User, Base

DATABASE_URL = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def initialize_database():
    Base.metadata.create_all(bind=engine)
    
    db = SessionLocal()
    #please change all these (XXXX) with your own user credentials
    initial_user = User(
        username="XXXX",
        password=bcrypt.hash("XXXX"),
        name="XXXX",
        surname="XXXX",
        userrole ="XXXX",
        date="XXXX",
        email="XXXX@XXXX"
    )
    db.add(initial_user)
    db.commit()

if __name__ == "__main__":
    initialize_database()