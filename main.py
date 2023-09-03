import requests
from sqlalchemy import create_engine, Column, Integer, String, JSON
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

DATABASE_URL = "sqlite:///offers.db"
engine = create_engine(DATABASE_URL)
Base = declarative_base()

class Offer(Base):
    __tablename__ = "offers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    brand = Column(String)
    category = Column(String)
    merchant = Column(String)
    attributes = Column(JSON)
    image_url = Column(String)

Base.metadata.create_all(engine)

def download_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to download data. Status code: {response.status_code}")
        return None

def save_to_database(data):
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        for item in data["offers"]:
            offer = Offer(
                name=item["name"],
                brand=item["brand"],
                category=item["category"],
                merchant=item["merchant"],
                attributes=item["attributes"],
                image_url=item["image"]["url"]
            )
            session.add(offer)
        session.commit()
        print("Data saved to the database successfully.")
    except Exception as e:
        print(f"Error saving data to the database: {str(e)}")
        session.rollback()
    finally:
        session.close()

def main():
    url = "https://www.kattabozor.uz/hh/test/api/v1/offers"
    data = download_data(url)
    
    if data:
        save_to_database(data)

if __name__ == "__main__":
    main()
