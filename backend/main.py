from fastapi import FastAPI, Depends, HTTPException, Form, Request, Cookie, Response, UploadFile, File
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, SecretStr
from sqlalchemy.orm import Session
from passlib.context import CryptContext
from db.database import SessionLocal, User,UserInResponse, UserInListResponse, UserUpdate, BlogPost,BlogPostDB
from datetime import datetime
from sqlalchemy import select
from fastapi.templating import Jinja2Templates
from datetime import datetime
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from datetime import datetime, timedelta
import jwt
from typing import List, Union
import bcrypt
from db.database import get_building_data, get_person_private_data, get_building_private_data, get_queried_private_person_data, get_private_person_results,get_private_person_wikidata_results

tags_metadata = [{'name':'GND4C-Toolbox', 'description':''}]
app = FastAPI (description='GND4C-Toolbox', openapi_tags=tags_metadata)


@app.get("/get_person_wikidata_results")
def read_private_person_data_results():
    df = get_private_person_wikidata_results()
    return df.to_dict(orient="records")


@app.get("/get_person_data_results")
def read_private_person_data_results():
    df = get_private_person_results()
    return df.to_dict(orient="records")

@app.get("/get_queried_person_data")
def read_queried_private_data():
    df = get_queried_private_person_data()
    return df.to_dict(orient="records")


@app.get("/get_person_private_data")
def read_person_private_data():
    df = get_person_private_data()
    return df.to_dict(orient="records")

@app.get("/get_building_data")
def read_building_data():
    df = get_building_data()
    return df.to_dict(orient="records")

@app.get("/get_building_private_data")
def read_building_private_data():
    df = get_building_private_data()
    return df.to_dict(orient="records")

#############################################################
#user registration and login
#############################################################
# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
@app.get("/users/{username}")
def get_user_by_username(username: str, db: Session = Depends(get_db)):
    users = db.query(User).filter(User.username == username).all()
    if not users:
        raise HTTPException(status_code=404, detail="User not found")
    return users

# Register User
@app.post("/register", tags=['user_administration'])
def register_user(
    username: str = Form(...),
    password: str = Form(...),
    name: str = Form(...),
    surname: str = Form(...),
    userrole: str = Form(...),
    date: str = Form(...),
    email: str = Form(...),
    project_role: str = Form(...),
    biography: str = Form(...),
    organization: str = Form(...),
    db: Session = Depends(get_db)
):
    hashed_password = pwd_context.hash(password)
    new_user = User(
        username=username,
        password=hashed_password,
        name=name,
        surname=surname,
        userrole=userrole,
        date=date,
        email=email,
        project_role=project_role,
        biography=biography,
        organization=organization,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return {"message": "User registered successfully"}

@app.get("/get_users")
def get_users(db: Session = Depends(get_db)):
    users = db.execute(select(User.id, User.username, User.name, User.surname, User.userrole, User.email, User.last_login,User.project_role,User.organization,User.biography)).fetchall()
    user_list = [
        {
            "user_id": user.id,
            "username": user.username,
            "name": user.name,
            "surname": user.surname,
            "userrole": user.userrole,
            "email": user.email,
            "last_login": user.last_login,
            "project_role":user.project_role,
            "organization":user.organization,
            "biography":user.biography
        }
        for user in users
    ]
    return user_list

@app.get("/admin/users/{user_id}", response_model=Union[UserInResponse, dict], tags=['user_administration'])
def read_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    # Convert the SQLAlchemy model to a dictionary representation
    user_dict = user.__dict__

    # Remove SQLAlchemy internal attributes
    user_dict.pop("_sa_instance_state", None)

    return user_dict

@app.get("/admin/users/", response_model=List[Union[UserInListResponse, dict]], tags=['user_administration'])
def list_users(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    users = db.query(User).offset(skip).limit(limit).all()

    # Convert each SQLAlchemy model to a dictionary representation
    users_list = []
    for user in users:
        user_dict = user.__dict__
        user_dict.pop("_sa_instance_state", None)
        users_list.append(user_dict)

    return users_list

@app.put("/admin/users/{user_id}", response_model=Union[UserInResponse, dict], tags=['user_administration'])
def update_user(user_id: int, user_data: UserUpdate, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.id == user_id).first()
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")

    # Update user data
    for attr, value in user_data.dict().items():
        if attr == "password":
            # Hash the new password before updating
            hashed_password = pwd_context.hash(value)
            #hashed_password = bcrypt.hashpw(value.encode('utf-8'), bcrypt.gensalt())
            setattr(db_user, attr, hashed_password)
        else:
            setattr(db_user, attr, value)

    db.commit()
    db.refresh(db_user)

    # Convert the SQLAlchemy model to a dictionary representation
    user_dict = db_user.__dict__

    # Remove SQLAlchemy internal attributes
    user_dict.pop("_sa_instance_state", None)

    return user_dict

@app.delete("/admin/users/{user_id}", response_model=dict, tags=['user_administration'])
def delete_user(user_id: int, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.id == user_id).first()
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    
    db.delete(db_user)
    db.commit()
    return {"message": "User deleted"}


# Create a secret key for signing cookies
SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# CryptContext for password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Function to create access token
def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt 

@app.post("/login")
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db)
):
    user = db.query(User).filter(User.username == username).first()
    if user and pwd_context.verify(password, user.password):
        user.last_login = datetime.utcnow()
        db.commit()

        # Create an access token with the username
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(data={"sub": username}, expires_delta=access_token_expires)

        # Set the access token as a cookie
        response = JSONResponse(content={"message": "Login successful"})
        response.set_cookie(key="access_token", value=access_token)

        return response
    else:
        raise HTTPException(status_code=401, detail="Invalid credentials")

@app.post("/blog/post/")
async def create_blog_post(blog_post: BlogPost, db: Session = Depends(get_db)):
    db_post = BlogPostDB(
        author=blog_post.author,
        title=blog_post.title,
        article=blog_post.article,
        postdate=blog_post.postdate,
        email=blog_post.email,
        language=blog_post.language
    )
    db.add(db_post)
    db.commit()
    db.refresh(db_post)
    return {"message": "Blog post created successfully", "data": db_post}