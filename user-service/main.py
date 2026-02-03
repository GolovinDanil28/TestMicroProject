from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError
from typing import List
import logging
import time

from database import engine, SessionLocal, Base
from models import User
from schemas import UserCreate, UserResponse

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def wait_for_db():
    """Ожидание готовности базы данных"""
    max_retries = 30
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            # Пробуем подключиться
            connection = engine.connect()
            connection.close()
            logger.info(f"Database connected successfully (attempt {attempt + 1})")
            return True
        except OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"Database not ready (attempt {attempt + 1}/{max_retries}): {str(e).split(',')[0]}"
                )
                time.sleep(retry_delay)
            else:
                logger.error(
                    f"Failed to connect to database after {max_retries} attempts"
                )
                raise


app = FastAPI(
    title="User Service", version="1.0.0", docs_url="/docs", redoc_url="/redoc"
)


# Создание таблиц при запуске
@app.on_event("startup")
def startup():
    try:
        logger.info("Waiting for database...")
        wait_for_db()

        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=engine)
        logger.info("User service started successfully")
    except Exception as e:
        logger.error(f"Failed to start user service: {e}")
        # Не завершаем приложение - возможно, база станет доступна позже


# Зависимость для получения сессии БД
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
async def root():
    return {"message": "User Service API", "status": "running"}


@app.post("/users/", response_model=UserResponse)
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    try:
        # Проверка существования пользователя
        existing_user = (
            db.query(User)
            .filter((User.username == user.username) | (User.email == user.email))
            .first()
        )

        if existing_user:
            raise HTTPException(status_code=400, detail="User already exists")

        # Создание пользователя
        db_user = User(username=user.username, email=user.email)
        db.add(db_user)
        db.commit()
        db.refresh(db_user)

        logger.info(f"User created: {db_user.id}")
        return db_user
    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/users/", response_model=List[UserResponse])
def read_users(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    users = db.query(User).offset(skip).limit(limit).all()
    logger.info(f"Retrieved {len(users)} users")
    return users


@app.get("/users/{user_id}", response_model=UserResponse)
def read_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        logger.warning(f"User not found: {user_id}")
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.put("/users/{user_id}", response_model=UserResponse)
def update_user(user_id: int, user_update: UserCreate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    user.username = user_update.username
    user.email = user_update.email
    db.commit()
    db.refresh(user)

    logger.info(f"User updated: {user_id}")
    return user


@app.delete("/users/{user_id}")
def delete_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    db.delete(user)
    db.commit()

    logger.info(f"User deleted: {user_id}")
    return {"message": "User deleted successfully"}
