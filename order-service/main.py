from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
import logging
from prometheus_fastapi_instrumentator import Instrumentator
from database import engine, SessionLocal, Base
from models import Order
from schemas import OrderCreate, OrderUpdate, OrderResponse  # Импорт из schemas.py

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Order Service")
Instrumentator().instrument(app).expose(app)


# Создание таблиц при запуске
@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)
    logger.info("Order service started")


# Зависимость для получения сессии БД
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/orders/", response_model=OrderResponse)
def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    # Используем OrderCreate из schemas.py
    db_order = Order(**order.dict())  # Автоматическое преобразование
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    return db_order  # Автоматически преобразуется в OrderResponse


@app.get("/orders/{order_id}", response_model=OrderResponse)
def read_order(order_id: int, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@app.put("/orders/{order_id}", response_model=OrderResponse)
def update_order(
    order_id: int, order_update: OrderUpdate, db: Session = Depends(get_db)
):
    order = db.query(Order).filter(Order.id == order_id).first()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")

    # Проверяем валидность статуса
    valid_statuses = ["PENDING", "PROCESSING", "COMPLETED", "CANCELLED"]
    if order_update.status not in valid_statuses:
        raise HTTPException(
            status_code=400, detail=f"Invalid status. Valid statuses: {valid_statuses}"
        )

    order.status = order_update.status
    db.commit()
    db.refresh(order)

    logger.info(f"Order updated: {order_id}")
    return order


@app.delete("/orders/{order_id}")
def delete_order(order_id: int, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.id == order_id).first()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")

    db.delete(order)
    db.commit()

    logger.info(f"Order deleted: {order_id}")
    return {"message": "Order deleted successfully"}
