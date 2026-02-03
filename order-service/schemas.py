from pydantic import BaseModel
from typing import Optional
from datetime import datetime


# Схемы для запросов (Input)
class OrderCreate(BaseModel):
    user_id: int
    product_name: str
    quantity: int
    price: float


class OrderUpdate(BaseModel):
    status: str


# Схемы для ответов (Output)
class OrderResponse(BaseModel):
    id: int
    user_id: int
    product_name: str
    quantity: int
    price: float
    status: str
    created_at: Optional[datetime] = None  # если добавите поле в модель

    class Config:
        from_attributes = True  # Позволяет создавать из SQLAlchemy объектов
