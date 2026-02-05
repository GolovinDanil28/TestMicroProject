# order-service/models.py
from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from sqlalchemy.sql import func
from database import Base


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True, nullable=False)
    total_amount = Column(
        Float, nullable=False, default=0.0
    )  # Убедитесь, что есть эта строка
    status = Column(String(50), default="PENDING")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class ShoppingCart(Base):
    __tablename__ = "shopping_carts"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True, nullable=False)
    cart_name = Column(String(100), nullable=False, default="Основная корзина")
    is_default = Column(Integer, default=1)  # 1 = да, 0 = нет
    status = Column(String(50), default="active")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
