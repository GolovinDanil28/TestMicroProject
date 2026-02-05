# order-service/main.py (упрощенная версия)
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import logging
import json
import time
from prometheus_fastapi_instrumentator import Instrumentator
from confluent_kafka import Consumer, KafkaError
from models import Order
from database import engine, SessionLocal, Base

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "user.events"

app = FastAPI(title="Order Service", version="1.0.0")
Instrumentator().instrument(app).expose(app)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def wait_for_kafka():
    """Ожидание готовности Kafka"""
    max_retries = 30
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            # Простая проверка подключения
            test_consumer = Consumer(
                {
                    "bootstrap.servers": KAFKA_BROKER,
                    "group.id": "test-group",
                    "auto.offset.reset": "earliest",
                }
            )

            metadata = test_consumer.list_topics(timeout=5)
            if metadata:
                logger.info(f"Kafka подключен (попытка {attempt + 1})")
                test_consumer.close()
                return True
            test_consumer.close()
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"Kafka не готова (попытка {attempt + 1}/{max_retries}): {e}"
                )
                time.sleep(retry_delay)
            else:
                logger.error(f"Не удалось подключиться к Kafka: {e}")
                return False
    return False


def simple_kafka_consumer():
    """Простой потребитель Kafka для тестирования"""
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BROKER,
                "group.id": "order-service-simple",
                "auto.offset.reset": "earliest",
            }
        )

        consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Подписались на топик: {KAFKA_TOPIC}")

        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Ошибка Kafka: {msg.error()}")
                    break

            try:
                # Просто логируем полученное сообщение
                message_value = msg.value().decode("utf-8")
                event_data = json.loads(message_value)

                logger.info("📨 Получено сообщение из Kafka:")
                logger.info(f"   Топик: {msg.topic()}")
                logger.info(f"   Ключ: {msg.key()}")
                logger.info(f"   Тип события: {event_data.get('event_type')}")
                logger.info(
                    f"   Данные: {json.dumps(event_data.get('data'), indent=2)}"
                )

                # Создаем простую запись в БД
                if event_data.get("event_type") == "user.created":
                    user_data = event_data.get("data", {})
                    user_id = user_data.get("id")
                    username = user_data.get("username", "unknown")

                    # Просто создаем тестовый заказ
                    db = SessionLocal()
                    try:
                        test_order = Order(
                            user_id=user_id,
                            total_amount=0.0,
                            status="CREATED",
                            notes=f"Автоматический заказ для {username}",
                        )
                        db.add(test_order)
                        db.commit()
                        logger.info(
                            f"✅ Создан тестовый заказ #{test_order.id} для пользователя {username}"
                        )
                    finally:
                        db.close()

            except Exception as e:
                logger.error(f"Ошибка обработки сообщения: {e}")

    except Exception as e:
        logger.error(f"Ошибка в Kafka consumer: {e}")


@app.on_event("startup")
def startup():
    try:
        logger.info("Инициализация базы данных...")
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)

        logger.info("Ожидание подключения к Kafka...")
        if wait_for_kafka():
            logger.info("Запуск Kafka consumer в фоновом режиме...")
            import threading

            kafka_thread = threading.Thread(target=simple_kafka_consumer, daemon=True)
            kafka_thread.start()
            logger.info("Kafka consumer запущен")
        else:
            logger.warning("Kafka недоступна, работаем без обработки событий")

        logger.info("✅ Order Service успешно запущен")

    except Exception as e:
        logger.error(f"Ошибка запуска: {e}")


@app.get("/")
async def root():
    return {
        "message": "Order Service API",
        "status": "running",
        "description": "Упрощенная версия для тестирования Kafka",
    }


@app.get("/orders/")
def get_orders(db: Session = Depends(get_db)):
    orders = db.query(Order).all()
    return {
        "count": len(orders),
        "orders": [
            {
                "id": o.id,
                "user_id": o.user_id,
                "status": o.status,
                "total_amount": o.total_amount,
                "notes": o.notes,
            }
            for o in orders
        ],
    }


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "order-service",
        "kafka": "connected" if wait_for_kafka() else "disconnected",
    }
