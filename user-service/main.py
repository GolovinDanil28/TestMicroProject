from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError
from typing import List
import logging
import time
import json
import socket
from prometheus_fastapi_instrumentator import Instrumentator
from confluent_kafka import Producer, KafkaError

from database import engine, SessionLocal, Base
from models import User
from schemas import UserCreate, UserResponse

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("user_service.log")],
)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "user.events"
KAFKA_ENABLED = True  # Флаг для включения/отключения Kafka

# Улучшенная конфигурация Kafka Producer
kafka_conf = {
    # Основные настройки
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": f"user-service-{socket.gethostname()}",
    # Настройки надежности
    "acks": "all",  # Ждем подтверждения от всех реплик
    "retries": 5,  # Увеличиваем количество попыток
    "retry.backoff.ms": 1000,  # Задержка между попытками
    "message.timeout.ms": 15000,  # Таймаут отправки сообщения
    # Настройки производительности
    "linger.ms": 5,  # Задержка перед отправкой (батчинг)
    "batch.size": 16384,  # Размер батча
    "compression.type": "snappy",  # Сжатие для экономии трафика
    # Настройки надежности доставки
    "enable.idempotence": True,  # Идемпотентность (гарантия exactly-once)
    "max.in.flight.requests.per.connection": 1,  # Для идемпотентности
    # Настройки подключения
    "socket.keepalive.enable": True,
    "socket.timeout.ms": 30000,
    "connections.max.idle.ms": 300000,
    # Логирование Kafka клиента
    "log.connection.close": False,
    "debug": "broker,topic,msg",  # Включить отладку при необходимости
}

# Глобальный продюсер Kafka (инициализируется лениво)
kafka_producer = None
kafka_available = False


def init_kafka_producer():
    """Инициализация Kafka Producer с проверкой доступности"""
    global kafka_producer, kafka_available

    if not KAFKA_ENABLED:
        logger.info("Kafka отключена в конфигурации")
        return False

    try:
        logger.info(f"Инициализация Kafka Producer. Брокер: {KAFKA_BROKER}")

        # Пробуем подключиться несколько раз
        max_retries = 10
        retry_delay = 3

        for attempt in range(max_retries):
            try:
                # Тестовое подключение к Kafka
                test_producer = Producer(
                    {
                        "bootstrap.servers": KAFKA_BROKER,
                        "message.timeout.ms": 5000,
                    }
                )

                # Пробуем получить метаданные (проверка подключения)
                test_producer.list_topics(timeout=5)
                test_producer.flush(timeout=5)
                test_producer = None  # Закрываем тестовый продюсер

                logger.info(f"Kafka доступна (попытка {attempt + 1}/{max_retries})")

                # Инициализируем основной продюсер
                kafka_producer = Producer(kafka_conf)
                kafka_available = True

                # Тестовая отправка сообщения
                try:
                    kafka_producer.produce(
                        KAFKA_TOPIC,
                        key="test",
                        value=json.dumps({"test": "connection"}),
                    )
                    kafka_producer.poll(0)
                    kafka_producer.flush(timeout=2)
                except Exception:
                    pass  # Игнорируем ошибки тестовой отправки

                logger.info("Kafka Producer успешно инициализирован")
                return True

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Kafka недоступна (попытка {attempt + 1}/{max_retries}): {str(e)[:100]}"
                    )
                    time.sleep(retry_delay)
                else:
                    logger.error(
                        f"Не удалось подключиться к Kafka после {max_retries} попыток"
                    )
                    kafka_available = False
                    return False

    except Exception as e:
        logger.error(f"Ошибка инициализации Kafka Producer: {str(e)}")
        kafka_available = False
        return False


def delivery_report(err, msg):
    """Callback для получения результата отправки сообщения"""
    if err is not None:
        logger.error(f"Ошибка доставки сообщения в Kafka: {err}")

        # Если это критическая ошибка соединения, отмечаем Kafka как недоступную
        if err.code() in [
            KafkaError._ALL_BROKERS_DOWN,
            KafkaError._TRANSPORT,
            KafkaError._TIMED_OUT,
        ]:
            global kafka_available
            kafka_available = False
            logger.warning(
                "Kafka отмечена как недоступная, события временно не отправляются"
            )
    else:
        logger.debug(
            f"Сообщение доставлено в топик {msg.topic()} "
            f"[partition: {msg.partition()}, offset: {msg.offset()}]"
        )


def send_user_event(event_type: str, user_data: dict):
    """Отправка события пользователя в Kafka"""
    global kafka_producer, kafka_available

    if not KAFKA_ENABLED:
        logger.debug("Kafka отключена, событие не отправлено")
        return

    if not kafka_available or kafka_producer is None:
        logger.warning(f"Kafka недоступна, событие {event_type} не отправлено")

        # Попробуем переподключиться при первой неудачной попытке
        if kafka_producer is None:
            init_kafka_producer()
        return

    try:
        event_data = {
            "event_type": event_type,
            "data": user_data,
            "timestamp": time.time(),
            "service": "user-service",
            "version": "1.0.0",
        }

        # Генерируем уникальный ключ для сообщения
        message_key = (
            f"{event_type}_{user_data.get('id', 'unknown')}_{int(time.time() * 1000)}"
        )

        kafka_producer.produce(
            KAFKA_TOPIC,
            key=message_key,
            value=json.dumps(event_data, ensure_ascii=False),
            callback=delivery_report,
        )

        # Периодически вызываем poll для обработки callback'ов
        kafka_producer.poll(0)

        logger.info(
            f"Событие {event_type} отправлено для пользователя {user_data.get('id')}"
        )

    except BufferError:
        logger.warning("Буфер Kafka Producer переполнен, ожидаем освобождения...")
        kafka_producer.poll(1)  # Освобождаем буфер
        time.sleep(0.1)

        # Повторная попытка
        try:
            kafka_producer.produce(
                KAFKA_TOPIC,
                key=str(user_data.get("id")),
                value=json.dumps(event_data, ensure_ascii=False),
                callback=delivery_report,
            )
            kafka_producer.poll(0)
        except Exception as e:
            logger.error(f"Ошибка повторной отправки в Kafka: {str(e)}")

    except Exception as e:
        logger.error(f"Ошибка отправки события в Kafka: {str(e)}")

        # При некоторых ошибках помечаем Kafka как недоступную
        if "Broker transport failure" in str(e) or "Connection refused" in str(e):
            kafka_available = False
            logger.warning("Kafka отмечена как недоступная из-за ошибки подключения")


def wait_for_db():
    """Ожидание готовности базы данных"""
    max_retries = 30
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            connection = engine.connect()
            connection.close()
            logger.info(f"База данных подключена (попытка {attempt + 1})")
            return True
        except OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"База данных не готова (попытка {attempt + 1}/{max_retries}): {str(e).split(',')[0]}"
                )
                time.sleep(retry_delay)
            else:
                logger.error(
                    f"Не удалось подключиться к базе данных после {max_retries} попыток"
                )
                raise


def flush_kafka_messages():
    """Принудительная отправка всех сообщений в Kafka"""
    global kafka_producer

    if kafka_producer is not None:
        try:
            remaining = kafka_producer.flush(timeout=5)
            if remaining > 0:
                logger.warning(
                    f"Осталось {remaining} сообщений в буфере Kafka после flush"
                )
            else:
                logger.debug("Все сообщения Kafka успешно отправлены")
        except Exception as e:
            logger.error(f"Ошибка при flush Kafka: {str(e)}")


app = FastAPI(
    title="User Service",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    description="Микросервис управления пользователями с интеграцией Kafka",
)
Instrumentator().instrument(app).expose(app)


# Создание таблиц при запуске
@app.on_event("startup")
def startup():
    try:
        logger.info("=" * 50)
        logger.info("Запуск User Service...")
        logger.info("=" * 50)

        logger.info("Ожидание подключения к базе данных...")
        wait_for_db()

        logger.info("Очистка и пересоздание таблиц базы данных...")
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)
        logger.info("Таблицы базы данных созданы успешно")

        # Инициализация Kafka
        logger.info("Инициализация Kafka Producer...")
        kafka_success = init_kafka_producer()

        if kafka_success:
            logger.info("✅ Kafka Producer успешно инициализирован")
            logger.info(f"📡 Брокер: {KAFKA_BROKER}")
            logger.info(f"📨 Топик: {KAFKA_TOPIC}")
        else:
            logger.warning("⚠️ Kafka недоступна, события не будут отправляться")

        logger.info("=" * 50)
        logger.info("✅ User Service успешно запущен")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"❌ Ошибка запуска User Service: {e}")
        raise


@app.on_event("shutdown")
def shutdown_event():
    """Очистка при завершении работы"""
    logger.info("Остановка User Service...")

    # Принудительно отправляем все оставшиеся сообщения в Kafka
    flush_kafka_messages()

    logger.info("User Service остановлен")


# Middleware для логирования запросов
@app.middleware("http")
async def log_requests(request, call_next):
    start_time = time.time()

    response = await call_next(request)

    process_time = (time.time() - start_time) * 1000
    formatted_time = f"{process_time:.2f}ms"

    logger.info(
        f"{request.method} {request.url.path} "
        f"статус: {response.status_code} "
        f"время: {formatted_time}"
    )

    return response


# Зависимость для получения сессии БД
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
async def root():
    return {
        "message": "User Service API",
        "status": "running",
        "version": "1.0.0",
        "kafka": {
            "enabled": KAFKA_ENABLED,
            "available": kafka_available,
            "broker": KAFKA_BROKER,
            "topic": KAFKA_TOPIC,
        },
    }


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
            raise HTTPException(
                status_code=400,
                detail=f"Пользователь с username '{user.username}' или email '{user.email}' уже существует",
            )

        # Создание пользователя
        db_user = User(username=user.username, email=user.email)
        db.add(db_user)
        db.commit()
        db.refresh(db_user)

        logger.info(
            f"✅ Пользователь создан: ID={db_user.id}, username={db_user.username}"
        )

        # Отправка события о создании пользователя в Kafka
        user_event_data = {
            "id": db_user.id,
            "username": db_user.username,
            "email": db_user.email,
            "created_at": str(db_user.created_at) if db_user.created_at else None,
        }

        send_user_event("user.created", user_event_data)

        return db_user

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Ошибка создания пользователя: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@app.get("/users/", response_model=List[UserResponse])
def read_users(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    try:
        users = db.query(User).offset(skip).limit(limit).all()
        logger.info(
            f"📊 Получено {len(users)} пользователей (skip={skip}, limit={limit})"
        )
        return users
    except Exception as e:
        logger.error(f"Ошибка получения пользователей: {str(e)}")
        raise HTTPException(status_code=500, detail="Ошибка получения данных")


@app.get("/users/{user_id}", response_model=UserResponse)
def read_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        logger.warning(f"Пользователь не найден: {user_id}")
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    logger.info(f"📋 Получен пользователь: ID={user_id}, username={user.username}")
    return user


@app.put("/users/{user_id}", response_model=UserResponse)
def update_user(user_id: int, user_update: UserCreate, db: Session = Depends(get_db)):
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if user is None:
            raise HTTPException(status_code=404, detail="Пользователь не найден")

        old_username = user.username

        # Проверка, не занят ли новый username или email другим пользователем
        if user_update.username != user.username:
            existing = (
                db.query(User)
                .filter(User.username == user_update.username, User.id != user_id)
                .first()
            )
            if existing:
                raise HTTPException(
                    status_code=400,
                    detail=f"Username '{user_update.username}' уже занят",
                )

        if user_update.email != user.email:
            existing = (
                db.query(User)
                .filter(User.email == user_update.email, User.id != user_id)
                .first()
            )
            if existing:
                raise HTTPException(
                    status_code=400, detail=f"Email '{user_update.email}' уже занят"
                )

        user.username = user_update.username
        user.email = user_update.email
        db.commit()
        db.refresh(user)

        logger.info(f"✏️ Пользователь обновлён: {user_id}")

        # Отправка события об обновлении пользователя
        user_event_data = {
            "id": user.id,
            "old_username": old_username,
            "new_username": user.username,
            "email": user.email,
        }
        send_user_event("user.updated", user_event_data)

        return user

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка обновления пользователя: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@app.delete("/users/{user_id}")
def delete_user(user_id: int, db: Session = Depends(get_db)):
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if user is None:
            raise HTTPException(status_code=404, detail="Пользователь не найден")

        user_data = {"id": user.id, "username": user.username, "email": user.email}

        db.delete(user)
        db.commit()

        logger.info(f"🗑️ Пользователь удалён: {user_id}")

        # Отправка события об удалении пользователя
        send_user_event("user.deleted", user_data)

        return {"message": "Пользователь успешно удалён", "deleted_user": user_data}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка удаления пользователя: {str(e)}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@app.get("/health")
def health_check():
    """Проверка здоровья сервиса"""
    health_status = {
        "status": "healthy",
        "service": "user-service",
        "timestamp": time.time(),
        "version": "1.0.0",
        "kafka": {
            "enabled": KAFKA_ENABLED,
            "available": kafka_available,
            "broker": KAFKA_BROKER,
            "topic": KAFKA_TOPIC,
        },
    }

    # Проверка базы данных
    try:
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()
        health_status["database"] = "connected"
    except Exception as e:
        health_status["database"] = f"error: {str(e)[:100]}"
        health_status["status"] = "unhealthy"

    # Проверка Kafka (если включена)
    if KAFKA_ENABLED and kafka_available and kafka_producer is not None:
        try:
            # Быстрая проверка метаданных Kafka
            metadata = kafka_producer.list_topics(timeout=1)
            health_status["kafka"]["details"] = {
                "broker_count": len(metadata.brokers),
                "topics_count": len(metadata.topics),
            }
        except Exception as e:
            health_status["kafka"]["available"] = False
            health_status["kafka"]["error"] = str(e)[:100]
            health_status["status"] = "degraded"

    return health_status


@app.get("/metrics/kafka")
def kafka_metrics():
    """Метрики Kafka Producer"""
    metrics = {
        "kafka_enabled": KAFKA_ENABLED,
        "kafka_available": kafka_available,
        "producer_initialized": kafka_producer is not None,
        "config": {
            "broker": KAFKA_BROKER,
            "topic": KAFKA_TOPIC,
            "retries": kafka_conf.get("retries"),
            "timeout_ms": kafka_conf.get("message.timeout.ms"),
        },
    }

    if kafka_producer is not None:
        try:
            # Получаем метрики из продюсера
            kafka_metrics_data = kafka_producer.metrics()
            metrics["producer_metrics"] = {
                "messages_in_queue": len(kafka_producer),
                "total_requests": kafka_metrics_data.get("total_requests", 0)
                if kafka_metrics_data
                else 0,
                "total_responses": kafka_metrics_data.get("total_responses", 0)
                if kafka_metrics_data
                else 0,
            }
        except Exception:
            metrics["producer_metrics"] = "unavailable"

    return metrics


@app.post("/kafka/test")
def test_kafka_connection():
    """Тестовый эндпоинт для проверки Kafka"""
    if not KAFKA_ENABLED:
        return {"message": "Kafka отключена в конфигурации"}

    if not kafka_available or kafka_producer is None:
        return {"message": "Kafka недоступна", "status": "error"}

    try:
        test_message = {
            "event_type": "test.event",
            "data": {"test": "message", "timestamp": time.time()},
            "service": "user-service",
        }

        kafka_producer.produce(
            KAFKA_TOPIC,
            key="test",
            value=json.dumps(test_message),
            callback=delivery_report,
        )
        kafka_producer.poll(0)
        kafka_producer.flush(timeout=2)

        return {
            "message": "Тестовое сообщение отправлено в Kafka",
            "status": "success",
            "topic": KAFKA_TOPIC,
        }

    except Exception as e:
        return {
            "message": f"Ошибка отправки тестового сообщения: {str(e)}",
            "status": "error",
        }
