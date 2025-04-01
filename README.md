# Система заказов

## Сборка и поднятие

```bash
docker-compose up --build
docker exec -it orders_backend alembic upgrade head
```

## Завершение

```bash
docker-compose down
```