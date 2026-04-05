# Lavert Performance Profile (Anti-Lag)

## Цель
Минимизировать зависания на слабом хостинге и при медленном соединении к Backblaze B2.

## Обязательные переменные окружения
- `STATE_SYNC_TO_OBJECT_STORAGE=1`
- `STATE_SYNC_STRICT=0`
- `STATE_SYNC_BLOCKING=0`
- `STATE_STORAGE_SUBPROCESS=1`
- `SOCKETIO_ASYNC_MODE=threading`
- `STATE_CACHE_TTL_SEC=2.0`
- `SEARCH_CTX_TTL_SEC=8`
- `STORAGE_FALLBACK_LOCAL=0`

## Рекомендуемый запуск (Linux)
```bash
gunicorn -k gthread --threads 8 -w 1 -b 0.0.0.0:$PORT server.server:app
```

## Почему так быстрее
- Состояние чатов сначала пишется в локальную SQL БД (быстро), синк в B2 идет фоном.
- Поиск кэширует вычисления контактов и не сканирует все сообщения на каждый символ.
- Клиент не сбрасывает результаты поиска при сетевой ошибке и делает автоповтор.

## Минимальные ресурсы контейнера
- CPU: 1 vCPU
- RAM: 1024 MB
- Swap: желательно включить (если хостинг дает)

## Проверка после деплоя
1. Открыть `/api/storage_status` и убедиться, что:
   - `backend` = `b2`
   - `client_ready` = `true`
   - `state_sync` = `true`
   - `state_sync_strict` = `false`
   - `socketio_async_mode` = `threading`
2. Проверить поиск: при слабом интернете список не должен очищаться.
