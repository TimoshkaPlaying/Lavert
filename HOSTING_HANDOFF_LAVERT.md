# Lavert: Полное Техническое Описание Проекта Для Хостинг-Специалиста

## 1) Что это за проект
- Проект: **Lavert** (веб-мессенджер + desktop/mobile клиенты-обертки).
- Основной runtime: **Python Flask + Flask-SocketIO (eventlet)**.
- Основной интерфейс: HTML/CSS/JS (SPA-подобный фронт в `templates/` + `static/`).
- Реалтайм: Socket.IO (чат, статусы, звонки/события).
- Хранилище медиа и состояния: **Backblaze B2 (S3-compatible)**.

## 2) Текущая архитектура
- Backend: `server/server.py`
- Frontend:
  - HTML: `templates/landing.html`, `templates/login.html`, `templates/register.html`, `templates/messenger.html`
  - JS: `static/script.js` (+ `static/calls.js`, `static/help.js`)
  - CSS: `static/style.css` (+ `static/calls.css`, `static/help.css`)
- Клиенты:
  - Windows app wrapper: `devices/windows/`
  - Android wrapper: `devices/android/LevartAndroid/`
  - Desktop-electron: `devices/desktop-electron/`

## 3) Критичные требования хостинга
Хостинг должен поддерживать:
1. **Python 3.11** (зафиксировано `PYTHON_VERSION=3.11.11`).
2. **WebSocket / Socket.IO** без принудительного отключения upgrade.
3. Долгоживущие соединения (чат, звонки).
4. Возможность задать ENV-переменные.
5. Стабильный egress к Backblaze B2 (S3 endpoint).
6. Нормальные лимиты RAM/CPU (минимум 1 GB RAM рекомендовано).

## 4) Команда запуска
- Основная старт-команда:
`gunicorn --worker-class eventlet -w 1 -b 0.0.0.0:$PORT server.server:app`

Почему 1 worker:
- Для текущей модели eventlet + Socket.IO и сохранения простоты состояния.

## 5) Зависимости
`requirements.txt`:
- Flask==3.0.3
- Flask-SocketIO==5.3.6
- eventlet==0.36.1
- gunicorn==22.0.0
- psycopg[binary]==3.2.9
- boto3==1.35.99

## 6) ENV переменные (обязательные и важные)
### Базовые
- `PYTHON_VERSION=3.11.11`
- `EVENTLET_NO_GREENDNS=yes`

### Backblaze B2 (обязательные)
- `B2_ENDPOINT=https://s3.eu-central-003.backblazeb2.com`
- `B2_KEY_ID=<B2 key id>`
- `B2_APPLICATION_KEY=<B2 app key>`
- `B2_BUCKET_NAME=lavert-storage`

### Политика хранения (рекомендуемые)
- `STATE_SYNC_TO_OBJECT_STORAGE=1`
- `STATE_SYNC_STRICT=1`
- `STORAGE_FALLBACK_LOCAL=0`
- `STATE_STORAGE_SUBPROCESS=1`

### Опциональные
- `STORAGE_REGION=us-east-1`
- `DATA_DIR=/data` (если нужен локальный persistent volume)
- `DATABASE_URL=<postgres...>` (если используется Postgres)

## 7) Как сейчас устроено хранение данных
### 7.1 Состояние приложения
- Логические наборы данных (`users/messages/groups/stories/...`) обрабатываются как JSON-state.
- При текущей конфигурации:
  - Источник истины: object storage (B2), strict mode.
  - Local fallback для upload отключен (`STORAGE_FALLBACK_LOCAL=0`).

### 7.2 Медиа
- Upload endpoint: `/upload`
- Файлы должны попадать в B2 (S3 API), а не локально.

### 7.3 Что НЕ является постоянным состоянием
- In-memory online status, active calls, временные socket-структуры.

## 8) Проверочные endpoint-ы для диагностики
- `GET /api/online_status`
- `GET /api/storage_status`
  - ожидаемо:
  - `backend: "b2"`
  - `client_ready: true`
  - `state_sync: true`
  - `state_sync_strict: true`
  - `state_storage_subprocess: true`
  - `upload_fallback_local: false`
- `GET /api/storage_probe`
  - пишет тестовый объект в B2 и возвращает `ok:true` при успехе.

## 9) Что важно настроить в reverse proxy / ingress
1. Не ломать `Upgrade`/`Connection` для WebSocket.
2. Таймауты на upstream не занижать агрессивно (иначе рвутся сокеты).
3. Не кэшировать API-ответы с чувствительными данными.
4. Не блокировать большие multipart upload (минимум до 2GB в коде).

## 10) Частые симптомы и их причины
### Симптом: “все висит”, кнопки не реагируют
- Причина: сеть/сервер отвечает долго, фронт ждет fetch.
- Меры: на фронте есть timeout + offline-cache fallback.

### Симптом: регистрация падает с ошибкой сохранения
- Причина: ошибка записи state в B2.
- Проверка: `storage_status`, `storage_probe`, логи backend.

### Симптом: B2 пустой
- Причина: неверные ключи/endpoint/bucket или старый деплой-коммит.
- Проверка: `client_ready`, `storage_probe`, текущий commit deployed.

## 11) Клиентские приложения
### Windows
- Папка: `devices/windows/`
- Текущий wrapper: pywebview + edgechromium.
- Конфиг URL: `devices/windows/config.json`
- Последние сборки лежат в `devices/windows/dist/` и `git/devices/windows/dist/`.

### Android
- Папка: `devices/android/LevartAndroid/`
- URL задается через build config.

## 12) Что нужно хостинг-специалисту сделать в первую очередь
1. Поднять сервис из репозитория и убедиться, что старт-команда совпадает.
2. Прописать ENV из раздела 6.
3. Проверить `storage_status` -> `client_ready=true`.
4. Проверить `storage_probe` -> `ok=true`.
5. Проверить регистрацию нового пользователя.
6. Проверить upload файла и чтение истории/настроек.

## 13) Минимальный acceptance checklist
- [ ] Главная/логин/регистрация открываются без зависаний.
- [ ] Регистрация нового аккаунта успешна.
- [ ] Логин успешен.
- [ ] `storage_status` показывает рабочий B2.
- [ ] `storage_probe` успешен.
- [ ] Upload не уходит в локальный fallback.
- [ ] После restart контейнера данные не пропадают.
- [ ] WebSocket события (онлайн, новые сообщения) работают стабильно.

## 14) Текущий рабочий домен (на момент передачи)
- `https://lavert-chat-beta.onrender.com/`

Если домен/хостинг меняется, обязательно обновить:
- web client base URL (если нужен),
- Windows config (`devices/windows/config.json`),
- Android build config.

## 15) Контекст по безопасности
- Секреты не хранить в репозитории.
- Все ключи только через ENV.
- Ограничить доступ к dashboard/логам/секретам.

