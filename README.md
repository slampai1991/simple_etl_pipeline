# ETL Pipeline Framework

Проект представляет собой модульную, конфигурируемую систему для извлечения, трансформации, валидации, профилирования и загрузки данных из различных источников (CSV, SQLite) в целевые хранилища (PostgreSQL, ClickHouse).

## 🔧 Архитектура

```
extract/      - Извлечение данных из источников (CSV, SQLite)
transform/    - Преобразование данных (очистка, удаление дубликатов и др.)
validate/     - Проверка корректности и целостности данных
profile/      - Профилирование (описательная статистика, анализ типов и распределений)
load/         - Загрузка в Postgres и ClickHouse
utils/        - Вспомогательные генераторы, логгеры и утилиты
config.yaml   - Центральный файл конфигурации
main.py       - Основной скрипт запуска
```

## ⚙️ Конфигурация

Все параметры задаются через `config.yaml`, включая пути к данным, настройки БД, параметры трансформации и валидации. Пример:

```yaml
data_sources:
  sqlite: ./data
  csv: ./csv

sqlite_config:
  db_name: test.db

csv_config:
  headers: ["id", "name", "created_at"]

transformations:
  clean_data:
    type: drop_nulls
    columns: "*"
  remove_duplicates:
    type: drop_duplicates
    columns: "*"

validation:
  enable_foreign_keys: true
  foreign_keys:
    orders:
      user_id: users
      product_id: products

load_config:
  postgres:
    host: localhost
    port: 5432
    user: your_user
    password: your_password
    database: your_database
  clickhouse:
    host: your-ch-host.com
    port: 9440
    user: default
    password: your_password
    secure: true
```

## 🚀 Быстрый старт

1. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```

2. Убедитесь, что `config.yaml` настроен под ваш кейс.

3. Запустите основной скрипт:
   ```bash
   python main.py
   ```

## 📦 Поддерживаемые источники и приёмники

| Тип           | Поддержка | Комментарий                              |
|---------------|-----------|-------------------------------------------|
| SQLite        | ✅         | Автоматическая генерация + извлечение     |
| CSV           | ✅         | Поддержка кастомных заголовков            |
| PostgreSQL    | ✅         | Универсальный загрузчик (`to_sql`)        |
| ClickHouse    | 🔜         | Заготовка под `clickhouse-connect` или `clickhouse-driver` |

## 📈 Профилирование

Класс `DataProfiler` позволяет анализировать загруженные DataFrame:

- Типы данных и доли null-значений
- Распределения, уникальные значения
- Частоты по категориям

## 🛡️ Валидаторы

Поддерживается проверка:
- Обязательных столбцов
- Уникальности
- Внешних ключей (с мягкой обработкой ошибок)

## 🧪 Тестирование

Планируется покрытие модулей юнит-тестами (`pytest`).

## 📌 Зависимости

- `pandas`
- `sqlite3`
- `psycopg2`
- `clickhouse-connect` (опционально)
- `PyYAML`

## 📂 Структура проекта

```
project/
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── utils.py
├── config.yaml
├── main.py
└── README.md
```

## 📃 Лицензия

MIT License — свободное распространение и модификация с указанием авторства.
