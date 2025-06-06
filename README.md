# ETL Pipeline

Проект представляет собой модульную, конфигурируемую систему для извлечения, трансформации, валидации, профилирования и загрузки данных из различных источников (CSV, SQLite) в целевые хранилища (PostgreSQL, ClickHouse).

## 🔧 Архитектура

```
extract/      - Извлечение данных из источников (CSV, SQLite)
transform/    - Преобразование данных (очистка, удаление дубликатов и др.)
load/         - Загрузка в Postgres и ClickHouse
utils/        - Вспомогательные генераторы и утилиты по профилированию и валидации
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
  drop_nulls: true
  drop_duplicates: true

validation:
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
| SQLite        | ✅         | Автоматическая генерация + извлечение + загрузка    |
| CSV           | ✅         | Поддержка кастомных заголовков            |
| PostgreSQL    | 🔜         | Универсальный загрузчик (`to_sql`)        |
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

## 🧪 Тестирование (SOON)

Планируется покрытие модулей юнит-тестами (`pytest`).

## 📌 Зависимости

- `pandas`
- `sqlite3`
- `psycopg2`
- `clickhouse-connect` (опционально)

## 📂 Структура проекта

```
project/
├── analytics/
│   ├── analytics.py
│   └── visualiazation.py
├── cfg/
│   ├── generation/
│   │   ├── csv_gen_config.yml
│   │   └── sqlite_gen_config.yml
│   ├── ingestion/
│   │   └── data_sources_config.yml
│   ├── transformation/
│   │   ├── transformation_config.yml
│   │   └── validation_config.yml
│   ├── loading/
│   │   └── loading_config.yml
│   ├── analytics_config.yml
│   └── profiling_config.yml
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── utils.py
├── config_loader.py
├── main.py
└── README.md
```

## 📃 Лицензия

MIT License — свободное распространение и модификация с указанием авторства.
