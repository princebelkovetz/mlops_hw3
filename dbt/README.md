# 🕵️‍♂️ DBT Project - Real-Time Transactions Analytics

### *MLOps Homework 4 — Бельковец Григорий (БПМИ221)*

Сервис подготовлен в демонстрационных целях в качестве домашнего задания на курсе МТС ШАД 2025 в рамках занятий по MLOps.
Датасеты предоставлены в рамках соревнования [https://www.kaggle.com/competitions/teta-ml-1-2025](https://www.kaggle.com/competitions/teta-ml-1-2025)

## Описание проекта

### Слоистая архитектура
```
raw (source) → staging → marts
```

### Staging-слой
Очистка и нормализация сырых данных + добавление вычислимых полей с помощью макроса (amount_bucket)

### Витрины (marts)
6 витрин: `mart_daily_state_metrics`, `mart_fraud_by_category`, `mart_fraud_by_state`, `mart_customer_risk_profile`, `mart_hourly_fraud_pattern`, `mart_merchant_analytics`

### Тесты
Папка singular тесты в папке tests/ + проверки с помощью dbt expectations в models/marts/schema.yml

## 🏗️ Архитектура

```
dbt
├── macros
│   └── amount_bucket.sql
├── models
│   ├── marts
│   │   ├── mart_customer_risk_profile.sql
│   │   ├── mart_daily_state_metrics.sql
│   │   ├── mart_fraud_by_category.sql
│   │   ├── mart_fraud_by_state.sql
│   │   ├── mart_hourly_fraud_pattern.sql
│   │   ├── mart_merchant_analytics.sql
│   │   └── schema.yml
│   ├── sources
│   │   └── sources.yml
│   └── staging
│       └── stg_transactions.sql
├── tests
│   ├── assert_fraud_rate_bounds.sql
│   ├── assert_no_negative_amounts.sql
│   └── assert_us_state_not_null.sql
├── README.md
├── dbt_project.yml
├── profiles.yml.example
└── packages.yml
```

## 🛠️ Инструкция по использованию

### Шаг 1: Настройка ClickHouse
Проделайте все команды из основного README файла. Убедитесь, что у вас создалась clickhouse база данных с транзакциями:

```bash
docker exec -it clickhouse clickhouse-client
SHOW TABLES;
```

Ожидаемый результат должен содержать "transactions".

### Шаг 2: Настройка профиля dbt
⚠️ Убедитесь, что папка ~/.dbt существует:
```bash
mkdir -p ~/.dbt
```

Скопируйте шаблон профиля:

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
```
### Шаг 3: Установка и запуск dbt
#### 1) Установите зависимости:
```bash
cd dbt
dbt deps
```
#### 2) Запустите модели:
```bash
dbt run
```
#### 3) Запустите тесты:
```bash
dbt test
```
#### 4) Сгенерируйте и откройте документацию:
```bash
dbt docs generate
dbt docs serve --port 8081  # порт 8080 может быть занят Kafka UI
```
→ Откройте http://localhost:8081 для просмотра DAG и метаданных.


## 📌 Примечания

* Версии:
    - dbt-core: 1.10.15
    - dbt-clickhouse: 1.9.6
* CSV `train.csv` должен быть в корне проекта
* Порты 2181, 9095, 8123, 9000, 8080 должны быть свободны на хосте.

## 💡 Автор

**Григорий Бельковец**  
БПМИ221
