# CDCF Journal Templates Specification

## Overview

Система генерации JSON-инструкций CDCF Journal потоков на основе метамодели.

### Структура генерации
```
templates/cdcf/
  ├── cdcf_mssql_journal.j2     # MS SQL MultiTable (nodeTypeId: 135)
  ├── cdcf_postgres_journal.j2  # PostgreSQL MultiTable (nodeTypeId: 114)
  ├── cdcf_oracle_journal.j2    # Oracle MultiTable (nodeTypeId: 116)
  └── cdcf_mongo_journal.j2     # MongoDB MultiTable (nodeTypeId: 140)

generate_cdcf.py                # Скрипт генерации
```

---

## Входные данные (Метамодель)

### Структура метамодели
```json
{
  "current": {
    "workflow_code": "cids-data.{app}.{module}.cdcf_{source}2kafka_bdp{module}_{type}",
    "workflow_name": "...",
    "metaclass_code": "mcCDCJournalFlow",
    "propvalues_json": {
      "metaflow_code": "mfCDCJournalFlow",
      "flow_uuid": "{uuid}"
    },
    "tasks": [...],
    "providers": [...],
    "appsystems": [...],
    "metaobjects": [...],
    "flowtasks": [...],
    "flowtransitions": [...]
  }
}
```

### Ключевые сущности метамодели

| Сущность | Описание | Использование |
|----------|----------|---------------|
| `workflow_code` | Код потока | Именование диаграммы |
| `flow_uuid` | UUID потока | diagram_id, diagramVersionId |
| `providers[0]` | Провайдер БД | data_provider_uuid, dbtype_code |
| `metaobjects[]` | Список таблиц | Список таблиц для CDC и фильтра |
| `flowtasks[]` | Задачи потока | UUID для узлов диаграммы |
| `flowtransitions[]` | Переходы | Links между узлами |

### Metaobject структура
```json
{
  "metaobject_code": "cids-data.{app}.{module}.{schema}__{table}",
  "entitytype_pname": "{schema}__{table}",  // Например: "dbo__lz_agreement_types"
  "application_code": "{app}",               // Например: "elm"
  "module_code": "{module}"                  // Например: "elm"
}
```

**Парсинг entitytype_pname:**
- Schema: часть до `__` (например, `dbo` из `dbo__lz_agreement_types`)
- Table: часть после `__` (например, `lz_agreement_types`)

---

## Выходные данные (JSON Diagram)

### Структура выходного JSON
```json
{
  "diagrams": {
    "{diagram_id}": {
      "diagramWithParameters": { ... },
      "nodes": [
        { "nodeTypeId": 1xx, ... },  // CDC Source
        { "nodeTypeId": 104, ... },  // Mapping
        { "nodeTypeId": 105, ... },  // Filter
        { "nodeTypeId": 103, ... }   // Kafka
      ],
      "links": [ ... ]
    }
  },
  "rootIdsAndDescriptions": { ... }
}
```

### NodeTypeId по типу источника
| Тип БД | nodeTypeId | displayType |
|--------|------------|-------------|
| PostgreSQL | 114 | Источник - CDC - PostgreSQL MultiTable |
| Oracle | 116 | Источник - CDC - Oracle MultiTable |
| MS SQL | 135 | Источник - CDC - MS SQL MultiTable |
| MongoDB | 140 | Источник - CDC - MongoDB MultiTable |

---

## Общие правила генерации

### 1. UUID Handling
- **diagram_id**: `flow.propvalues_json.flow_uuid`
- **node UUID**: Из соответствующего `flowtask.propvalues_json.flowtask_uuid`
- **link UUID**: Из соответствующего `flowtransition.propvalues_json.flowtransition_uuid`
- **data_provider_uuid**: Из `providers[0].tool_uuid` или `environments[0].component_uuid`

### 2. Именование

#### Diagram Name
```
cdcf__{db}2kafka_bdp__{app_name}
```
Где `{db}` = mssql, postgres, oracle, mongo

#### Object Name
```
cdcf__{db}2kafka_bdp__{app_code}
```

#### Node Names
- CDC Source: `Источник - CDC - {DB} MultiTable_0`
- Mapping: `{table_lower}_key_mapping`
- Filter: `{table_lower}_filter`
- Kafka: `Kafka {app_lower}`

### 3. Schema и Table
- **Schema**: Префикс `entitytype_pname` до `__`
- **Table**: Остальная часть `entitytype_pname` после `__`
- Пример: `dbo__lz_agreement_types` → schema=`dbo`, table=`lz_agreement_types`

### 4. Kafka Topic
Формат: `{app_code}__{table}__journal`

### 5. Key Value Expression (Mapping)
Для всех типов источников (lowercase):
```javascript
cast(
  case when table_name in ({table_list})
  then IF(json_value(data, '$.op') = 'd', json_value(data, '$.before.id'), json_value(data, '$.after.id'))
  end as string
)
```

Где `{table_list}` - список всех таблиц в формате `'{schema}.{table}'`

### 6. Filter Expression
```sql
table_name in (
  '{schema}.{table1}',
  '{schema}.{table2}',
  ...
)
```

### 7. Позиции узлов
Могут быть фиксированными (для простоты) или 0,0.

---

## Специфичные настройки по типу БД

### PostgreSQL (nodeTypeId: 114)

**cdc.postgres:**
```json
{
  "decoding": { "plugin": { "name": "pgoutput" } },
  "server": { "time": { "zone": "UTC" } },
  "slot": { "name": "{slot_name}" },
  "table": { "name": ["table1", "table2", ...] }
}
```

**debezium properties:**
```json
[
  { "name": "publication.name", "value": "cdc_publication" },
  { "name": "snapshot.mode", "value": "never" },
  { "name": "schema.include.list", "value": "{schema}" },
  { "name": "heartbeat.interval.ms", "value": "5000" },
  { "name": "max.batch.size", "value": "10000" },
  { "name": "max.queue.size", "value": "32000" }
]
```

### Oracle (nodeTypeId: 116)

**cdc.oracle:**
```json
{
  "scan": { "startup": { "mode": "LATEST_OFFSET" } },
  "server": { "time": { "zone": "UTC" } }
}
```

**debezium properties:**
```json
[
  { "name": "snapshot.mode", "value": "no_data" },
  { "name": "database.connection.adapter", "value": "xstream" },
  { "name": "database.out.server.name", "value": "dbzxout" },
  { "name": "database.pdb.name", "value": "{pdb_name}" },
  { "name": "schema.include.list", "value": "{schema}" }
]
```

### MS SQL (nodeTypeId: 135)

**cdc.mssql:**
```json
{
  "scan": { "startup": { "mode": "LATEST_OFFSET" } },
  "server": { "time": { "zone": "UTC" } }
}
```

**debezium properties:**
```json
[
  { "name": "snapshot.mode", "value": "no_data" },
  { "name": "database.hostname", "value": "{host}" },
  { "name": "database.port", "value": "{port}" },
  { "name": "database.user", "value": "{user}" },
  { "name": "database.dbname", "value": "{dbname}" },
  { "name": "schema.include.list", "value": "dbo" }
]
```

### MongoDB (nodeTypeId: 140)

**cdc.mongo:**
```json
{
  "scan": { "startup": { "mode": "LATEST_OFFSET" } },
  "server": { "time": { "zone": "UTC" } }
}
```

**debezium properties:**
```json
[
  { "name": "snapshot.mode", "value": "no_data" },
  { "name": "mongodb.hosts", "value": "{hosts}" },
  { "name": "mongodb.name", "value": "{rs_name}" },
  { "name": "collection.include.list", "value": "{db}.{collection}" }
]
```

---

## Генератор (generate_cdcf.py)

### Функционал
1. Загрузка метамодели из JSON файла
2. Определение типа источника по `providers[0].tool_code`
3. Выбор соответствующего шаблона
4. Рендеринг шаблона с данными метамодели
5. Сохранение результата в JSON файл

### Использование
```bash
python generate_cdcf.py --meta <path_to_meta.json> --output <path_to_output.json>
```

### Параметры рендеринга
| Параметр | Источник | Описание |
|----------|----------|----------|
| `diagram_id` | `flow_uuid` | UUID диаграммы |
| `app_code` | `metaobject.application_code` | Код приложения |
| `module_code` | `metaobject.module_code` | Код модуля |
| `source_type` | `provider.tool_code` | mssql.2019, tPostgres, etc |
| `provider_uuid` | `provider.tool_uuid` | UUID провайдера |
| `metaobjects` | `flow.metaobjects` | Список таблиц |
| `tables` | вычисляется | Список `{schema}.{table}` |
| `flowtasks` | `flow.flowtasks` | Задачи для UUID узлов |
| `flowtransitions` | `flow.flowtransitions` | Переходы для links |
| `current_date` | `datetime.now()` | Текущая дата/время |

---

## Пример рендеринга

### Вход (метамодель фрагмент):
```json
{
  "application_code": "elm",
  "module_code": "elm",
  "entitytype_pname": "dbo__lz_agreement_types",
  "propvalues_json": {
    "flow_uuid": "095f24b9-9278-541a-a82c-0cb9c0ae2771"
  }
}
```

### Выход (диаграмма фрагмент):
```json
{
  "diagramNameEntity": {
    "objectName": "cdcf__mssql2kafka_bdp__elm"
  },
  "nodes": [
    {
      "nodeVersionId": "...",
      "nodeTypeId": 135,
      "jsonProperty": {
        "cdc": {
          "mssql": { ... }
        },
        "data_provider_uuid": "..."
      }
    }
  ]
}
```

---

## Зависимости

- Python 3.9+
- jinja2
- dacite

---

## Примечания

1. Если в метамодели нет каких-либо данных, спрашивать у пользователя
2. Для MSSQL source по умолчанию используется lowercase для key field (`id`)
3. Schema извлекается из префикса `entitytype_pname`
4. При auto-capture (capture_all_tables=true) не используется filter node
