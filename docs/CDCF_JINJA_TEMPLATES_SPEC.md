# CDCF Jinja Templates Specification

## Overview

Система генерации JSON-инструкций CDCF (Change Data Capture Flow) пайплайнов на основе метамодели.
Генерирует полный pipeline: Источник CDC → Трансформация (Mapper + Filter) → Kafka Sink.

## Target Source Types

| Source Type | nodeTypeId | Description |
|-------------|------------|-------------|
| PostgreSQL  | 114        | PostgreSQL MultiTable CDC |
| Oracle      | 116        | Oracle MultiTable CDC |
| MS SQL      | 135        | MS SQL MultiTable CDC |
| MongoDB     | 140        | MongoDB MultiTable CDC |

## File Structure

```
work/
├── templates/
│   └── cdcf/
│       ├── cdcf_postgres.j2    # PostgreSQL MultiTable template
│       ├── cdcf_oracle.j2       # Oracle MultiTable template
│       ├── cdcf_mssql.j2        # MS SQL MultiTable template
│       └── cdcf_mongo.j2        # MongoDB MultiTable template
├── meta/
│   └── cdcf_metamodel_extended.json  # Extended metamodel with CDCF-specific fields
├── generate_cdcf.py             # Generation script (separate for each source type)
├── docs/
│   └── CDCF_JINJA_TEMPLATES_SPEC.md  # This file
```

## Metamodel Requirements

### Current Metamodel Structure

Метамодель находится в `cids-data.elm.elm.cdcf_elm2kafka_bdpelm_journal.json`:

```json
[
  {
    "current": {
      "tasks": [...],
      "providers": [...],
      "appsystems": [...],
      "metaobjects": [...],
      "workflow_code": "...",
      "workflow_pname": "...",
      "propvalues_json": {
        "metaflow_code": "..."
      },
      ...
    }
  }
]
```

### Required Additional Fields

Для генерации CDCF пайплайнов требуются следующие дополнительные поля в метамодели:

#### 1. Workflow Level (`current.propvalues_json`)

```json
{
  "propvalues_json": {
    "metaflow_code": "mfCDCJournalFlow",
    "cdcf": {
      "source_type": "postgres",  // postgres | oracle | mssql | mongo
      "data_provider_uuid": "0aeb9206-946e-1884-8194-b734bfd61e1e",
      "kafka_connection_uuid": "0ae951b0-95a4-125a-8195-b297b027017f",
      "topic_prefix": "sb1c__sb1c_container_workflow1__",
      "topic_postfix": "journal"
    }
  }
}
```

#### 2. Provider Level (`current.providers[]`)

```json
{
  "providers": [
    {
      "propvalues_json": {
        "component_uuid": "...",
        "dbtype_code": "msSQL",  // msSQL | postgres | oracle | mongo
        "dbtype_version_code": "2019",
        "connection": {
          "hostname": "...",
          "port": "...",
          "database": "..."
        }
      },
      "environments": [...]
    }
  ]
}
```

#### 3. Metaobject Level (`current.metaobjects[]`)

Каждый metaobject должен содержать информацию о таблице:

```json
{
  "metaobjects": [
    {
      "metaobject_code": "...",
      "entitytype_pname": "table_name",
      "application_code": "schema_name",
      "propvalues_json": {
        "table": {
          "name": "table_name",
          "schema": "public",  // или схема для Oracle/MS SQL
          "key_field": "id",   // первичный ключ для mapper
          "full_name": "public.table_name"
        }
      },
      "attributes": [  // опционально, для detailed mapping
        {
          "attribute_pname": "id",
          "attribute_name": "ID",
          "domain_code": "dSourceKeyColumn",
          "tooldatatype_code": "int",
          "basedatatype_code": "bdtInteger"
        }
      ]
    }
  ]
}
```

#### 4. Debezium Properties (`current.propvalues_json.debezium`)

```json
{
  "propvalues_json": {
    "debezium": {
      "snapshot_mode": "never",  // postgres: never, oracle: no_data
      "schema_include_list": "public",  // из application_code
      "table_include_list": ["table1", "table2"],  // из metaobjects
      "publication_name": "cdc_publication",  // postgres only
      "slot_name": "bdp_debezium_slot",  // postgres only
      "database_adapter": "xstream",  // oracle only
      "database_pdb_name": "ESKDDEV",  // oracle only
      "out_server_name": "dbzxout"  // oracle only
    }
  }
}
```

## Template Structure

### Common Pattern for All Templates

```
1. Setup namespaces for nodes
2. Extract flowtasks from flow
3. Extract metaobjects (tables)
4. Generate CDC Source node
5. Generate Mapper node (key extraction)
6. Generate Filter node (table filtering)
7. Generate Kafka Sink node
8. Generate links between nodes
9. Render JSON structure
```

### Template Variables

| Variable | Source | Description |
|----------|--------|-------------|
| `diagram_id` | `flow.propvalues_json.flow_uuid` | Unique diagram ID |
| `schema` | `metaobject.application_code` | Schema name |
| `table_list` | `metaobjects[]` | List of tables to capture |
| `key_field` | `metaobject.propvalues_json.table.key_field` | Primary key field |
| `source_type` | `workflow.propvalues_json.cdcf.source_type` | DB type |
| `data_provider_uuid` | `workflow.propvalues_json.cdcf.data_provider_uuid` | Connection UUID |
| `kafka_connection_uuid` | `workflow.propvalues_json.cdcf.kafka_connection_uuid` | Kafka UUID |
| `topic_prefix` | `workflow.propvalues_json.cdcf.topic_prefix` | Topic prefix |
| `topic_postfix` | `workflow.propvalues_json.cdcf.topic_postfix` | Topic postfix |

## Node Specifications

### 1. CDC Source Node

**PostgreSQL (nodeTypeId: 114):**
```json
{
  "nodeTypeId": 114,
  "jsonProperty": {
    "nodeType": "114",
    "capture_all_tables": false,
    "data_provider_uuid": "{{ data_provider_uuid }}",
    "cdc": {
      "postgres": {
        "slot": {"name": "{{ debezium.slot_name }}"},
        "table": {"name": {{ table_list | to_json }}},
        "decoding": {"plugin": {"name": "pgoutput"}},
        "server": {"time": {"zone": "UTC"}},
        "changelog": {"mode": "all"}
      }
    },
    "debezium": {
      "properties": [
        {"name": "publication.name", "value": "{{ debezium.publication_name }}"},
        {"name": "snapshot.mode", "value": "{{ debezium.snapshot_mode }}"},
        {"name": "schema.include.list", "value": "{{ schema }}"},
        {"name": "heartbeat.interval.ms", "value": "5000"},
        ...
      ]
    }
  }
}
```

**Oracle (nodeTypeId: 116):**
```json
{
  "nodeTypeId": 116,
  "jsonProperty": {
    "nodeType": "116",
    "capture_all_tables": true,
    "data_provider_uuid": "{{ data_provider_uuid }}",
    "cdc": {
      "oracle": {
        "scan": {"startup": {"mode": "LATEST_OFFSET"}},
        "server": {"time": {"zone": "UTC"}}
      }
    },
    "debezium": {
      "properties": [
        {"name": "snapshot.mode", "value": "{{ debezium.snapshot_mode }}"},
        {"name": "database.connection.adapter", "value": "{{ debezium.database_adapter }}"},
        {"name": "database.out.server.name", "value": "{{ debezium.out_server_name }}"},
        {"name": "database.pdb.name", "value": "{{ debezium.database_pdb_name }}"},
        {"name": "schema.include.list", "value": "{{ schema }}"}
      ]
    }
  }
}
```

**MS SQL (nodeTypeId: 135):**
```json
{
  "nodeTypeId": 135,
  "jsonProperty": {
    "nodeType": "135",
    "capture_all_tables": true,
    "data_provider_uuid": "{{ data_provider_uuid }}",
    "cdc": {
      "mssql": {
        "scan": {"startup": {"mode": "LATEST_OFFSET"}},
        "server": {"time": {"zone": "UTC"}}
      }
    },
    "debezium": {
      "properties": [
        {"name": "snapshot.mode", "value": "{{ debezium.snapshot_mode }}"},
        {"name": "schema.include.list", "value": "{{ schema }}"}
      ]
    }
  }
}
```

**MongoDB (nodeTypeId: 140):**
```json
{
  "nodeTypeId": 140,
  "jsonProperty": {
    "nodeType": "140",
    "capture_all_tables": true,
    "data_provider_uuid": "{{ data_provider_uuid }}",
    "cdc": {
      "mongodb": {
        "scan": {"startup": {"mode": "LATEST_OFFSET"}},
        "server": {"time": {"zone": "UTC"}}
      }
    },
    "debezium": {
      "properties": [
        {"name": "snapshot.mode", "value": "{{ debezium.snapshot_mode }}"},
        {"name": "database.include.list", "value": "{{ schema }}"}
      ]
    }
  }
}
```

### 2. Mapper Node (nodeTypeId: 104)

Генерирует expression для извлечения `key_value`:

**PostgreSQL key extraction:**
```sql
cast(
  case when table_name in ({{ table_list | join(', ') }})
  then IF(json_value(data, '$.op') = 'd', json_value(data, '$.before.{{ key_field }}'), json_value(data, '$.after.{{ key_field }}'))
  end as string
)
```

**Oracle/MS SQL key extraction:**
```sql
cast(
case when table_name in ({{ table_list | join(', ') }})
then IF(json_value(data, '$.op') = 'd', json_value(data, '$.before.{{ key_field }}'), json_value(data, '$.after.{{ key_field }}'))
end
as string )
```

**MongoDB key extraction:**
```sql
cast(
  case when table_name in ({{ table_list | join(', ') }})
  then IF(json_value(data, '$.op') = 'd', json_value(data, '$.before._id'), json_value(data, '$.after._id'))
  end as string
)
```

### 3. Filter Node (nodeTypeId: 105)

Фильтрует по списку таблиц (всегда включается):

```json
{
  "nodeTypeId": 105,
  "jsonProperty": {
    "nodeType": "105",
    "filter": "table_name in ({{ table_list | join(', ') }})"
  }
}
```

### 4. Kafka Sink Node (nodeTypeId: 103)

```json
{
  "nodeTypeId": 103,
  "jsonProperty": {
    "nodeType": "103",
    "kafka_connection_uuid": "{{ kafka_connection_uuid }}",
    "kafka": {
      "producer": {
        "semantic": "AT_LEAST_ONCE",
        "topic_selection_type": "dynamic",
        "topic_selector": {
          "prefix": "{{ topic_prefix }}",
          "delimiter": "__",
          "fields": ["topic_body", "postfix"]
        }
      }
    },
    "serde": {
      "key": {
        "enabled": true,
        "format": "csv",
        "key_by": ["key_value"]
      },
      "value": {
        "format": "json"
      }
    }
  }
}
```

## Generation Script

### generate_cdcf.py

Для каждого типа источника создаётся отдельный скрипт:

- `generate_postgres.py`
- `generate_oracle.py`
- `generate_mssql.py`
- `generate_mongo.py`

**Usage:**
```bash
python generate_postgres.py --input meta/cdcf_metamodel_extended.json --output generated/postgres_cdcf.json
```

**Features:**
1. Загружает метамодель через dacite (как в templates_dummy_gen/generate.py)
2. Проверяет наличие обязательных полей
3. Рендерит соответствующий .j2 шаблон
4. При отсутствии обязательных полей - падает с детальной ошибкой

**Error Handling:**
```
MissingRequiredFieldError: Field 'data_provider_uuid' not found in workflow.propvalues_json.cdcf
Required structure:
{
  "propvalues_json": {
    "cdcf": {
      "data_provider_uuid": "<UUID>"
    }
  }
}
```

## Missing Fields Error Handling

При рендеринге шаблона, если поле отсутствует:

```jinja
{%- if workflow.propvalues_json.cdcf is defined -%}
  {%- set data_provider_uuid = workflow.propvalues_json.cdcf.data_provider_uuid -%}
{%- else -%}
  {{ raise("Missing field: workflow.propvalues_json.cdcf.data_provider_uuid") }}
{%- endif -%}
```

Python скрипт также проверяет поля перед рендерингом:

```python
def validate_metamodel(metaobject, source_type):
    required_fields = {
        "postgres": [...],
        "oracle": [...],
        ...
    }
    missing = []
    for field in required_fields[source_type]:
        if not get_nested_value(metaobject, field):
            missing.append(field)
    if missing:
        raise MissingRequiredFieldError(missing)
```

## Output Format

Сгенерированный JSON соответствует структуре:
- `Export_cmCrMngnSi.json` (PostgreSQL)
- `Export_FEdaqUdnKl.json` (Oracle)
- `Export_NedxpyGrSZ.json` (mixed sources)

Ключевые секции:
- `diagrams.{versionId}.diagramWithParameters` - метаданные диаграммы
- `diagrams.{versionId}.nodes[]` - массив узлов пайплайна
- `diagrams.{versionId}.links[]` - связи между узлами
- `rootIdsAndDescriptions` - маппинг ID

## Implementation Priority

1. **Phase 1**: PostgreSQL template (самый простой, с明确的 table list)
2. **Phase 2**: Oracle template (с capture_all_tables и filter)
3. **Phase 3**: MS SQL template (похож на Oracle)
4. **Phase 4**: MongoDB template (особенности с _id)

## Debezium Properties Reference

| Property | PostgreSQL | Oracle | MS SQL | MongoDB |
|----------|-----------|--------|--------|---------|
| snapshot.mode | never | no_data | no_data | no_data |
| schema.include.list | public | SCHEMA | SCHEMA | - |
| database.include.list | - | - | - | DB |
| publication.name | ✓ | - | - | - |
| slot.name | ✓ | - | - | - |
| database.connection.adapter | - | xstream | - | - |
| database.out.server.name | - | ✓ | - | - |
| database.pdb.name | - | ✓ | - | - |

## Notes

1. Все UUID должны быть взяты из метамодели. Генерация новых UUID не допускается.
2. Позиции узлов (`metaInfo.position.x/y`) захардкодены в шаблоне.
3. `jsonPropertyHashCode` - фиксированные значения из примеров.
4. `nodeCounter` в `metaInfo` должен соответствовать количеству узлов.
5. Для Oracle используется `capture_all_tables: true` + filter node для фильтрации.
6. Для PostgreSQL используется `capture_all_tables: false` + список таблиц в source node.
