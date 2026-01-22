# Спецификация шаблона CDCF MSSQL Journal

## Обзор
Шаблон `cdcf_mssql_journal.j2` генерирует JSON конфигурацию диаграммы Talys для CDCF (Change Data Capture Flow) потока из MS SQL в Kafka.

## Изменения по сравнению с оригинальным шаблоном

### 1. Исправление структуры JSON
- Исправлены все строки с `\r\n` на `\\n` для валидного JSON
- Добавлены правильные escape последовательности для переносов строк

### 2. nodeId и nodeVersionId
- **Раньше**: nodeId == nodeVersionId (одинаковые UUID)
- **Теперь**: nodeId из `task_uuids` (из метакласса), nodeVersionId генерируется через `uuid_generate()`
- Это соответствует структуре референса `Export_odvCiwrsCC.json`

### 3. row_key для таблиц
- **Раньше**: `row_key: "{{ tbl.full }}__id"` (строка)
- **Теперь**: `row_key: "{{ ns.table_row_keys.get(tbl.full, uuid_generate()) }}"` (случайный UUID для каждой таблицы)
- Генерируется при рендере через `uuid_generate()`

### 4. Debezium properties с row_key
- **Раньше**: properties без row_key
- **Теперь**: каждый property имеет уникальный row_key (UUID)
```json
"properties": [
    {
        "name": "schema.include.list",
        "row_key": "{{ ns.debezium_schema_row_key }}",
        "value": "dbo"
    },
    ...
]
```

### 5. Link IDs
- **Раньше**: жёстко заданные UUID плейсхолдеры
- **Теперь**: берётся из `transition_uuids` или генерируется новый UUID
- Использует `task_from_code` и `task_to_code` для составления ключа

### 6. Параметры шаблона
Параметры, которые должны передаваться при рендере:

| Параметр | Тип | Описание | Источник |
|----------|-----|----------|----------|
| `provider_uuid` | str | UUID провайдера данных | `modules[].propvalues_json.data_provider_uuid` |
| `diagram_id` | str | UUID диаграммы | Генерируется новый при рендере |
| `task_uuids` | dict | Словарь `{task_pname: uuid}` | `tasks[].propvalues_json.task_uuid` |
| `transition_uuids` | dict | Словарь `{from__to: uuid}` | `task_transitions[].propvalues_json.flowtransition_uuid` |
| `tables` | list | Список таблиц `[{'schema': ..., 'table': ..., 'full': ...}]` | Из параметра или из `metaobjects` |
| `app_code` | str | Код приложения (например, 'contact') | `application_code` |
| `module_code` | str | Код модуля (например, 'contact2_web') | `module_pname` |
| `current_date` | datetime | Текущая дата/время | `datetime.now()` |

### 7. Глобальная функция uuid_generate
При рендере необходимо добавить в Jinja окружение:

```python
env.globals['uuid_generate'] = lambda: str(uuid.uuid4())
```

## Структура генерируемой диаграммы

### Узлы (nodes)
1. **Источник - CDC - MS SQL MultiTable** (nodeTypeId: 135)
   - nodeId: из `task_uuids['read_db_log']`
   - nodeVersionId: генерируется новый UUID
   - tables: с уникальными row_key для каждой таблицы

2. **Трансформация - Простая - Маппинг** (nodeTypeId: 104)
   - nodeId: из `task_uuids['split_data']`
   - nodeVersionId: генерируется новый UUID
   - Появляется только если `tables | length > 0`

3. **Трансформация - Фильтр** (nodeTypeId: 105)
   - nodeId: из `task_uuids['produce_data']`
   - nodeVersionId: генерируется новый UUID
   - Появляется только если `tables | length > 0`

4. **Приёмник - Брокер сообщений - Kafka** (nodeTypeId: 103)
   - nodeId: `kafka__{app_code}__{module_code}`
   - nodeVersionId: `fa789b5b-dddf-43c1-9816-081ec38f28a9` (фиксированный)

### Связи (links)
- read_db_log -> split_data (если есть таблицы)
- split_data -> produce_data (если есть таблицы)
- produce_data -> kafka (или read_db_log -> kafka если нет таблиц)

## Пример использования

```python
from jinja2 import Environment, FileSystemLoader
import uuid
from datetime import datetime

env = Environment(loader=FileSystemLoader('templates/cdcf'))
env.globals['uuid_generate'] = lambda: str(uuid.uuid4())

context = {
    'provider_uuid': '8876ea2f-b5ad-453f-9b85-03098efe30c8',
    'diagram_id': str(uuid.uuid4()),
    'task_uuids': {
        'read_db_log': 'b5453bb5-f808-581f-b2ef-c167bc374f9b',
        'split_data': '87d26546-b18c-5395-b540-e7d1b97b1be9',
        'produce_data': '095f24b9-9278-541a-a82c-0cb9c0ae2771',
    },
    'transition_uuids': {},
    'tables': [
        {'schema': 'dbo', 'table': 'contact_log', 'full': 'dbo.contact_log'},
    ],
    'app_code': 'contact',
    'module_code': 'contact2_web',
    'current_date': datetime.now(),
}

template = env.get_template('cdcf_mssql_journal.j2')
result = template.render(**context)
```

## Примечания
- Шаблон универсальный и подходит для любого MSSQL источника
- nodeCounter = 3 когда есть таблицы, иначе 2
- Позиции узлов жёстко заданы как в референсе
- jsonPropertyHashCode совпадает с референсом
