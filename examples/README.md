# CDCF Generation Examples

Эта директория содержит примеры генерации CDCF Journal JSON диаграмм из метамоделей.

## Структура

```
examples/
├── meta/           # Метамодели для генерации
├── generated/      # Результаты генерации
├── generate.py     # Скрипт генерации
└── README.md       # Этот файл
```

## Пример: MS SQL to Kafka

### Описание

Генерация CDCF Journal pipeline для захвата изменений из MS SQL базы данных
и отправки в Kafka.

### Метамодель

- **Файл**: `meta/cids-data.elm.elm.cdcf_elm2kafka_bdpelm_journal.json`
- **Система**: elm (EGAR Limits Manager)
- **Таблицы**:
  - `dbo.lz_agreement_types`
  - `dbo.lz_books`

### Запуск генерации

```bash
cd examples
python generate.py
```

### Результат

Сгенерированный JSON диаграмма сохраняется в `generated/elm_cdcf.json`.

#### Структура pipeline:

1. **Источник CDC (MS SQL MultiTable)** - nodeTypeId: 135
   - Захват изменений из MS SQL
   - Провайдер: `7ea2de0e-b9cd-46fa-86a2-bd483edb90eb`

2. **Маппинг** - nodeTypeId: 104
   - Извлечение ключа (key_value)
   - Формирование имени топика

3. **Фильтр** - nodeTypeId: 105
   - Фильтрация по списку таблиц

4. **Kafka Sink** - nodeTypeId: 103
   - Отправка в Kafka
   - Динамический выбор топика

### Связи (links)

```
read_db_log -> split_data -> produce_data -> Kafka
```

## Добавление новых примеров

Чтобы добавить новый пример:

1. Поместите метамодель в `meta/`
2. Запустите генерацию: `python generate.py`
3. Результат будет в `generated/`
4. Обновите этот README
