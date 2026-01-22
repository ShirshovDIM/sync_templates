# -*- coding: utf-8 -*-
"""
Тест рендера с полным списком таблиц из референса.
"""
import json
import uuid
from datetime import datetime
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


# Таблицы из референса
TABLES = [
    {'schema': 'dbo', 'table': 'appeal', 'full': 'dbo.appeal'},
    {'schema': 'dbo', 'table': 'appeal_entity', 'full': 'dbo.appeal_entity'},
    {'schema': 'dbo', 'table': 'cf_appeal_0', 'full': 'dbo.cf_appeal_0'},
    {'schema': 'dbo', 'table': 'contact_log', 'full': 'dbo.contact_log'},
    {'schema': 'dbo', 'table': 'guarantee_contract', 'full': 'dbo.guarantee_contract'},
    {'schema': 'dbo', 'table': 'guarantor', 'full': 'dbo.guarantor'},
    {'schema': 'dbo', 'table': 'person', 'full': 'dbo.person'},
    {'schema': 'dbo', 'table': 'cf_property_0', 'full': 'dbo.cf_property_0'},
    {'schema': 'dbo', 'table': 'property', 'full': 'dbo.property'},
    {'schema': 'dbo', 'table': 'payment_promise', 'full': 'dbo.payment_promise'},
    {'schema': 'dbo', 'table': 'debt', 'full': 'dbo.debt'},
    {'schema': 'dbo', 'table': 'debt_promise', 'full': 'dbo.debt_promise'},
    {'schema': 'dbo', 'table': 'dict_name', 'full': 'dbo.dict_name'},
    {'schema': 'dbo', 'table': 'dict_term', 'full': 'dbo.dict_term'},
    {'schema': 'dbo', 'table': 'cf_phone_0', 'full': 'dbo.cf_phone_0'},
]

# Task UUIDs из метакласса
TASK_UUIDS = {
    'read_db_log': 'b5453bb5-f808-581f-b2ef-c167bc374f9b',
    'split_data': '87d26546-b18c-5395-b540-e7d1b97b1be9',
    'produce_data': '095f24b9-9278-541a-a82c-0cb9c0ae2771',
}

CONTEXT = {
    'provider_uuid': '8876ea2f-b5ad-453f-9b85-03098efe30c8',
    'diagram_id': str(uuid.uuid4()),
    'task_uuids': TASK_UUIDS,
    'transition_uuids': {},
    'tables': TABLES,
    'app_code': 'contact',
    'module_code': 'contact2_web',
    'current_date': datetime.now(),
}


def render_template(template_path: str, context: dict) -> str:
    """Рендерит Jinja шаблон с контекстом."""
    template_dir = Path(template_path).parent
    template_name = Path(template_path).name

    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        extensions=['jinja2.ext.do']
    )

    # Добавляем uuid_generate как глобальную функцию
    env.globals['uuid_generate'] = lambda: str(uuid.uuid4())

    template = env.get_template(template_name)
    return template.render(**context)


def main():
    template_path = Path(__file__).parent / 'templates' / 'cdcf' / 'cdcf_mssql_journal.j2'
    output_path = Path(__file__).parent / 'test_render_full_tables.json'

    print(f"Rendering template with {len(TABLES)} tables...")
    rendered = render_template(str(template_path), CONTEXT)

    # Валидация JSON
    try:
        data = json.loads(rendered)
        print("JSON validation: OK")

        # Проверка количества узлов
        diagram = list(data['diagrams'].values())[0]
        nodes = diagram['nodes']
        print(f"Nodes count: {len(nodes)}")

        # Проверка таблиц в CDC узле
        cdc_node = next((n for n in nodes if n['nodeTypeId'] == 135), None)
        if cdc_node:
            tables_count = len(cdc_node['jsonProperty']['tables'])
            print(f"Tables in CDC node: {tables_count}")
            print(f"Expected: {len(TABLES)}")

    except json.JSONDecodeError as e:
        print(f"JSON validation: FAILED - {e}")
        return

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(rendered)

    print(f"Output written to: {output_path}")


if __name__ == '__main__':
    main()
