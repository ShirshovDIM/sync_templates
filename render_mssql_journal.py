# -*- coding: utf-8 -*-
"""
Рендер CDCF Journal шаблона для MSSQL из метакласса.

Usage:
    python render_mssql_journal.py <metadata_file> <output_file>
"""
import json
import sys
import uuid
from datetime import datetime
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


def parse_metaclass(metadata_path: str) -> dict:
    """Парсит метакласс из JSON файла и извлекает нужные параметры."""
    with open(metadata_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Извлекаем текущее состояние
    current = data[0].get('current', {})

    # Извлекаем задачи и их UUID
    tasks = current.get('tasks', [])
    task_uuids = {}
    for task in tasks:
        task_pname = task.get('task_pname', '')
        task_uuid = task.get('propvalues_json', {}).get('task_uuid', '')
        if task_pname and task_uuid:
            task_uuids[task_pname] = task_uuid

    # Извлекаем переходы и их UUID
    transitions = current.get('task_transitions', [])
    transition_uuids = {}
    for trans in transitions:
        from_code = trans.get('task_from_code', '').split('.')[-1]
        to_code = trans.get('task_to_code', '').split('.')[-1]
        key = f"{from_code}__{to_code}"
        propvalues = trans.get('propvalues_json')
        if propvalues:
            trans_uuid = propvalues.get('flowtransition_uuid', str(uuid.uuid4()))
        else:
            trans_uuid = str(uuid.uuid4())
        transition_uuids[key] = trans_uuid

    # Извлекаем provider_uuid из modules
    appsystems = current.get('appsystems', [])
    provider_uuid = None
    module_uuid = None
    data_provider_uuid = None
    module_pname = None
    application_code = None

    for appsys in appsystems:
        if appsys.get('domain_code') == 'dBusinessSystem':
            applications = appsys.get('applications', [])
            for app in applications:
                application_code = app.get('application_code')
                modules = app.get('modules', [])
                for module in modules:
                    module_pname = module.get('module_pname', '')
                    module_uuid = module.get('propvalues_json', {}).get('module_uuid')
                    data_provider_uuid = module.get('propvalues_json', {}).get('data_provider_uuid')
                    if data_provider_uuid:
                        provider_uuid = data_provider_uuid
                    break

    # Извлекаем таблицы из metaobjects
    metaobjects = current.get('metaobjects', [])
    tables = []
    for obj in metaobjects:
        entitytype_pname = obj.get('entitytype_pname', '')
        if entitytype_pname:
            # Парсим schema__table формат
            parts = entitytype_pname.split('__')
            if len(parts) == 2:
                schema, table = parts
                tables.append({
                    'schema': schema,
                    'table': table,
                    'full': f"{schema}.{table}"
                })

    # Генерируем diagram_id
    diagram_id = str(uuid.uuid4())

    # Текущая дата
    current_date = datetime.now()

    # Код приложения и модуля
    app_code = application_code or current.get('application_code', 'app')
    mod_code = module_pname or current.get('module_code', 'module')

    return {
        'provider_uuid': provider_uuid or str(uuid.uuid4()),
        'diagram_id': diagram_id,
        'task_uuids': task_uuids,
        'transition_uuids': transition_uuids,
        'tables': tables,
        'app_code': app_code,
        'module_code': mod_code.replace(app_code + '__', '') if '__' in mod_code else mod_code,
        'current_date': current_date,
    }


def render_template(template_path: str, context: dict) -> str:
    """Рендерит Jinja шаблон с контекстом."""
    template_dir = Path(template_path).parent
    template_name = Path(template_path).name

    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        extensions=['jinja2.ext.do']
    )

    # Добавляем uuid_generate как глобальную функцию (не фильтр)
    env.globals['uuid_generate'] = lambda: str(uuid.uuid4())

    template = env.get_template(template_name)
    return template.render(**context)


def main():
    if len(sys.argv) < 3:
        print("Usage: python render_mssql_journal.py <metadata_file> <output_file>")
        sys.exit(1)

    metadata_file = sys.argv[1]
    output_file = sys.argv[2]
    template_path = Path(__file__).parent / 'templates' / 'cdcf' / 'cdcf_mssql_journal.j2'

    print(f"Parsing metaclass from: {metadata_file}")
    context = parse_metaclass(metadata_file)

    print(f"Context:")
    print(f"  app_code: {context['app_code']}")
    print(f"  module_code: {context['module_code']}")
    print(f"  provider_uuid: {context['provider_uuid']}")
    print(f"  diagram_id: {context['diagram_id']}")
    print(f"  task_uuids: {list(context['task_uuids'].keys())}")
    print(f"  tables: {len(context['tables'])} tables")

    print(f"\nRendering template: {template_path}")
    rendered = render_template(str(template_path), context)

    # Валидация JSON
    try:
        json.loads(rendered)
        print("JSON validation: OK")
    except json.JSONDecodeError as e:
        print(f"JSON validation: FAILED - {e}")
        sys.exit(1)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(rendered)

    print(f"Output written to: {output_file}")


if __name__ == '__main__':
    main()
