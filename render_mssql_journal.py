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
    """Парсит метакласс из JSON файла и передаёт как есть для шаблона."""
    with open(metadata_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Извлекаем текущее состояние
    current = data[0].get('current', {})

    # Генерируем diagram_id (это не из метакласса, требуется для рендера)
    diagram_id = str(uuid.uuid4())

    # Текущая дата (это не из метакласса, требуется для рендера)
    current_date = datetime.now()

    # Передаём весь метакласс как есть, без трансформаций
    return {
        'metaclass': current,
        'diagram_id': diagram_id,
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

    metaclass = context['metaclass']
    print(f"Context:")
    print(f"  application_code: {metaclass.get('application_code', '')}")
    print(f"  module_code: {metaclass.get('module_code', '')}")
    # module_pname находится в appsystems[0].applications[0].modules[0].module_pname
    module_pname = ''
    if metaclass.get('appsystems'):
        apps = metaclass['appsystems'][0].get('applications', [])
        if apps:
            modules = apps[0].get('modules', [])
            if modules:
                module_pname = modules[0].get('module_pname', '')
    print(f"  module_pname: {module_pname}")
    print(f"  workflow_pname: {metaclass.get('workflow_pname', '')}")
    print(f"  diagram_id: {context['diagram_id']}")
    print(f"  metaobjects: {len(metaclass.get('metaobjects', []))} objects")

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
