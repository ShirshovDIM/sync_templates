# -*- coding: utf-8 -*-
"""
CDCF Journal Generator

Генератор JSON-инструкций CDCF Journal потоков на основе метамодели.

Usage:
    python generate_cdcf.py --meta <path_to_meta.json> --output <path_to_output.json>
    python generate_cdcf.py --meta <path_to_meta.json> --template mssql --output <path_to_output.json>
"""

import json
import os
import sys
import argparse
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined


# Типы источников CDC
SOURCE_TYPES = {
    'tMssql.2019': 'mssql',
    'tMssql.2022': 'mssql',
    'tPostgres': 'postgres',
    'tPostgreSQL': 'postgres',
    'tOracle': 'oracle',
    'tOracle.19': 'oracle',
    'tMongoDB': 'mongo',
    'tMongo': 'mongo',
}

NODE_TYPE_IDS = {
    'mssql': 135,
    'postgres': 114,
    'oracle': 116,
    'mongo': 140,
}

TEMPLATE_FILES = {
    'mssql': 'cdcf_mssql_journal.j2',
    'postgres': 'cdcf_postgres_journal.j2',
    'oracle': 'cdcf_oracle_journal.j2',
    'mongo': 'cdcf_mongo_journal.j2',
}


@dataclass
class MetaModel:
    """Метамодель из JSON файла."""

    application_code: str
    module_code: str
    workflow_code: str
    workflow_name: str
    metaclass_code: str
    propvalues_json: Dict[str, Any]

    tasks: List[Dict[str, Any]] = field(default_factory=list)
    providers: List[Dict[str, Any]] = field(default_factory=list)
    appsystems: List[Dict[str, Any]] = field(default_factory=list)
    metaobjects: List[Dict[str, Any]] = field(default_factory=list)
    task_transitions: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MetaModel':
        """Создание из словаря."""
        # Поддержка разных форматов входных данных
        if isinstance(data, list) and len(data) > 0:
            current = data[0].get('current', data[0])
        else:
            current = data.get('current', data)

        propvalues = current.get('propvalues_json', {})

        return cls(
            application_code=current.get('application_code', ''),
            module_code=current.get('module_code', ''),
            workflow_code=current.get('workflow_code', ''),
            workflow_name=current.get('workflow_name', ''),
            metaclass_code=current.get('metaclass_code', ''),
            propvalues_json=propvalues,
            tasks=current.get('tasks', []),
            providers=current.get('providers', []),
            appsystems=current.get('appsystems', []),
            metaobjects=current.get('metaobjects', []),
            task_transitions=current.get('task_transitions', []),
        )

    def get_source_type(self) -> Optional[str]:
        """Определение типа источника по провайдерам."""
        for provider in self.providers:
            tool_code = provider.get('tool_code', '')
            for pattern, source_type in SOURCE_TYPES.items():
                if pattern in tool_code:
                    return source_type
        return None

    def get_provider_uuid(self) -> str:
        """UUID провайдера данных."""
        for provider in self.providers:
            if provider.get('domain_code') == 'dComponent':
                uuid = provider.get('tool_uuid',
                         provider.get('propvalues_json', {}).get('component_uuid', ''))
                if uuid:
                    return uuid
        return ''

    def get_source_settings_id(self) -> str:
        """source_settings_id из провайдера."""
        for provider in self.providers:
            if provider.get('domain_code') == 'dComponent':
                return provider.get('propvalues_json', {}).get('component_uuid', '')
        return ''

    def get_source_id(self) -> str:
        """sourceId из appsystems[].applications[].modules[].module_uuid."""
        for appsystem in self.appsystems:
            for application in appsystem.get('applications', []):
                for module in application.get('modules', []):
                    if module.get('propvalues_json'):
                        uuid = module['propvalues_json'].get('module_uuid', '')
                        if uuid:
                            return uuid
        return ''

    def get_diagram_id(self) -> str:
        """diagram_id из propvalues_json.flow_uuid или tasks[0].task_uuid."""
        # Сначала проверяем flow_uuid в propvalues_json workflow
        flow_uuid = self.propvalues_json.get('flow_uuid', '')
        if flow_uuid:
            return flow_uuid

        # Фолбэк: используем task_uuid первой задачи
        if self.tasks and len(self.tasks) > 0:
            task = self.tasks[0]
            return task.get('propvalues_json', {}).get('task_uuid',
                   task.get('task_uuid',
                   '00000000-0000-0000-0000-000000000000'))
        return '00000000-0000-0000-0000-000000000000'

    def get_task_uuids(self) -> Dict[str, str]:
        """UUID задач по их именам."""
        result = {}
        for task in self.tasks:
            task_pname = task.get('task_pname', '')
            task_uuid = task.get('propvalues_json', {}).get('task_uuid', task.get('task_uuid', ''))
            if task_pname and task_uuid:
                result[task_pname] = task_uuid
        return result

    def get_task_codes(self) -> Dict[str, str]:
        """Коды задач по их именам."""
        result = {}
        for task in self.tasks:
            task_pname = task.get('task_pname', '')
            task_code = task.get('task_code', '')
            if task_pname and task_code:
                result[task_pname] = task_code
        return result

    def get_transition_uuids(self) -> Dict[str, str]:
        """UUID переходов (links).

        Создает маппинг из task_pname в flowtransition_uuid.
        Связь: task_transitions.task_from_code -> tasks.metatask_code
        """
        # Создадим маппинг metatask_code -> task_pname
        metatask_to_pname = {}
        for task in self.tasks:
            metatask_code = task.get('metatask_code', '')
            task_pname = task.get('task_pname', '')
            if metatask_code and task_pname:
                # Извлекаем последнюю часть metatask_code (например, mmReadTransactionLog)
                metatask_suffix = metatask_code.split('.')[-1]
                metatask_to_pname[metatask_suffix] = task_pname

        result = {}
        for transition in self.task_transitions:
            if transition.get('propvalues_json'):
                uuid_val = transition['propvalues_json'].get('flowtransition_uuid', '')
                if uuid_val:
                    # Извлекаем суффиксы из task_from_code и task_to_code
                    from_suffix = transition.get('task_from_code', '').split('.')[-1]
                    to_code = transition.get('task_to_code', '')

                    # Special case для kafka_sink
                    if to_code == 'kafka_sink':
                        to_pname = 'kafka'
                    else:
                        to_suffix = to_code.split('.')[-1]
                        to_pname = metatask_to_pname.get(to_suffix, to_suffix)

                    # Получаем task_pname из суффиксов
                    from_pname = metatask_to_pname.get(from_suffix, from_suffix)

                    # Создаем ключ в формате task_pname__task_pname
                    key = from_pname + '__' + to_pname
                    result[key] = uuid_val

        return result

    def get_tables(self) -> List[Dict[str, str]]:
        """Список таблиц из metaobjects."""
        tables = []
        for obj in self.metaobjects:
            entitytype = obj.get('entitytype_pname', '')
            if entitytype:
                parts = entitytype.split('__')
                if len(parts) == 2:
                    tables.append({'schema': parts[0], 'table': parts[1], 'full': parts[0] + '.' + parts[1]})
                else:
                    # Fallback: application_code как схема
                    schema = self.application_code
                    tables.append({'schema': schema, 'table': entitytype, 'full': schema + '.' + entitytype})
        return tables


def load_metamodel(file_path: str) -> MetaModel:
    """Загрузка метамодели из JSON файла."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return MetaModel.from_dict(data)


def prepare_context(meta: MetaModel) -> Dict[str, Any]:
    """Подготовка контекста для рендеринга шаблона."""
    # Извлекаем все UUID в Python (избегаем Jinja2 scoping проблем)
    provider_uuid = meta.get_provider_uuid()
    source_settings_id = meta.get_source_settings_id()
    source_id = meta.get_source_id()
    diagram_id = meta.get_diagram_id()
    task_uuids = meta.get_task_uuids()
    task_codes = meta.get_task_codes()
    transition_uuids = meta.get_transition_uuids()
    tables = meta.get_tables()

    # Для отладки
    import sys
    print(f"  Extracted UUIDs:")
    print(f"    provider_uuid: {provider_uuid}")
    print(f"    source_settings_id: {source_settings_id}")
    print(f"    source_id: {source_id}")
    print(f"    diagram_id: {diagram_id} (source: flow_uuid={meta.propvalues_json.get('flow_uuid', 'MISSING')})")
    print(f"    tasks: {list(task_uuids.keys())}")
    print(f"    tables: {len(tables)}")

    return {
        'application_code': meta.application_code,
        'module_code': meta.module_code,
        'workflow_code': meta.workflow_code,
        'workflow_name': meta.workflow_name,
        'metaclass_code': meta.metaclass_code,
        'propvalues_json': meta.propvalues_json,
        'providers': meta.providers,
        'appsystems': meta.appsystems,
        'metaobjects': meta.metaobjects,
        'tasks': meta.tasks,
        'task_transitions': meta.task_transitions,
        'flow': meta,  # Передаем весь объект для доступа из шаблона
        'current_date': datetime.now(),
        # Извлеченные UUID (готовые для использования в шаблоне)
        'provider_uuid': provider_uuid,
        'source_settings_id': source_settings_id,
        'source_id': source_id,
        'diagram_id': diagram_id,
        'task_uuids': task_uuids,
        'task_codes': task_codes,
        'transition_uuids': transition_uuids,
        'tables': tables,
    }


def render_template(template_path: str, context: Dict[str, Any]) -> str:
    """Рендеринг Jinja шаблона."""
    template_dir = os.path.dirname(template_path)
    template_name = os.path.basename(template_path)

    env = Environment(
        loader=FileSystemLoader(template_dir),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )

    template = env.get_template(template_name)
    return template.render(**context)


def main():
    parser = argparse.ArgumentParser(
        description='Генератор CDCF Journal JSON инструкций из метамодели'
    )
    parser.add_argument(
        '--meta',
        required=True,
        help='Путь к файлу метамодели (JSON)'
    )
    parser.add_argument(
        '--template',
        choices=['mssql', 'postgres', 'oracle', 'mongo', 'auto'],
        default='auto',
        help='Тип шаблона для генерации (auto - определение автоматически)'
    )
    parser.add_argument(
        '--templates-dir',
        default='templates/cdcf',
        help='Директория с Jinja шаблонами'
    )
    parser.add_argument(
        '--output', '-o',
        default=None,
        help='Путь к выходному JSON файлу (по умолчанию: <meta>_cdcf.json)'
    )
    parser.add_argument(
        '--pretty',
        action='store_true',
        help='Форматировать вывод JSON с отступами'
    )

    args = parser.parse_args()

    # Загрузка метамодели
    print(f"Loading metamodel from: {args.meta}")
    meta = load_metamodel(args.meta)

    print(f"  Application: {meta.application_code}")
    print(f"  Module: {meta.module_code}")
    print(f"  Workflow: {meta.workflow_code}")
    print(f"  Metaobjects: {len(meta.metaobjects)}")

    # Определение типа источника
    source_type = args.template
    if source_type == 'auto':
        source_type = meta.get_source_type()
        if source_type is None:
            print("ERROR: Could not determine source type from providers!")
            print("Available providers:")
            for p in meta.providers:
                print(f"  - {p.get('tool_code', 'unknown')}")
            sys.exit(1)

    print(f"  Source type: {source_type}")
    print(f"  NodeTypeId: {NODE_TYPE_IDS.get(source_type, 'unknown')}")

    # Путь к шаблону
    template_file = TEMPLATE_FILES.get(source_type)
    if template_file is None:
        print(f"ERROR: Unknown source type: {source_type}")
        sys.exit(1)

    template_path = os.path.join(args.templates_dir, template_file)
    if not os.path.exists(template_path):
        print(f"ERROR: Template not found: {template_path}")
        sys.exit(1)

    print(f"  Template: {template_file}")

    # Подготовка контекста
    context = prepare_context(meta)

    # Рендеринг
    print(f"Rendering template...")
    try:
        output = render_template(template_path, context)
    except Exception as e:
        print(f"ERROR: Template rendering failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Парсинг результата для валидации JSON
    output_json = None
    try:
        output_json = json.loads(output)
    except json.JSONDecodeError as e:
        print(f"WARNING: Generated JSON is not valid: {e}")

    # Определение пути вывода
    if args.output is None:
        meta_path = Path(args.meta)
        args.output = str(meta_path.parent / f"{meta_path.stem}_cdcf.json")

    # Сохранение
    print(f"Writing output to: {args.output}")
    with open(args.output, 'w', encoding='utf-8') as f:
        if args.pretty and output_json is not None:
            json.dump(output_json, f, indent=2, ensure_ascii=False)
        else:
            f.write(output)

    print("Done!")


if __name__ == '__main__':
    main()
