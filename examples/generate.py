#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDCF Generation Example

Этот скрипт демонстрирует генерацию JSON диаграммы CDCF Journal
из метамодели MS SQL.

Usage:
    python generate.py
"""

import sys
import os

# Добавляем родительскую директорию в path для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from generate_cdcf import load_metamodel, prepare_context, render_template
import json

# Пути к файлам
META_FILE = os.path.join(os.path.dirname(__file__), 'meta', 'cids-data.elm.elm.cdcf_elm2kafka_bdpelm_journal.json')
TEMPLATE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'templates', 'cdcf', 'cdcf_mssql_journal.j2')
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), 'generated', 'elm_cdcf.json')

def main():
    print("=" * 60)
    print("CDCF Generation Example - MS SQL to Kafka")
    print("=" * 60)

    # Загрузка метамодели
    print(f"\n1. Loading metamodel from: {META_FILE}")
    meta = load_metamodel(META_FILE)

    print(f"   Application: {meta.application_code}")
    print(f"   Module: {meta.module_code}")
    print(f"   Workflow: {meta.workflow_code}")
    print(f"   Metaobjects: {len(meta.metaobjects)}")

    # Определение типа источника
    source_type = meta.get_source_type()
    print(f"\n2. Source type: {source_type}")
    print(f"   NodeTypeId: {source_type}")

    # Подготовка контекста
    print(f"\n3. Preparing template context...")
    context = prepare_context(meta)

    # Рендеринг
    print(f"\n4. Rendering template: {os.path.basename(TEMPLATE_FILE)}")
    try:
        output = render_template(TEMPLATE_FILE, context)
    except Exception as e:
        print(f"   ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Валидация JSON
    print(f"\n5. Validating JSON output...")
    try:
        output_json = json.loads(output)
        diagram_id = list(output_json['diagrams'].keys())[0]
        nodes = output_json['diagrams'][diagram_id]['nodes']
        links = output_json['diagrams'][diagram_id]['links']
        print(f"   ✓ JSON is valid")
        print(f"   ✓ diagram_id: {diagram_id}")
        print(f"   ✓ nodes: {len(nodes)}")
        print(f"   ✓ links: {len(links)}")
    except json.JSONDecodeError as e:
        print(f"   WARNING: JSON validation failed: {e}")
        output_json = None

    # Сохранение
    print(f"\n6. Writing output to: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        if output_json is not None:
            json.dump(output_json, f, indent=2, ensure_ascii=False)
        else:
            f.write(output)

    print("\n" + "=" * 60)
    print("✓ Generation complete!")
    print("=" * 60)
    return 0

if __name__ == '__main__':
    sys.exit(main())
