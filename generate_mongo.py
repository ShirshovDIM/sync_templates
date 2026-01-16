# -*- coding: utf-8 -*-
"""
CDCF Generator for MongoDB MultiTable
Generates JSON instructions for MongoDB CDC -> Kafka pipeline
"""

import json
import os
import sys
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, get_origin, get_args
from dacite import from_dict, Config
from jinja2 import Environment, FileSystemLoader, TemplateError


# Paths
METADATA_PATH = os.path.dirname(__file__) + '/meta/'
TEMPLATES_PATH = os.path.dirname(__file__) + '/templates/cdcf/'
GENERATED_PATH = os.path.dirname(__file__) + '/generated/'


class MissingRequiredFieldError(Exception):
    """Raised when required field is missing from metamodel"""
    def __init__(self, missing_fields: List[str]):
        self.missing_fields = missing_fields
        msg = "Missing required fields: " + ', '.join(missing_fields) + "\n\n" + \
              "Required structure:\n" + \
              "workflow.propvalues_json.cdcf {\n" + \
              "  source_type: 'mongo'\n" + \
              "  data_provider_uuid: '<UUID>'\n" + \
              "  kafka_connection_uuid: '<UUID>'\n" + \
              "  topic_prefix: '<prefix>'\n" + \
              "  topic_postfix: '<postfix>'\n" + \
              "  debezium: {\n" + \
              "    snapshot_mode: 'no_data'\n" + \
              "  }\n" + \
              "}"
        super().__init__(msg)


def merge_dicts(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge two dictionaries"""
    result = a.copy()
    for k, v in b.items():
        if k in result:
            if isinstance(result[k], dict) and isinstance(v, dict):
                result[k] = merge_dicts(result[k], v)
            else:
                result[k] = v
        else:
            result[k] = v
    return result


def get_common_structure(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Get common structure from list of dicts"""
    if not items:
        return {}

    all_keys = set()
    for item in items:
        all_keys.update(item.keys())

    common = {}
    for key in all_keys:
        values = [item[key] for item in items if key in item]
        if len(values) < len(items):
            common[key] = None
        else:
            non_null_values = [v for v in values if v is not None]
            if not non_null_values:
                common[key] = None
            elif any(isinstance(v, dict) for v in non_null_values):
                dicts = [v for v in non_null_values if isinstance(v, dict)]
                if dicts:
                    common[key] = get_common_structure(dicts)
                else:
                    common[key] = None
            elif any(isinstance(v, list) for v in non_null_values):
                list_samples = [v for v in non_null_values if isinstance(v, list) and v]
                if list_samples:
                    common[key] = [list_samples[0][0]]
                else:
                    common[key] = []
            else:
                common[key] = non_null_values[0] if non_null_values else None
    return common


def create_dataclass_from_dict(class_name: str, data: Dict[str, Any]) -> type:
    """Recursively create dataclass from dict"""
    fields = {}

    for key, value in data.items():
        if value is None:
            field_type = Optional[Any]
            default = None
        elif isinstance(value, dict):
            nested_class_name = key.title()
            nested_class = create_dataclass_from_dict(nested_class_name, value)
            field_type = Optional[nested_class]
            default = None
        elif isinstance(value, list):
            if len(value) == 0:
                field_type = List[Any]
                fields[key] = (field_type, field(default_factory=list))
                continue

            dicts = [item for item in value if isinstance(item, dict)]
            if dicts:
                common_dict = get_common_structure(dicts)
                item_class_name = key.title()
                item_class = create_dataclass_from_dict(item_class_name, common_dict)
                field_type = List[item_class]
                fields[key] = (field_type, field(default_factory=list))
                continue

            item_types = {type(item) for item in value if item is not None}
            if not item_types:
                field_type = List[Optional[Any]]
            elif len(item_types) == 1:
                field_type = List[item_types.pop()]
            else:
                field_type = List[Any]
            fields[key] = (field_type, field(default_factory=list))
            continue
        else:
            if value is None:
                field_type = Optional[Any]
                default = None
            elif isinstance(value, bool):
                field_type = Optional[bool]
                default = None
            elif isinstance(value, int):
                field_type = Optional[int]
                default = None
            elif isinstance(value, float):
                field_type = Optional[float]
                default = None
            elif isinstance(value, str):
                field_type = Optional[str]
                default = None
            else:
                field_type = Optional[type(value)]
                default = None

        if key in fields:
            continue
        fields[key] = (field_type, field(default=default))

    cls = dataclass(type(class_name, (), {
        '__annotations__': {k: t for k, (t, _) in fields.items()},
    }))
    return cls


def validate_metamodel(workflow: Any) -> List[str]:
    """Validate that all required fields are present in metamodel"""
    required_fields = [
        "propvalues_json.cdcf.source_type",
        "propvalues_json.cdcf.data_provider_uuid",
        "propvalues_json.cdcf.kafka_connection_uuid",
        "propvalues_json.cdcf.topic_prefix",
        "propvalues_json.cdcf.topic_postfix",
        "propvalues_json.cdcf.debezium.snapshot_mode",
    ]

    missing = []
    for field_path in required_fields:
        parts = field_path.split(".")
        value = workflow
        try:
            for part in parts:
                value = getattr(value, part)
                if value is None:
                    missing.append(field_path)
                    break
        except (AttributeError, KeyError):
            missing.append(field_path)

    return missing


def main(metadata_file: str, output_file: str = None):
    """Main generation function"""
    print(f"Loading metamodel from: {metadata_file}")

    # Load JSON data
    with open(metadata_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    # Extract workflow data
    if not isinstance(raw_data, list) or len(raw_data) == 0:
        raise ValueError("Invalid metamodel format: expected non-empty list")

    data = raw_data[0].get('current')
    if not data:
        raise ValueError("Invalid metamodel format: missing 'current' key")

    # Create dataclass from workflow
    WorkflowClass = create_dataclass_from_dict("Workflow", data)

    # Setup dacite config
    def type_hooks_factory(cls):
        hooks = {}

        def is_dataclass_type(tp):
            origin = get_origin(tp)
            if origin is list or origin is List:
                args = get_args(tp)
                if args:
                    inner = args[0]
                    if hasattr(inner, '__dataclass_fields__'):
                        return inner
            return None

        def walk_fields(tp):
            if hasattr(tp, '__dataclass_fields__'):
                for f_name, f_field in tp.__dataclass_fields__.items():
                    field_type = f_field.type
                    inner_cls = is_dataclass_type(field_type)
                    if inner_cls:
                        hooks[field_type] = lambda v, t=inner_cls: [from_dict(t, item, config=config) for item in v]
                        walk_fields(inner_cls)

        walk_fields(cls)
        return hooks

    config = Config(
        type_hooks=type_hooks_factory(WorkflowClass),
        strict=False,
        check_types=False,
        cast=[int, float, str, bool]
    )

    try:
        workflow = from_dict(WorkflowClass, data, config=config)
    except Exception as e:
        raise ValueError(f"Failed to parse metamodel: {e}")

    # Validate metamodel
    missing_fields = validate_metamodel(workflow)
    if missing_fields:
        raise MissingRequiredFieldError(missing_fields)

    print(f"Workflow: {workflow.workflow_code if hasattr(workflow, 'workflow_code') else 'unknown'}")
    print(f"Source type: mongo")
    print(f"Tables found: {len(workflow.metaobjects) if hasattr(workflow, 'metaobjects') and workflow.metaobjects else 0}")

    # Prepare template context
    current_time = datetime.now()
    current_date = current_time.strftime("%Y-%m-%d %H:%M:%S") + f".{current_time.microsecond // 1000:03d}"

    template_context = {
        "workflow": workflow,
        "current": data,
        "current_date": current_date,
    }

    # Setup Jinja environment
    env = Environment(
        loader=FileSystemLoader(TEMPLATES_PATH),
        extensions=['jinja2.ext.do'],
        autoescape=False
    )

    # Load template
    template_name = "cdcf_mongo.j2"
    try:
        template = env.get_template(template_name)
    except Exception as e:
        raise ValueError(f"Failed to load template '{template_name}': {e}")

    # Render template
    try:
        rendered = template.render(**template_context)
    except TemplateError as e:
        raise ValueError(f"Template rendering error: {e}")
    except Exception as e:
        if str(e).startswith("Missing required field:"):
            raise MissingRequiredFieldError([str(e)])
        raise ValueError(f"Template rendering error: {e}")

    # Parse rendered JSON to verify validity
    try:
        rendered_json = json.loads(rendered)
    except json.JSONDecodeError as e:
        raise ValueError(f"Generated invalid JSON: {e}")

    # Determine output file
    if output_file is None:
        workflow_name = workflow.workflow_pname if hasattr(workflow, 'workflow_pname') else 'output'
        output_file = os.path.join(GENERATED_PATH, f"{workflow_name}_mongo_cdcf.json")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Write output
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(rendered)

    print(f"Generated: {output_file}")
    return output_file


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate CDCF JSON for MongoDB MultiTable")
    parser.add_argument('--input', '-i', required=True, help='Path to metamodel JSON file')
    parser.add_argument('--output', '-o', help='Output JSON file path (default: generated/<workflow>_mongo_cdcf.json)')

    args = parser.parse_args()

    try:
        main(args.input, args.output)
    except MissingRequiredFieldError as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
