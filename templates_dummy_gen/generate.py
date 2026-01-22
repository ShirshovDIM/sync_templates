# -*- coding: utf-8 -*-
import json
import os
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, get_origin, get_args
from dacite import from_dict, Config
from jinja2 import Environment, FileSystemLoader


# Пути к данным
METADATA_PATH = os.path.dirname(__file__) + '/data/'

# Соответствие имён для генерации классов
NAMES = {
    'structures': 'Structure',
    'attributes': 'Attribute',
    'indexes': 'Index',
    'index_attributes': 'IndexAttribute',
    'partitions': 'Partition',
    'partition_attributes': 'PartitionAttribute',
    'flows': 'Flow',
    'flowtasks': 'FlowTask',
    'flowtask_structures': 'FlowTaskStructure',
    'flowtransitions': 'FlowTransition',
    'flowtask_params': 'FlowTaskParam',
    'propvalues_json': 'Properties',
}


def merge_dicts(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """
    Объединяет два словаря, рекурсивно объединяя вложенные словари.
    Если ключ есть в обоих, но типы разные (не dict), оставляет Optional.
    """
    result = a.copy()
    for k, v in b.items():
        if k in result:
            if isinstance(result[k], dict) and isinstance(v, dict):
                result[k] = merge_dicts(result[k], v)
            else:
                # Если значения разные — помечаем как Optional[Any] (упрощение)
                result[k] = None  # будет Optional[Any]
        else:
            result[k] = v
    return result


def get_common_structure(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Возвращает обобщённую структуру, где все поля, которые не везде есть, становятся Optional (через None).
    """
    if not items:
        return {}

    # Собираем все ключи
    all_keys = set()
    for item in items:
        all_keys.update(item.keys())

    common = {}
    for key in all_keys:
        values = [item[key] for item in items if key in item]

        # Если ключ не везде — значит, он Optional
        if len(values) < len(items):
            common[key] = None  # будет Optional
        else:
            # Анализируем типы
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
                # Берём первый непустой список для анализа
                list_samples = [v for v in non_null_values if isinstance(v, list) and v]
                if list_samples:
                    common[key] = [list_samples[0][0]]  # Упрощённо: берём образец
                else:
                    common[key] = []
            else:
                # Берём первый непустой тип
                common[key] = non_null_values[0] if non_null_values else None
    return common


def create_dataclass_from_dict(class_name: str, data: Dict[str, Any]) -> type:
    """
    Рекурсивно создаёт dataclass на основе словаря.
    Для списков анализирует все элементы.
    """
    fields = {}

    for key, value in data.items():
        if value is None:
            field_type = Optional[Any]
            default = None
        elif isinstance(value, dict):
            nested_class_name = NAMES.get(key, key.title())
            nested_class = create_dataclass_from_dict(nested_class_name, value)
            field_type = Optional[nested_class]
            default = None
        elif isinstance(value, list):
            if len(value) == 0:
                field_type = List[Any]
                fields[key] = (field_type, field(default_factory=list))
                continue

            # Анализируем все элементы списка
            dicts = [item for item in value if isinstance(item, dict)]
            if dicts:
                common_dict = get_common_structure(dicts)
                item_class_name = NAMES.get(key, key.title())
                item_class = create_dataclass_from_dict(item_class_name, common_dict)
                field_type = List[item_class]
                fields[key] = (field_type, field(default_factory=list))
                continue

            # Если не словари — берём общий тип
            item_types = {type(item) for item in value if item is not None}
            if not item_types:
                field_type = List[Optional[Any]]
            elif len(item_types) == 1:
                field_type = List[item_types.pop()]
            else:
                field_type = List[Any]  # Неоднородный список
            fields[key] = (field_type, field(default_factory=list))
            continue
        else:
            if value is None:
                field_type = Optional[Any]
                default = None
            elif isinstance(value, bool):
                field_type = Optional[bool]  # даже если True/False, может быть null
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

        # Установка значения по умолчанию
        if key in fields:
            continue  # Уже обработано в ветке list
        fields[key] = (field_type, field(default=default))

    # Создание класса
    cls = dataclass(type(class_name, (), {
        '__annotations__': {k: t for k, (t, _) in fields.items()},
    }))
    return cls


def main(metadata_file: str):
    # Загрузка JSON-данных
    with open(metadata_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    # Предполагаем, что данные находятся в data[0]['current']
    data = raw_data[0]['current']

    # Создание корневого dataclass
    Metaobject = create_dataclass_from_dict("Metaobject", data)

    # Настройка dacite
    # --- 🔧 ВАЖНО: Настраиваем Config для рекурсивной обработки списков ---
    def type_hooks_factory(cls):
        """Рекурсивно создаёт type_hooks для всех вложенных списков."""
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
                        hooks[field_type] = lambda v, t=inner_cls: [from_dict(t, item, config) for item in v]
                        walk_fields(inner_cls)  # рекурсия

        walk_fields(cls)
        return hooks

    config = Config(
        type_hooks=type_hooks_factory(Metaobject),
        strict=False,
        check_types=False,
        cast=[int, float, str, bool]
    )

    try:
        metaobject = from_dict(Metaobject, data, config=config)
        print("✅ Успешно создан объект Metaobject:")
        print(repr(metaobject)[:500] + "..." if len(repr(metaobject)) > 500 else repr(metaobject))
    except Exception as e:
        raise e

    return metaobject


@dataclass
class DiagramMeta:
    diagram_version_id: str
    diagram_id: str
    entity_id: str
    diagram_name: str
    target_table_name: str
    schema: str
    sysdate: str

    consume_data_id: str
    consume_data_version_id: str

    protect_data_id: str
    protect_data_version_id: str

    load_target_id: str
    load_target_version_id: str

    source_schema_id: str
    source_schema_version_id: str
    mask_schema_id: str
    mask_schema_version_id: str

    source_attribute_list: List[str]
    target_attribute_list: List[str]
    protection_script: List[str]

    consume_sensitive_flag_id: Optional[str] = None
    consume_sensitive_flag_version_id: Optional[str] = None

    union_sensitivecheck_id: Optional[str] = None
    union_sensitivecheck_version_id: Optional[str] = None

    check_sensitiverule_id: Optional[str] = None
    check_sensitiverule_version_id: Optional[str] = None

    enrich_techcols_id: Optional[str] = None
    enrich_techcols_version_id: Optional[str] = None

    enrich_techcols_cdc_id: Optional[str] = None
    enrich_techcols_cdc_version_id: Optional[str] = None

    enrich_techcols_rdo_id: Optional[str] = None
    enrich_techcols_rdo_version_id: Optional[str] = None

    load_target_cdc_id: Optional[str] = None
    load_target_cdc_version_id: Optional[str] = None

    uuid_link_id_1: Optional[str] = None
    uuid_link_id_2: Optional[str] = None
    uuid_link_id_3: Optional[str] = None
    uuid_link_id_4: Optional[str] = None
    uuid_link_id_5: Optional[str] = None
    uuid_link_id_6: Optional[str] = None
    uuid_link_id_7: Optional[str] = None
    uuid_link_id_8: Optional[str] = None

    @classmethod
    def from_metaclass(
        cls,
        metaclass,
        type: str = "initial"
    ) -> "DiagramMeta":
        if type == "initial":
            return cls._build_inital(metaclass, type)
        else:
            return cls._build_journal(metaclass, type)

    @classmethod
    def _build_inital(cls, metaclass, type):
        flow = cls._extract_flow(metaclass, type)
        nodes = cls._map_nodes(flow)

        schema_name = metaclass.application_code
        table_name = metaclass.entitytype_pname

        current_time = datetime.now()
        sysdate = current_time.strftime("%Y-%m-%d %H:%M:%S") + f".{current_time.microsecond // 1000:03d}"

        diagram_version_id = flow.propvalues_json.flow_uuid
        diagram_name = f"{schema_name.upper()}_INITIAL_{table_name.upper()}"

        protect_task = cls._fetch_protect_task(flow.flowtasks)
        source_attributes = cls._fetch_source_attributes(
            flowtask_structures=protect_task.flowtask_structures
        )

        target_attributes = cls._fetch_target_attributes(
            flowtask_structures=protect_task.flowtask_structures
        )

        protection_script = cls._generate_protection_script(
            flowtask_structures=protect_task.flowtask_structures
        )

        return cls(
            diagram_version_id=diagram_version_id,
            diagram_id=diagram_version_id,
            entity_id=diagram_version_id,
            diagram_name=diagram_name,
            target_table_name=table_name.upper(),
            schema=schema_name.upper(),
            sysdate=sysdate,

            consume_data_id=cls.get_task_uuid("mcRDO.mfJMONInitialFlow.mmConsumeData", nodes),
            consume_data_version_id=cls.get_task_uuid("mcRDO.mfJMONInitialFlow.mmConsumeData", nodes),

            protect_data_id=cls.get_task_uuid("mcRDO.mfJMONInitialFlow.mmHideSensitiveData", nodes),
            protect_data_version_id=cls.get_task_uuid("mcRDO.mfJMONInitialFlow.mmHideSensitiveData", nodes),

            enrich_techcols_id=cls.get_task_uuid("mcRDO.mfJMONInitialFlow.mmEnrichTechCols", nodes),
            enrich_techcols_version_id=cls.get_task_uuid("mcRDO.mfJMONInitialFlow.mmEnrichTechCols", nodes),

            load_target_id=cls.get_task_uuid("mcRDO.mfJMONInitialFlow.mmLoadTargetData", nodes),
            load_target_version_id=cls.get_task_uuid("mcRDO.mfJMONInitialFlow.mmLoadTargetData", nodes),

            source_schema_id=cls._fetch_source_schema_id(metaclass.structures),
            source_schema_version_id=cls._fetch_source_schema_id(metaclass.structures),
            mask_schema_id=cls._fetch_mask_schema_id(metaclass.structures),
            mask_schema_version_id=cls._fetch_mask_schema_id(metaclass.structures),

            source_attribute_list=source_attributes,
            target_attribute_list=target_attributes,
            protection_script=protection_script,

            uuid_link_id_1="",
            uuid_link_id_2="",
            uuid_link_id_3="",
        )

    @classmethod
    def _build_journal(cls, metaclass, type):
        flow = cls._extract_flow(metaclass, type)
        nodes = cls._map_nodes(flow)

        schema_name = metaclass.application_code
        table_name = metaclass.entitytype_pname

        current_time = datetime.now()
        sysdate = current_time.strftime("%Y-%m-%d %H:%M:%S") + f".{current_time.microsecond // 1000:03d}"

        diagram_version_id = flow.propvalues_json.flow_uuid
        diagram_name = f"{schema_name.upper()}_JORNAL_{table_name.upper()}"

        protect_task = cls._fetch_protect_task(flow.flowtasks)
        source_attributes = cls._fetch_source_attributes(
            flowtask_structures=protect_task.flowtask_structures
        )

        target_attributes = cls._fetch_target_attributes(
            flowtask_structures=protect_task.flowtask_structures
        )

        protection_script = cls._generate_protection_script(
            flowtask_structures=protect_task.flowtask_structures
        )

        return cls(
            diagram_version_id=diagram_version_id,
            diagram_id=diagram_version_id,
            entity_id=diagram_version_id,
            diagram_name=diagram_name,
            target_table_name=table_name.upper(),
            schema=schema_name.upper(),
            sysdate=sysdate,

            consume_data_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmConsumeData", nodes),
            consume_data_version_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmConsumeData", nodes),

            consume_sensitive_flag_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmConsumeSensitiveFlag", nodes),
            consume_sensitive_flag_version_id=cls.get_task_uuid(
                "mcRDO.mfJMONStreamingFlow.mmConsumeSensitiveFlag", nodes
            ),

            union_sensitivecheck_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmUnionSensitiveFlagData", nodes),
            union_sensitivecheck_version_id=cls.get_task_uuid(
                "mcRDO.mfJMONStreamingFlow.mmUnionSensitiveFlagData", nodes
            ),

            protect_data_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmHideSensitiveData", nodes),
            protect_data_version_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmHideSensitiveData", nodes),

            check_sensitiverule_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmCheckSensitiveRules", nodes),
            check_sensitiverule_version_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmCheckSensitiveRules", nodes),

            enrich_techcols_cdc_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmEnrichTechCols.cdc", nodes),
            enrich_techcols_cdc_version_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmEnrichTechCols.cdc", nodes),

            enrich_techcols_rdo_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmEnrichTechCols.rdo", nodes),
            enrich_techcols_rdo_version_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmEnrichTechCols.rdo", nodes),

            load_target_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmLoadTargetData.rdo", nodes),
            load_target_version_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmLoadTargetData.rdo", nodes),

            load_target_cdc_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmLoadTargetData.cdc", nodes),
            load_target_cdc_version_id=cls.get_task_uuid("mcRDO.mfJMONStreamingFlow.mmLoadTargetData.cdc", nodes),

            source_schema_id=cls._fetch_source_schema_id(metaclass.structures),
            source_schema_version_id=cls._fetch_source_schema_id(metaclass.structures),
            mask_schema_id=cls._fetch_mask_schema_id(metaclass.structures),
            mask_schema_version_id=cls._fetch_mask_schema_id(metaclass.structures),

            source_attribute_list=source_attributes,
            target_attribute_list=target_attributes,
            protection_script=protection_script,

            uuid_link_id_1="",
            uuid_link_id_2="",
            uuid_link_id_3="",
            uuid_link_id_4="",
            uuid_link_id_5="",
            uuid_link_id_6="",
            uuid_link_id_7="",
            uuid_link_id_8="",
        )

    @staticmethod
    def _fetch_source_attributes(flowtask_structures) -> List[str]:
        sources = set()
        domains = ["dSourceKeyColumn", "dSourceColumn"]

        for structure in flowtask_structures:
            for attribute in structure["structure"]["attributes"]:
                attr_name = attribute["attribute_pname"]
                attr_domain = attribute["domain_code"]

                if attr_domain not in domains:
                    continue
                if attr_name.endswith("_mask") or attr_name.endswith("_hash"):
                    continue

                sources.add(attr_name.upper())

        return sorted(sources)

    @staticmethod
    def _fetch_target_attributes(flowtask_structures) -> List[str]:
        sources = set()
        domains = ["dSourceKeyColumn", "dSourceColumn", "dSourceMaskedColumn", "dSourceHashedColumn"]

        for structure in flowtask_structures:
            for attribute in structure["structure"]["attributes"]:
                attr_name = attribute["attribute_pname"]
                attr_domain = attribute["domain_code"]

                if attr_domain not in domains:
                    continue

                sources.add(attr_name)

        return sorted(sources)

    @staticmethod
    def _generate_protection_script(flowtask_structures) -> List[str]:
        script_lines = []
        processed_attrs = set()

        output_structure = None
        for struct in flowtask_structures:
            if struct.get("directiontype_code") == "output":
                output_structure = struct
                break
        else:
            for struct in flowtask_structures:
                if struct.get("metaobject"):
                    output_structure = struct
                    break

        if not output_structure:
            return []

        attributes = output_structure["structure"]["attributes"]

        for attr in attributes:
            attr_name = attr["attribute_name"]
            attr_pname = attr["attribute_pname"]
            attr_domain = attr["domain_code"]
            tool_type = attr["tooldatatype_code"]
            base_type = attr["basedatatype_code"]
            src_upper = attr_name.upper()

            if attr_pname in processed_attrs:
                continue
            processed_attrs.add(attr_pname)

            if attr_domain.startswith("dMeta"):
                continue

            if attr_domain in ["dSourceKeyColumn", "dSourceColumn"]:
                if tool_type == "timestamp" or "bdtTimestamp" in base_type:
                    line = f"{attr_name} = LocalDateTime.parse({src_upper}, " \
                           "DateTimeFormatter.ofPattern('yyyy-MM-dd HH:mm:ss.SSSSSS'))"
                else:
                    line = f'{attr_name} = {src_upper}'
                script_lines.append(line)

            elif attr_domain == "dSourceMaskedColumn":
                line = f"{attr_name}_mask = protector.maskData('{attr_name}', {src_upper})"
                script_lines.append(line)

            elif attr_domain == "dSourceHashedColumn":
                line = (
                    f"{attr_name}_hash = ({src_upper} == null) ? null : "
                    f"({src_upper}.trim() == \'\') ? null : "
                    f"protector.hashData('{attr_name}', {src_upper}.trim())"
                )
                script_lines.append(line)

        return "\\n".join(script_lines)

    @staticmethod
    def _fetch_source_schema_id(structures):
        for entity in structures:
            if entity.metastructure_code == "mcRDO.msSourceDefinition":
                return entity.propvalues_json.structure_uuid

    @staticmethod
    def _fetch_mask_schema_id(structures):
        for entity in structures:
            if entity.metastructure_code == "mcRDO.msReplicaDataObject":
                return entity.propvalues_json.structure_uuid

    @staticmethod
    def _extract_flow(metaclass, flow_type: str) -> Any:
        for flow in metaclass.flows:
            if flow.flow_pname.split("__")[-1] == flow_type:
                return flow
        raise ValueError(f"Flow с типом '{flow_type}' не найден в metaclass")

    @staticmethod
    def _map_nodes(flow) -> Dict[str, Any]:
        return {task.metaflowtask_code: task for task in flow.flowtasks}

    @staticmethod
    def _fetch_protect_task(flows):
        for flow in flows:
            if flow.metaflowtask_code.endswith(".mmHideSensitiveData"):
                return flow

    @staticmethod
    def get_task_uuid(task_path: str, nodes) -> str:
        task = nodes[task_path]
        return task.propvalues_json.flowtask_uuid


if __name__ == "__main__":
    import re

    def render(template_path, output_path, **kwargs):
        env = Environment(
            loader=FileSystemLoader('templates'),
            extensions=['jinja2.ext.do']
        )
        env.globals["re"] = re

        template = env.get_template(template_path)
        generated_dag = template.render(**kwargs)

        with open(output_path, "w") as f:
            f.write(generated_dag)

    metaobject = main("meta/odh/odh.ors.data.if_ksm1transfer.json")

    try:
        for struct in metaobject.structures:
            if struct.metastructure_code == "mcRDO.msSourceDefinition":
                source_structure = struct
            if struct.metastructure_code == "mcRDO.msReplicaDataObject":
                rdo_structure = struct
            if struct.metastructure_code == "mcRDO.msReplicaDataView":
                rdv_structure = struct

        for flow in metaobject.flows:
            if flow.metaflow_code == "mfJMONInitialFlow":
                initial_flow = flow
            elif flow.metaflow_code == "mfJMONStreamingFlow":
                journal_flow = flow
    except Exception:
        pass

    render("odh/test_2.j2", "generated/left_join_test.sql", metaobject=metaobject)
