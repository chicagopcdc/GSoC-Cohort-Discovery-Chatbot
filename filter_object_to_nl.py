import json
from typing import Any, Dict, List, Union

def filter_object_to_nl(filter_obj: Union[str, Dict[str, Any]]) -> str:
    """
    change filter_object structure to natural language description.
    support selectedValues, lowerBound/upperBound, nested, AND/OR, etc.
    """
    if isinstance(filter_obj, str):
        try:
            filter_obj = json.loads(filter_obj)
        except Exception:
            return str(filter_obj)

    # recursive processing
    return _parse_filter(filter_obj)

def _parse_filter(obj: Any) -> str:
    if not obj:
        return ""
    # process AND/OR/NOT/COMPOSED etc.
    if isinstance(obj, dict):
        if '__combineMode' in obj:
            mode = obj['__combineMode']
            items = [v for k, v in obj.items() if k not in ['__combineMode', '__type']]
            if mode == 'AND':
                return ' and '.join([_parse_filter(i) for i in items])
            elif mode == 'OR':
                return ' or '.join([_parse_filter(i) for i in items])
            elif mode == 'NOT':
                return 'not (' + ' and '.join([_parse_filter(i) for i in items]) + ')'
        if '__type' in obj and obj['__type'] == 'COMPOSED':
            # composite type, value is list
            return _parse_filter(obj.get('value', []))
        if 'value' in obj:
            return _parse_filter(obj['value']) 
        # process field
        descs = []
        for k, v in obj.items():
            if k in ['__type', '__combineMode']:
                continue
            if isinstance(v, dict):
                # distinguish option/range/filter etc.
                if v.get('__type') == 'OPTION' and 'selectedValues' in v:
                    descs.append(f"{_field_to_nl(k)} is {', '.join(map(str, v['selectedValues']))}")
                elif v.get('__type') == 'RANGE':
                    lb = v.get('lowerBound')
                    ub = v.get('upperBound')
                    if lb is not None and ub is not None:
                        descs.append(f"{_field_to_nl(k)} between {lb} and {ub}")
                    elif lb is not None:
                        descs.append(f"{_field_to_nl(k)} >= {lb}")
                    elif ub is not None:
                        descs.append(f"{_field_to_nl(k)} <= {ub}")
                elif 'selectedValues' in v:
                    descs.append(f"{_field_to_nl(k)} is {', '.join(map(str, v['selectedValues']))}")
                elif 'filter' in v:
                    descs.append(_parse_filter(v['filter']))
                elif 'value' in v:
                    descs.append(_parse_filter(v['value']))
                else:
                    descs.append(f"{_field_to_nl(k)}: {_parse_filter(v)}")
            elif isinstance(v, list):
                descs.append(' or '.join([_parse_filter(i) for i in v]))
            else:
                descs.append(f"{_field_to_nl(k)} is {v}")
        return ' and '.join(descs)
    elif isinstance(obj, list):
        return ' or '.join([_parse_filter(i) for i in obj])
    else:
        return str(obj)

def _field_to_nl(field: str) -> str:
    """
        change field name to natural language (can be extended to Chinese/English mapping)
    """
    mapping = {
        'sex': 'Sex',
        'race': 'Race',
        'ethnicity': 'Ethnicity',
        'consortium': 'Consortium',
        'age_at_censor_status': 'Age at censor status',
        'tumor_assessments.tumor_classification': 'Tumor classification',
        'tumor_assessments.tumor_site': 'Tumor site',
        'tumor_assessments.tumor_state': 'Tumor state',
        'histologies.histology': 'Histology',
        'molecular_analysis.molecular_abnormality': 'Molecular abnormality',
        'molecular_analysis.molecular_abnormality_result': 'Molecular abnormality result',
        'molecular_analysis.gene1': 'Gene1',
        'molecular_analysis.gene2': 'Gene2',
        'survival_characteristics.lkss': 'Survival status',
        'censor_status': 'Censor status',
        'stagings.stage': 'Stage',
        'stagings.stage_system': 'Stage system',
        'year_at_disease_phase': 'Year at disease phase',
        'tumor_assessments.age_at_tumor_assessment': 'Age at tumor assessment',
        # can be extended to Chinese/English mapping
    }
    return mapping.get(field, field) 