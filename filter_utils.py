"""
filter_utils.py - GraphQL过滤器转换工具

此模块提供了将前端FilterState对象转换为GraphQL过滤器格式的工具，
以及将GraphQL过滤器转换回FilterState对象的功能。
支持自动从PCDC schema读取字段类型信息，动态处理不同类型的过滤条件。

主要功能:
- getGQLFilter: 将FilterState对象转换为GraphQL过滤器
- getFilterState: 将GraphQL过滤器转换为FilterState对象
- SchemaTypeHandler: 基于schema类型自动处理不同字段类型
"""

import json
from typing import Dict, List, Any, Optional, Union, Tuple
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 定义过滤器类型常量
class FILTER_TYPE:
    """过滤器类型常量"""
    COMPOSED = 'COMPOSED'
    ANCHORED = 'ANCHORED'
    STANDARD = 'STANDARD'
    OPTION = 'OPTION'
    RANGE = 'RANGE'

# 类型定义
FilterState = Dict[str, Any]
GqlFilter = Dict[str, Any]
GqlSimpleFilter = Dict[str, Any]
GqlNestedFilter = Dict[str, Dict[str, Any]]

class SchemaTypeHandler:
    """
    基于PCDC schema类型信息处理不同字段类型的过滤器转换
    """
    
    def __init__(self, node_properties: Dict = None):
        """
        初始化SchemaTypeHandler
        
        Args:
            node_properties: 从schema_parser.parse_pcdc_schema获取的节点属性信息
        """
        self.node_properties = node_properties or {}
        self._field_type_cache = {}  # 缓存字段类型信息
    
    def get_field_type_info(self, field_path: str) -> Tuple[str, Dict]:
        """
        获取字段的类型信息
        
        Args:
            field_path: 字段路径，可能包含点号表示嵌套字段
            
        Returns:
            元组 (字段类型, 字段详细信息)
        """
        # 检查缓存
        if field_path in self._field_type_cache:
            return self._field_type_cache[field_path]
        
        # 解析字段路径
        parts = field_path.split('.')
        node_type = parts[0] if len(parts) > 1 else 'subject'
        field_name = parts[-1]
        
        # 获取节点属性
        node_info = self.node_properties.get(node_type, {})
        field_info = node_info.get(field_name, {})
        
        # 确定字段类型
        field_type = 'unknown'
        if 'enum' in field_info:
            field_type = 'enum'
        elif 'type' in field_info:
            type_info = field_info['type']
            if isinstance(type_info, list) and 'number' in type_info:
                field_type = 'number'
            elif isinstance(type_info, list) and 'string' in type_info:
                field_type = 'string'
            else:
                field_type = str(type_info)
        
        # 缓存结果
        result = (field_type, field_info)
        self._field_type_cache[field_path] = result
        return result
    
    def parse_filter_value(self, field_name: str, filter_values: Dict[str, Any]) -> Optional[GqlSimpleFilter]:
        """
        根据字段类型和过滤值解析为GraphQL过滤器
        
        Args:
            field_name: 字段名称
            filter_values: 过滤值对象
            
        Returns:
            GraphQL简单过滤器对象或None
        """
        # 获取字段类型信息
        field_type, field_info = self.get_field_type_info(field_name)
        
        # 根据过滤器类型处理
        filter_type = filter_values.get('__type')
        
        if filter_type == FILTER_TYPE.OPTION:
            # 选项类型过滤器
            selected_values = filter_values.get('selectedValues', [])
            if selected_values:
                return {'IN': {field_name: selected_values}}
        
        elif filter_type == FILTER_TYPE.RANGE:
            # 范围类型过滤器
            lower_bound = filter_values.get('lowerBound')
            upper_bound = filter_values.get('upperBound')
            
            if lower_bound is not None and upper_bound is not None:
                # 同时有上下限
                return {'AND': [
                    {'GTE': {field_name: lower_bound}},
                    {'LTE': {field_name: upper_bound}}
                ]}
            elif lower_bound is not None:
                # 只有下限
                return {'GTE': {field_name: lower_bound}}
            elif upper_bound is not None:
                # 只有上限
                return {'LTE': {field_name: upper_bound}}
        
        # 对于其他类型，尝试基于schema信息智能处理
        if field_type == 'enum' and 'value' in filter_values:
            # 枚举字段的直接值
            return {field_name: filter_values['value']}
        elif field_type == 'number' and 'value' in filter_values:
            # 数值字段的直接值
            return {field_name: filter_values['value']}
        elif field_type == 'string' and 'value' in filter_values:
            # 字符串字段的直接值
            return {field_name: filter_values['value']}
        
        # 未能识别的过滤器类型
        logger.debug(f"未能解析过滤器值: {field_name}={filter_values}, 字段类型={field_type}")
        return None


def parse_anchored_filters(field_name: str, filter_values: Dict[str, Any], combine_mode: str) -> List[Dict[str, Any]]:
    """
    解析锚定类型过滤器
    
    Args:
        field_name: 字段名称
        filter_values: 过滤值对象
        combine_mode: 组合模式 (AND/OR)
        
    Returns:
        解析后的过滤器列表
    """
    # 注意：此函数需要根据实际的锚定过滤器结构实现
    # 目前返回空列表作为占位符
    logger.warning(f"锚定过滤器解析尚未完全实现: {field_name}")
    return []


def parse_simple_filter(field_name: str, filter_values: Dict[str, Any], schema_handler: Optional[SchemaTypeHandler] = None) -> Optional[GqlSimpleFilter]:
    """
    解析简单过滤器
    
    Args:
        field_name: 字段名称
        filter_values: 过滤值对象
        schema_handler: schema类型处理器
        
    Returns:
        GraphQL简单过滤器对象或None
    """
    # 如果提供了schema处理器，使用它进行智能解析
    if schema_handler:
        return schema_handler.parse_filter_value(field_name, filter_values)
    
    # 兜底逻辑：只处理OPTION类型
    if filter_values.get('__type') == FILTER_TYPE.OPTION:
        return {'IN': {field_name: filter_values.get('selectedValues', [])}}
    
    return None


def getGQLFilter(filter_state: Optional[FilterState], schema_handler: Optional[SchemaTypeHandler] = None) -> Optional[GqlFilter]:
    """
    将FilterState对象转换为GraphQL过滤器格式
    
    Args:
        filter_state: FilterState对象
        schema_handler: schema类型处理器
        
    Returns:
        GraphQL过滤器对象或None
    """
    # 检查空值
    if (
        filter_state is None or
        'value' not in filter_state or
        not filter_state['value']
    ):
        return None

    # 获取组合模式
    combine_mode = filter_state.get('__combineMode', 'AND')
    
    # 处理组合过滤器
    if filter_state.get('__type') == FILTER_TYPE.COMPOSED:
        return {combine_mode: [getGQLFilter(fs, schema_handler) for fs in filter_state['value']]}

    # 初始化过滤器列表
    simple_filters = []
    nested_filters = []
    nested_filter_indices = {}
    nested_filter_index = 0

    # 处理每个过滤条件
    for filter_key, filter_values in filter_state['value'].items():
        # 解析字段路径
        parts = filter_key.split('.')
        field_str = parts[0]
        nested_field_str = parts[1] if len(parts) > 1 else None
        is_nested_field = nested_field_str is not None
        field_name = nested_field_str if is_nested_field else field_str

        # 处理锚定类型过滤器
        if filter_values.get('__type') == FILTER_TYPE.ANCHORED:
            parsed_anchored_filters = parse_anchored_filters(field_name, filter_values, combine_mode)
            for item in parsed_anchored_filters:
                if 'nested' in item:
                    nested = item['nested']
                    path = nested['path']
                    
                    if path not in nested_filter_indices:
                        nested_filter_indices[path] = nested_filter_index
                        nested_filters.append({
                            'nested': {'path': path, combine_mode: []}
                        })
                        nested_filter_index += 1
                    
                    nested_filters[nested_filter_indices[path]]['nested'][combine_mode].append({'AND': nested['AND']})
        
        # 处理简单过滤器
        else:
            simple_filter = parse_simple_filter(field_name, filter_values, schema_handler)
            
            if simple_filter is not None:
                if is_nested_field:
                    # 嵌套字段
                    path = field_str
                    
                    if path not in nested_filter_indices:
                        nested_filter_indices[path] = nested_filter_index
                        nested_filters.append({
                            'nested': {'path': path, combine_mode: []}
                        })
                        nested_filter_index += 1
                    
                    nested_filters[nested_filter_indices[path]]['nested'][combine_mode].append(simple_filter)
                else:
                    # 普通字段
                    simple_filters.append(simple_filter)

    # 组合所有过滤器
    return {combine_mode: simple_filters + nested_filters} if simple_filters or nested_filters else None


def getFilterState(gql_filter: Optional[GqlFilter]) -> Optional[FilterState]:
    """
    将GraphQL过滤器转换为FilterState对象
    
    Args:
        gql_filter: GraphQL过滤器对象
        
    Returns:
        FilterState对象或None
    """
    # 检查空值
    if gql_filter is None:
        return None
    
    # 获取组合模式
    combinator = list(gql_filter.keys())[0]
    filter_values = gql_filter[combinator]
    
    # 检查空值
    if not filter_values:
        return None
    
    # 处理AND/OR组合
    if combinator in ('AND', 'OR'):
        values = {}
        
        for filter_value in filter_values:
            # 获取过滤器类型和值
            value_combinator = list(filter_value.keys())[0]
            value = filter_value[value_combinator]
            
            # 处理IN操作符（选项类型）
            if value_combinator == 'IN':
                option = {}
                
                for field, val in value.items():
                    option[field] = {
                        '__type': FILTER_TYPE.OPTION,
                        'selectedValues': val,
                        'isExclusion': False
                    }
                
                values = {**option, **values}
            
            # 处理GTE/LTE操作符（范围类型）
            elif value_combinator in ('GTE', 'LTE') and isinstance(value, dict):
                for field, val in value.items():
                    if field not in values:
                        values[field] = {
                            '__type': FILTER_TYPE.RANGE,
                            'lowerBound': val if value_combinator == 'GTE' else None,
                            'upperBound': val if value_combinator == 'LTE' else None
                        }
                    else:
                        # 更新现有范围
                        if value_combinator == 'GTE':
                            values[field]['lowerBound'] = val
                        else:
                            values[field]['upperBound'] = val
            
            # 处理嵌套过滤器
            elif value_combinator == 'nested' and isinstance(value, dict):
                path = value.get('path')
                nested_combinator = 'AND'  # 默认使用AND
                
                # 查找实际使用的组合器
                for key in value:
                    if key in ('AND', 'OR'):
                        nested_combinator = key
                        break
                
                # 处理嵌套过滤器的每个条件
                for nested_filter in value.get(nested_combinator, []):
                    nested_value_combinator = list(nested_filter.keys())[0]
                    nested_value = nested_filter[nested_value_combinator]
                    
                    # 处理嵌套的IN操作符
                    if nested_value_combinator == 'IN':
                        for field, val in nested_value.items():
                            nested_field = f"{path}.{field}"
                            values[nested_field] = {
                                '__type': FILTER_TYPE.OPTION,
                                'selectedValues': val,
                                'isExclusion': False
                            }
            
            # 处理其他类型的过滤器
            # 这里可以根据需要扩展更多类型的处理
        
        # 返回FilterState对象
        return {
            '__combineMode': combinator,
            '__type': FILTER_TYPE.STANDARD,
            'value': values
        }
    
    return None


# 导出的主要函数
__all__ = ['getGQLFilter', 'getFilterState', 'SchemaTypeHandler', 'FILTER_TYPE'] 