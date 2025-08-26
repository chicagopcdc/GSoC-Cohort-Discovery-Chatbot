import json
import os
from typing import List
import re
import ast

def extract_context_from_user_query(input) -> List:
    """
    改为按空格或者标点符号(, .)分割input, 返回数组
    提取用户查询中的关键词，过滤掉常见停用词
    """
    stop_words = {
        'the', 'a', 'an', 'but', 'on', 'at', 'to', 'for', 
        'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
        'before', 'after', 'above', 'below', 'between', 'among', 'under', 'over',
        'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
        'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
        'must', 'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she',
        'it', 'we', 'they', 'me', 'him', 'her', 'us', 'them', 'my', 'your', 'his',
        'her', 'its', 'our', 'their', 'who', 'what', 'when', 'where', 'why', 'how',
        'consists', 'participants', 'specifically', 'classified', 'located', 'as',
        'show', 'find', 'get', 'select', 'search', 'list', 'display', 'return'
    }
    
    # 使用正则表达式分割并过滤
    words = re.split(r'[,.\s]+', input)
    
    # 过滤掉空字符串、停用词和短词
    filtered_words = []
    for word in words:
        if (word and  # 不是空字符串
            len(word) >= 2 and  # 长度至少2
            word.lower() not in stop_words and  # 不在停用词列表
            not word.isdigit()):  # 不是纯数字
            filtered_words.append(word)
    
    return filtered_words

def parse_pcdc_schema_prod(file):
    def recursive_enum_extract(obj, current_key=None, result=None):
        """递归提取所有enum值并关联到对应的key"""
        if result is None:
            result = {}
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "enum" and isinstance(value, list):
                    # 找到enum，将enum列表中的每个值作为result的key
                    # current_key是包含这个enum的上一级key
                    if current_key:
                        for enum_value in value:
                            if isinstance(enum_value, str):
                                if enum_value not in result:
                                    result[enum_value] = []
                                if current_key not in result[enum_value]:
                                    result[enum_value].append(current_key)
                else:
                    # 递归处理嵌套的对象
                    recursive_enum_extract(value, key, result)
        elif isinstance(obj, list):
            # 如果是列表，递归处理列表中的每个元素
            for item in obj:
                recursive_enum_extract(item, current_key, result)
        
        return result
    
    try:
        # 读取JSON文件
        with open(file, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
        
        # 递归提取所有enum值
        result = recursive_enum_extract(schema_data)
        
        # 生成输出文件路径
        file_dir = os.path.dirname(file)
        output_file = os.path.join(file_dir, "processed_pcdc_schema_prod.json")
        
        # 保存结果到JSON文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"Processed schema saved to: {output_file}")
        print(f"Total enum values extracted: {len(result)}")
        
        return result
        
    except Exception as e:
        print(f"Error in parse_pcdc_schema_prod: {str(e)}")
        return {}

def parse_gitops(file):
    def recursive_fields_extract(obj, result=None):
        """递归提取所有fields值并分析字段映射，每个字段对应一个包含所有表名的列表"""
        if result is None:
            result = {}
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "fields" and isinstance(value, list):
                    # 找到fields，处理列表中的每个字段
                    for field in value:
                        if isinstance(field, str) and '.' in field:
                            # 按点号分割字段名
                            parts = field.split('.', 1)  # 只分割第一个点号
                            if len(parts) == 2:
                                table_name = parts[0]  # 点号前的部分作为表名
                                field_name = parts[1]  # 点号后的部分作为字段名
                                
                                # 如果字段名不存在，创建新的列表
                                if field_name not in result:
                                    result[field_name] = []
                                
                                # 如果表名不在列表中，则追加（去重）
                                if table_name not in result[field_name]:
                                    result[field_name].append(table_name)
                        elif isinstance(field, str):
                            # 没有点号的字段，使用字段名本身作为key，值为空列表
                            if field not in result:
                                result[field] = []
                else:
                    # 递归处理嵌套的对象
                    recursive_fields_extract(value, result)
        elif isinstance(obj, list):
            # 如果是列表，递归处理列表中的每个元素
            for item in obj:
                recursive_fields_extract(item, result)
        
        return result
    
    try:
        # 读取JSON文件
        with open(file, 'r', encoding='utf-8') as f:
            gitops_data = json.load(f)
        
        # 递归提取所有fields字段映射
        result = recursive_fields_extract(gitops_data)
        
        # 生成输出文件路径
        file_dir = os.path.dirname(file)
        output_file = os.path.join(file_dir, "processed_gitops.json")
        
        # 保存结果到JSON文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"Processed gitops saved to: {output_file}")
        print(f"Total field mappings extracted: {len(result)}")
        
        # 显示一些统计信息
        fields_with_multiple_tables = {k: v for k, v in result.items() if len(v) > 1}
        print(f"Fields appearing in multiple tables: {len(fields_with_multiple_tables)}")
        
        return result
        
    except Exception as e:
        print(f"Error in parse_gitops: {str(e)}")
        return {}


async def query_processed_pcdc_result(lowercase_pcdc_dict, keyword, user_query, llm):
    """
        如果出现一对多的mapping关系, 比如
        "Metastatic": [
            "lesion_classification",
            "molecular_analysis_classification",
            "tumor_classification"
        ],
        就让llm根据user_query的context决定最终的mapping schema 
    """
    try:
        # 使用小写的keyword进行查找
        keyword_lower = keyword.lower()
        if keyword_lower in lowercase_pcdc_dict:
            mapping_schemas_in_pcdc_schema_prod = lowercase_pcdc_dict[keyword_lower]
            print(f"keyword: {keyword}, mapping_list_in_pcdc_schema_prod: {mapping_schemas_in_pcdc_schema_prod}")
            if len(mapping_schemas_in_pcdc_schema_prod) == 1:
                return mapping_schemas_in_pcdc_schema_prod[0]
            elif len(mapping_schemas_in_pcdc_schema_prod) > 1:
                prompt = f"""
                    Multiple medical terms from the query map to overlapping or conflicting database fields in pcdc-schema-prod.json. Resolve these conflicts to choose the most appropriate field.

                    Original Query: "{user_query}"
                    
                    Current Term: "{keyword}"
                    Conflicting Fields: {mapping_schemas_in_pcdc_schema_prod}
                    
                    Resolve by:
                    1. Identifying semantic overlaps (e.g., "cancer" and "tumor" might refer to same field)
                    2. Choosing more specific terms over general ones  
                    3. Maintaining clinical accuracy
                    4. Preserving user intent
                    5. Considering the medical context of the query
                    
                    From the conflicting fields list, select the ONE field that best matches the user query context.
                    Only return the selected field name as a string, no explanation needed.
                """
                llm_result = llm.invoke(prompt)
                print(f"llm_result: {llm_result}")
                # Extract content from the LLM response
                if hasattr(llm_result, 'content'):
                    llm_mapping_result = llm_result.content.strip().strip('"')  # Remove quotes if present
                else:
                    llm_mapping_result = str(llm_result).strip().strip('"')
                return llm_mapping_result
        return ""
    except Exception as e:
        print(f"Error in query_processed_pcdc_result: {str(e)}")
        return ""

async def query_processed_gitops_result(lowercase_gitops_dict, pcdc_schema, user_query, llm):
    """
    如果出现一对多的mapping关系(比如某个pcdc schema property对应多个gitops field node), 让llm根据user query的context决定最终的mapping schema 
    Args:
        query_pcdc_schema_prod_result: 从PCDC schema查询得到的property名称
        processed_gitops_file: 处理过的gitops文件路径
        user_query: 用户查询
        llm: LLM agent
    Returns:
        对应的gitops field node名称
    """
    try:
        # 如果PCDC查询结果为空，直接返回空字符串
        if not pcdc_schema:
            return ""
        # 使用小写的query_pcdc_schema_prod_result进行查找
        pcdc_property_lower = pcdc_schema.lower()
        if pcdc_property_lower in lowercase_gitops_dict:
            mapping_gitops_field_nodes = lowercase_gitops_dict[pcdc_property_lower]
            print(f"pcdc_schema: {pcdc_schema}, mapping_gitops_field_nodes: {mapping_gitops_field_nodes}")
            if len(mapping_gitops_field_nodes) == 0:
                return ""
            elif len(mapping_gitops_field_nodes) == 1:
                return mapping_gitops_field_nodes[0]
            elif len(mapping_gitops_field_nodes) > 1:
                # 多个映射，需要LLM根据上下文选择最合适的
                prompt = f"""
                    Multiple GitOps field nodes map to the same PCDC schema property. Resolve this conflict to choose the most contextually appropriate field node.

                    Original Query: "{user_query}"
                    
                    PCDC Schema Property: "{pcdc_schema}"
                    Conflicting GitOps Field Nodes: {mapping_gitops_field_nodes}
                    
                    Example Context Mapping:
                    - If query mentions "tumors" + "assessment" → choose "tumor_assessments"
                    - If query mentions "surgery" or "biopsy" → choose "biopsy_surgical_procedures"  
                    - If query mentions "radiation" or "therapy" → choose "radiation_therapies"
                    
                    Resolve by:
                    1. Analyzing the medical procedure/context mentioned in the query
                    2. Choosing the field node that best matches the clinical workflow
                    3. Considering the temporal or procedural relationship
                    4. Maintaining semantic consistency with user intent
                    5. Prioritizing more specific contexts over general ones
                    
                    From the conflicting GitOps field nodes, select the ONE that best matches the user query context.
                    Only return the selected field node name as a string, no explanation needed.
                """
                llm_result = llm.invoke(prompt)
                print(f"gitops llm_result: {llm_result}")
                # Extract content from the LLM response
                if hasattr(llm_result, 'content'):
                    llm_mapping_result = llm_result.content.strip().strip('"')  # Remove quotes if present
                else:
                    llm_mapping_result = str(llm_result).strip().strip('"')
                return llm_mapping_result
        
        # 如果在gitops中找不到对应的mapping，返回空字符串
        return ""
        
    except Exception as e:
        print(f"Error in query_processed_gitops_result: {str(e)}")
        return ""

def convert_to_executable_nested_graphql(nested_graphql, llm):
    """
    Convert nested GraphQL filter to executable GraphQL format
    
    Args:
        nested_graphql: The raw LLM response content containing nested GraphQL
        llm: LLM instance for processing
        
    Returns:
        Executable GraphQL query in the format expected by execute_graphql_query()
    """
    prompt = f"""
    你需要根据以下nested GraphQL结果生成一个能实际执行/query接口的nested graphql版本。

    输入的nested GraphQL结果:
    {nested_graphql}

    请输出一个能实际执行的nested graphql，格式如下示例:
    {{
      "query": "query GetAggregation($filter: JSON) {{ _aggregation {{ subject(accessibility: all, filter: $filter) {{ _totalCount }} }} }}",
      "variables": {{
        "filter": {{
          "AND": [
            {{
              "IN": {{
                "consortium": ["INRG"]
              }}
            }},
            {{
              "nested": {{
                "path": "tumor_assessments",
                "AND": [
                  {{
                    "IN": {{
                      "tumor_classification": ["Metastatic"]
                    }}
                  }},
                  {{
                    "IN": {{
                      "tumor_state": ["Absent"]
                    }}
                  }},
                  {{
                    "IN": {{
                      "tumor_site": ["Skin"]
                    }}
                  }}
                ]
              }}
            }}
          ]
        }}
      }}
    }}

    要求:
    1. query字段必须使用aggregation查询格式
    2. variables.filter要包含完整的嵌套过滤条件
    3. 返回标准的JSON格式，不要包含任何解释文字
    4. 确保path字段在nested结构的正确位置
    """
    
    try:
        # 调用LLM生成可执行的GraphQL
        response = llm.invoke(prompt)
        response_content = response.content if hasattr(response, 'content') else str(response)
        
        # 清理响应内容，移除可能的markdown标记
        clean_response = response_content.strip()
        if clean_response.startswith('```json'):
            clean_response = clean_response[7:-3]
        elif clean_response.startswith('```'):
            clean_response = clean_response[3:-3]
        
        # 解析JSON响应
        try:
            guppy_graphql = json.loads(clean_response.strip())
            
            # 验证返回的结果是否包含必要的字段
            if isinstance(guppy_graphql, dict) and "query" in guppy_graphql and "variables" in guppy_graphql:
                print(f"Successfully generated executable GraphQL: {json.dumps(guppy_graphql, ensure_ascii=False, indent=2)}")
                return guppy_graphql
            else:
                print(f"Invalid GraphQL format returned by LLM: {guppy_graphql}")
                return None
                
        except json.JSONDecodeError as e:
            print(f"Error parsing LLM response as JSON: {str(e)}")
            print(f"Raw LLM response: {response_content}")
            return None
            
    except Exception as e:
        print(f"Error in convert_to_executable_nested_graphql: {str(e)}")
        return None

def test_query_functions():
    pcdc_schema_prod_file = "../../schema/schema/pcdc-schema-prod-20250114.json"
    processed_pcdc_schema_prod_result = parse_pcdc_schema_prod(pcdc_schema_prod_file)
    gitops_file = "../../schema/gitops.json"
    processed_gitop_result = parse_gitops(gitops_file)

if __name__ == "__main__":
    # test_query_functions()
    pass