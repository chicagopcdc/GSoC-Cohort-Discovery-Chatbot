"""
test_filter_utils.py - 过滤器工具单元测试

此模块包含对filter_utils.py中函数和类的单元测试。
"""

import sys
import os
import unittest
import json
from typing import Dict, Any

# 添加项目根目录到路径，以便导入filter_utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from filter_utils import (
    getGQLFilter, 
    getFilterState, 
    SchemaTypeHandler, 
    FILTER_TYPE
)


class TestFilterUtils(unittest.TestCase):
    """测试filter_utils模块的功能"""

    def setUp(self):
        """测试前的准备工作"""
        # 示例schema
        self.test_schema = {
            "subject": {
                "sex": {"enum": ["Male", "Female", "Unknown", "Not Reported"]},
                "race": {"enum": ["White", "Black", "Asian", "Multiracial", "Unknown", "Not Reported"]},
                "age_at_censor_status": {"type": ["number"]},
            },
            "disease_characteristic": {
                "bulky_nodal_aggregate": {"enum": ["Yes", "No", "Unknown", "Not Reported"]}
            }
        }
        
        # 创建SchemaTypeHandler实例
        self.handler = SchemaTypeHandler(self.test_schema)
    
    def test_simple_option_filter(self):
        """测试简单OPTION过滤器"""
        filter_state = {
            "__type": "STANDARD",
            "__combineMode": "AND",
            "value": {
                "sex": {
                    "__type": "OPTION",
                    "selectedValues": ["Male"],
                    "isExclusion": False
                }
            }
        }
        
        expected = {
            "AND": [
                {"IN": {"sex": ["Male"]}}
            ]
        }
        
        result = getGQLFilter(filter_state, self.handler)
        self.assertEqual(result, expected)
    
    def test_nested_field_filter(self):
        """测试嵌套字段过滤器"""
        filter_state = {
            "__type": "STANDARD",
            "__combineMode": "AND",
            "value": {
                "disease_characteristic.bulky_nodal_aggregate": {
                    "__type": "OPTION",
                    "selectedValues": ["No"],
                    "isExclusion": False
                }
            }
        }
        
        expected = {
            "AND": [
                {
                    "nested": {
                        "path": "disease_characteristic",
                        "AND": [
                            {"IN": {"bulky_nodal_aggregate": ["No"]}}
                        ]
                    }
                }
            ]
        }
        
        result = getGQLFilter(filter_state, self.handler)
        self.assertEqual(result, expected)
    
    def test_range_filter(self):
        """测试范围过滤器"""
        filter_state = {
            "__type": "STANDARD",
            "__combineMode": "AND",
            "value": {
                "age_at_censor_status": {
                    "__type": "RANGE",
                    "lowerBound": 0,
                    "upperBound": 18
                }
            }
        }
        
        expected = {
            "AND": [
                {
                    "AND": [
                        {"GTE": {"age_at_censor_status": 0}},
                        {"LTE": {"age_at_censor_status": 18}}
                    ]
                }
            ]
        }
        
        result = getGQLFilter(filter_state, self.handler)
        self.assertEqual(result, expected)
    
    def test_compound_filter(self):
        """测试复合过滤器"""
        filter_state = {
            "__type": "STANDARD",
            "__combineMode": "AND",
            "value": {
                "sex": {
                    "__type": "OPTION",
                    "selectedValues": ["Male"],
                    "isExclusion": False
                },
                "race": {
                    "__type": "OPTION",
                    "selectedValues": ["White", "Asian"],
                    "isExclusion": False
                },
                "disease_characteristic.bulky_nodal_aggregate": {
                    "__type": "OPTION",
                    "selectedValues": ["No"],
                    "isExclusion": False
                }
            }
        }
        
        result = getGQLFilter(filter_state, self.handler)
        
        # 检查结果的基本结构
        self.assertIn("AND", result)
        self.assertEqual(len(result["AND"]), 3)
        
        # 检查是否包含所有预期的过滤条件
        filters = result["AND"]
        found_sex = False
        found_race = False
        found_nested = False
        
        for f in filters:
            if "IN" in f and "sex" in f["IN"]:
                found_sex = True
                self.assertEqual(f["IN"]["sex"], ["Male"])
            elif "IN" in f and "race" in f["IN"]:
                found_race = True
                self.assertEqual(f["IN"]["race"], ["White", "Asian"])
            elif "nested" in f:
                found_nested = True
                self.assertEqual(f["nested"]["path"], "disease_characteristic")
                self.assertIn("AND", f["nested"])
                self.assertEqual(len(f["nested"]["AND"]), 1)
                self.assertIn("IN", f["nested"]["AND"][0])
                self.assertEqual(f["nested"]["AND"][0]["IN"]["bulky_nodal_aggregate"], ["No"])
        
        self.assertTrue(found_sex, "未找到sex过滤条件")
        self.assertTrue(found_race, "未找到race过滤条件")
        self.assertTrue(found_nested, "未找到嵌套过滤条件")
    
    def test_filter_state_conversion(self):
        """测试从GraphQL过滤器转回FilterState"""
        # 先创建一个GraphQL过滤器
        gql_filter = {
            "AND": [
                {"IN": {"sex": ["Male"]}},
                {"IN": {"race": ["White", "Asian"]}},
                {
                    "nested": {
                        "path": "disease_characteristic",
                        "AND": [
                            {"IN": {"bulky_nodal_aggregate": ["No"]}}
                        ]
                    }
                }
            ]
        }
        
        # 转换为FilterState
        filter_state = getFilterState(gql_filter)
        
        # 检查基本结构
        self.assertEqual(filter_state["__type"], "STANDARD")
        self.assertEqual(filter_state["__combineMode"], "AND")
        self.assertIn("value", filter_state)
        
        # 检查字段
        value = filter_state["value"]
        self.assertIn("sex", value)
        self.assertEqual(value["sex"]["__type"], "OPTION")
        self.assertEqual(value["sex"]["selectedValues"], ["Male"])
        
        self.assertIn("race", value)
        self.assertEqual(value["race"]["__type"], "OPTION")
        self.assertEqual(value["race"]["selectedValues"], ["White", "Asian"])
        
        self.assertIn("disease_characteristic.bulky_nodal_aggregate", value)
        self.assertEqual(value["disease_characteristic.bulky_nodal_aggregate"]["__type"], "OPTION")
        self.assertEqual(value["disease_characteristic.bulky_nodal_aggregate"]["selectedValues"], ["No"])
    
    def test_round_trip_consistency(self):
        """测试往返转换一致性"""
        # 创建初始GraphQL过滤器
        original_filter = {
            "AND": [
                {"IN": {"sex": ["Male"]}},
                {"IN": {"race": ["White"]}}
            ]
        }
        
        # 转换为FilterState
        filter_state = getFilterState(original_filter)
        
        # 再转回GraphQL过滤器
        round_trip_filter = getGQLFilter(filter_state, self.handler)
        
        # 比较结果
        self.assertEqual(
            json.dumps(original_filter, sort_keys=True),
            json.dumps(round_trip_filter, sort_keys=True)
        )
    
    def test_empty_filter(self):
        """测试空过滤器"""
        self.assertIsNone(getGQLFilter(None, self.handler))
        self.assertIsNone(getGQLFilter({}, self.handler))
        self.assertIsNone(getGQLFilter({"value": {}}, self.handler))
        
        self.assertIsNone(getFilterState(None))
        self.assertIsNone(getFilterState({"AND": []}))


if __name__ == "__main__":
    unittest.main() 