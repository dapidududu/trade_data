from decimal import Decimal

import numpy
import json

data = [{"id": "1", "name": "Alice", "price": "9.1231231", "is": "FALSE"},
        {"id": 1, "name": "Alice", "price": 100.5453, "is": True}]
def convert_value(value):
    """转换数据类型：数值转换为 float，布尔值转换为 bool，字符串保持 str"""
    if isinstance(value, str):
        # 处理布尔值
        if value.lower() in ["true", "false"]:
            return value.lower() == "true"
        # 处理数值
        try:
            return numpy.float64(value) if "." in value else int(value)
        except ValueError:
            return value  # 如果转换失败，保持原始字符串
    elif isinstance(value, Decimal):
        return numpy.float64(value)
    return value  # 其他类型保持不变


for line in data:
    line_data = {key: convert_value(value) for key, value in line.items()}
    print(line_data)

    line_str = json.dumps(line_data)
    print(line_str)
