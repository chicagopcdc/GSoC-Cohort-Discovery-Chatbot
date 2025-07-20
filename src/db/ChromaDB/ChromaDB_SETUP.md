# ChromaDB 集成设置指南

## 1. 安装依赖

首先安装更新的依赖：

```bash
pip install -r requirements.txt
```

新增的依赖包括：
- `chromadb==0.4.15` - 向量数据库
- `langchain-community>=0.0.10` - Langchain 社区版本

## 2. 功能概览

### 自动存储功能
现在 `chainlit_app.py` 会自动将所有 LLM 响应存储到 ChromaDB 中，包括：
- 用户查询
- GraphQL 查询结果
- 查询变量
- 解释说明
- 会话信息和时间戳

### 存储位置
- ChromaDB 数据存储在 `./chroma_db/` 目录中
- 同时保持原有的 `chat_history/` 文本文件存储

## 3. 使用方法

### 启动 Chainlit 应用
```bash
chainlit run chainlit_app.py
```

现在每次对话都会自动保存到 ChromaDB！

### 查询和管理数据

#### 命令行工具使用

1. **搜索相似响应**：
```bash
python ChromaDB/chroma_utils.py --search "GraphQL query for users"
```

2. **查看会话历史**：
```bash
python ChromaDB/chroma_utils.py --session "your-session-id"
```

3. **查看数据库统计**：
```bash
python ChromaDB/chroma_utils.py --stats
```

4. **导出数据**：
```bash
python ChromaDB/chroma_utils.py --export export_data.json
```

5. **清空数据库**：
```bash
python ChromaDB/chroma_utils.py --clear
```

6. **交互式模式**：
```bash
python ChromaDB/chroma_utils.py
```

#### Python 代码使用

```python
from ChromaDB.chroma_manager import ChromaDBManager

# 初始化
chroma_manager = ChromaDBManager()

# 搜索相似响应
results = chroma_manager.search_similar_responses("GraphQL query", k=5)

# 获取会话历史
history = chroma_manager.get_session_history("session-id")

# 查看统计
stats = chroma_manager.get_statistics()
```

## 4. 文件结构

```
project/
├── chainlit_app.py          # 主应用（已修改）
├── ChromaDB/                # ChromaDB 包目录
│   ├── __init__.py         # 包初始化文件
│   ├── chroma_manager.py   # ChromaDB 管理类（新增）
│   ├── chroma_utils.py     # 管理工具（新增）
│   └── CHROMADB_SETUP.md   # 设置说明文档
├── requirements.txt         # 依赖（已更新）
├── chroma_db/              # ChromaDB 数据目录（自动创建）
│   └── ...
└── chat_history/           # 原有文本存储（保持不变）
    └── ...
```

## 5. 环境变量

确保在 `.env` 文件中设置：
```
OPENAI_API_KEY=your_openai_api_key_here
```

## 6. 功能特性

### 🔍 语义搜索
- 基于向量相似度的智能搜索
- 可以找到意思相近的查询和响应

### 📊 结构化存储
- 自动提取和存储元数据
- 支持按会话、时间、类型等筛选

### 🚀 高性能
- 向量化存储，搜索速度快
- 支持大量数据的高效检索

### 🔧 灵活管理
- 完整的命令行工具
- 支持数据导出和备份

## 7. 故障排除

### 常见问题

1. **ImportError: No module named 'chromadb'**
   ```bash
   pip install chromadb>=0.4.15
   ```

2. **权限错误**
   确保对 `chroma_db/` 目录有写权限

3. **OpenAI API 错误**
   检查 `.env` 文件中的 API 密钥设置

### 重置数据库
如果遇到数据库问题，可以完全重置：
```bash
rm -rf chroma_db/
python chroma_utils.py --clear
```

## 8. 高级使用

### 自定义配置
可以在 `ChromaDB/chroma_manager.py` 中修改：
- 存储目录
- 集合名称
- 嵌入模型等

### 批量导入数据
可以修改 `ChromaDB/chroma_utils.py` 添加批量导入功能

### 与其他系统集成
ChromaDB 支持多种客户端，可以与其他应用集成使用相同的数据库 