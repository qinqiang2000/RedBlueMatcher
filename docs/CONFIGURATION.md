# 配置文档

本文档说明如何配置 RedBlueMatcher 的数据库连接和表名设置。

## 快速开始

### 1. 创建配置文件

从模板复制配置文件：

```bash
cp .env.example .env
```

### 2. 编辑配置

打开 `.env` 文件，修改数据库连接信息：

```bash
# 数据库配置
DB_HOST=localhost          # 数据库主机地址
DB_PORT=5432              # 数据库端口
DB_NAME=qinqiang02        # 数据库名称
DB_USER=qinqiang02        # 数据库用户名
DB_PASSWORD=              # 数据库密码（如果有）

# 表名后缀
TABLE_SUFFIX=_1201        # 表名后缀（包含下划线）
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 测试配置

```bash
python -c "from config import load_config; load_config(); print('配置加载成功！')"
```

### 5. 运行程序

```bash
# 测试运行（处理前10条）
python red_blue_matcher.py --test-limit 10

# 完整运行
python red_blue_matcher.py

# 指定匹配算法运行
python red_blue_matcher.py --algorithm greedy_large

# 组合参数运行
python red_blue_matcher.py --test-limit 100 --algorithm greedy_large --output results.xlsx
```

## 命令行参数

### 主要参数

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `--algorithm` | str | `greedy_large` | 匹配算法（可选） |
| `--test-limit` | int | 无 | 测试模式：仅处理前N条负数单据 |
| `--output` | str | `match_results.xlsx` | 输出XLSX文件路径 |

### 算法选择

目前支持的算法：

- **`greedy_large`**（默认）：贪心大额优先算法
  - 优先精确匹配：使用 NumPy 向量化查找
  - 贪心消耗：按蓝票金额从大到小消耗
  - 整数数量优先：尽量使红冲数量为整数

#### 使用示例

```bash
# 使用默认算法
python red_blue_matcher.py

# 显式指定算法
python red_blue_matcher.py --algorithm greedy_large

# 查看可用算法
python red_blue_matcher.py --help
```

#### 命令行帮助

```bash
$ python red_blue_matcher.py --help

usage: red_blue_matcher.py [-h] [--test-limit N] [--output FILE]
                           [--algorithm NAME]

负数发票自动匹蓝算法

options:
  -h, --help        show this help message and exit
  --test-limit N    测试模式：仅处理前N条负数单据（不更新数据库状态）
  --output FILE     输出XLSX文件路径（默认: match_results.xlsx）
  --algorithm NAME  匹配算法（可选: greedy_large，默认: greedy_large）
```

## 配置参数详解

### 必需参数

| 参数名 | 说明 | 示例 |
|--------|------|------|
| `DB_HOST` | 数据库主机地址 | `localhost` 或 `192.168.1.100` |
| `DB_PORT` | 数据库端口 | `5432`（PostgreSQL 默认端口） |
| `DB_NAME` | 数据库名称 | `qinqiang02` |
| `DB_USER` | 数据库用户名 | `qinqiang02` |
| `DB_PASSWORD` | 数据库密码 | 留空表示无密码 |

### 可选参数

| 参数名 | 说明 | 默认值 | 示例 |
|--------|------|--------|------|
| `TABLE_SUFFIX` | 表名后缀（包含下划线） | `""`（空字符串） | `_1201`、`_test` |
| `ENV` | 环境名称 | 无 | `dev`、`test`、`prod` |

## 表名解析规则

系统使用以下规则动态构造表名：

```
表名 = 基础表名 + TABLE_SUFFIX
```

### 示例

**配置 1: 带后缀**
```bash
TABLE_SUFFIX=_1201
```

生成的表名：
- `t_sim_original_bill_1201`
- `t_sim_original_bill_item_1201`
- `t_sim_vatinvoice_1201`
- `t_sim_vatinvoice_item_1201`

**配置 2: 无后缀**
```bash
TABLE_SUFFIX=
```

生成的表名：
- `t_sim_original_bill`
- `t_sim_original_bill_item`
- `t_sim_vatinvoice`
- `t_sim_vatinvoice_item`

## 多环境配置

### 方式一：使用环境变量 ENV

创建多个环境配置文件：

```bash
# 开发环境
.env.dev

# 测试环境
.env.test

# 生产环境
.env.prod
```

运行时指定环境：

```bash
# 使用开发环境
export ENV=dev
python red_blue_matcher.py

# 或者在 .env 文件中设置
echo "ENV=dev" > .env
```

### 方式二：直接切换配置文件

```bash
# 使用开发环境
cp .env.dev .env
python red_blue_matcher.py

# 使用生产环境
cp .env.prod .env
python red_blue_matcher.py
```

## 配置文件示例

### 开发环境 (.env.dev)

```bash
ENV=dev
DB_HOST=localhost
DB_PORT=5432
DB_NAME=qinqiang02_dev
DB_USER=dev_user
DB_PASSWORD=dev_password
TABLE_SUFFIX=_1201
```

### 测试环境 (.env.test)

```bash
ENV=test
DB_HOST=test-db.example.com
DB_PORT=5432
DB_NAME=qinqiang02_test
DB_USER=test_user
DB_PASSWORD=test_password
TABLE_SUFFIX=_1201
```

### 生产环境 (.env.prod)

```bash
ENV=prod
DB_HOST=prod-db.example.com
DB_PORT=5432
DB_NAME=qinqiang02_prod
DB_USER=prod_user
DB_PASSWORD=prod_password
TABLE_SUFFIX=_1201
```

## 常见问题

### Q1: 配置文件不存在

**错误信息：**
```
❌ 配置加载失败: 配置文件 .env 不存在
```

**解决方法：**
```bash
cp .env.example .env
# 然后编辑 .env 文件
```

### Q2: 缺少必需配置

**错误信息：**
```
❌ 配置加载失败: 配置缺少必需字段: DB_HOST, DB_NAME
```

**解决方法：**
在 `.env` 文件中添加缺少的配置项。

### Q3: 数据库连接失败

**错误信息：**
```
psycopg2.OperationalError: could not connect to server
```

**解决方法：**
1. 检查数据库服务是否启动
2. 检查 `DB_HOST` 和 `DB_PORT` 是否正确
3. 检查防火墙设置
4. 检查用户名和密码是否正确

### Q4: 表不存在

**错误信息：**
```
psycopg2.errors.UndefinedTable: relation "t_sim_original_bill_1201" does not exist
```

**解决方法：**
检查 `TABLE_SUFFIX` 配置是否与实际表名匹配：

```bash
# 如果实际表名是 t_sim_original_bill_1201
TABLE_SUFFIX=_1201

# 如果实际表名是 t_sim_original_bill（无后缀）
TABLE_SUFFIX=
```

### Q5: 端口配置错误

**错误信息：**
```
❌ 配置加载失败: DB_PORT 必须在 1-65535 范围内
```

**解决方法：**
检查 `DB_PORT` 是否为有效的端口号（1-65535）。

## 安全最佳实践

### 1. 不要提交配置文件到 Git

`.env` 文件已在 `.gitignore` 中忽略，但请确保：

```bash
# 检查 .gitignore
cat .gitignore

# 应包含：
# .env
# .env.local
# .env.*.local
```

### 2. 使用环境变量（生产环境）

在生产环境中，建议通过系统环境变量设置配置，而不是使用 `.env` 文件：

```bash
export DB_HOST=prod-db.example.com
export DB_PORT=5432
export DB_NAME=qinqiang02_prod
export DB_USER=prod_user
export DB_PASSWORD=secure_password
export TABLE_SUFFIX=_1201
```

### 3. 限制配置文件权限

```bash
chmod 600 .env
```

### 4. 定期更新密码

定期更换数据库密码，并更新 `.env` 文件。

## 迁移指南

### 从硬编码配置迁移

如果你之前使用的是硬编码配置，迁移步骤如下：

#### 1. 备份现有文件

```bash
cp red_blue_matcher.py red_blue_matcher.py.backup
cp audit_results.py audit_results.py.backup
```

#### 2. 创建 .env 配置

```bash
cp .env.example .env
# 编辑 .env，填入之前硬编码的值
```

#### 3. 安装新依赖

```bash
pip install python-dotenv
```

#### 4. 测试运行

```bash
# 小数据量测试
python red_blue_matcher.py --test-limit 10

# 验证输出是否与之前一致
```

#### 5. 完整运行

确认测试通过后，执行完整运行。

## 配置验证工具

### 手动验证配置

```bash
python config.py
```

输出示例：
```
正在测试配置加载...

已加载配置文件: .env
配置加载成功:
  数据库: qinqiang02@localhost:5432
  表后缀: '_1201'
============================================================
当前配置信息
============================================================
数据库配置:
  Host: localhost
  Port: 5432
  Database: qinqiang02
  User: qinqiang02
  Password: (空)

表名配置:
  后缀: '_1201'
  负数单据主表: t_sim_original_bill_1201
  负数单据明细表: t_sim_original_bill_item_1201
  蓝票主表: t_sim_vatinvoice_1201
  蓝票明细表: t_sim_vatinvoice_item_1201
============================================================

配置测试成功!
```

## 技术支持

如果遇到配置问题，请检查：

1. `.env` 文件是否存在且格式正确
2. 所有必需字段是否已填写
3. 数据库连接是否正常
4. 表名配置是否与实际数据库表匹配

更多帮助，请查看项目 README 或提交 Issue。
