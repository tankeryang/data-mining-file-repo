# data-mining-file-repo

> 这个 repo 为 __数据挖掘__ 和 __特征工程__ 的脚本库，一般为 SQL 和 爬虫脚本

## 1. 数据挖掘&特征工程

`数据挖掘`，`ETL`和`普适的特征工程`部分一般用 SQL 完成，SQL 脚本放在 `sql/etl/data_warehouse/`下，文件目录如下:

```markdown
sql
├── etl
│   └── data_warehouse
│       ├── ads (应用数据层)
│       │   └── (项目名/schema名)
│       │       └── (表名)
│       │           ├── fully.sql (sql脚本, fully为全量)
│       │           └── increment (sql脚本, increment为增量)
│       ├── cdm (公共维度数据层)
│       │   └── (项目名/schema名)
│       │       └── (表名)
│       │           ├── fully.sql (sql脚本, fully为全量)
│       │           └── increment (sql脚本, increment为增量)
│       └── ods (源数据层)
│           └── (项目名/schema名)
│               └── (表名)
│                   ├── fully.sql (sql脚本, fully为全量)
│                   └── increment (sql脚本, increment为增量)
scrap (一般放爬虫脚本)
└── (项目名)
    └── (爬虫脚本)
```

## 2. 目录结构

* __azkaban__: azkaban 的 job 文件
* __config__: 配置
* __module__: 功能模块
* __scrap__: 爬虫脚本
* __script__: 功能脚本
* __sql__: 特征工程 & ETL 的 SQL 脚本

## 3. azkaban 调度

* 在`azkaban/job/<项目名>/`下编写`job`文件
* 使用`azkaban/pack2zip.sh`打包`job`:
    ```shell
    sh azkaban/pack2zip.sh --zip-name <压缩后的zip文件名> --dir-name <job/下的文件夹名, 即项目名>

    # 例子
    sh azkaban/pack2zip.sh --zip-name mms-mining-training_data --dir-name mms
    ```
* 上传`zip`文件至 azkaban