## Overview
PathConvertMetastoreFilter是一个实现了hive 3.X MetaStoreFilterHook接口的过滤器钩子(filter hook)，用于对表的hdfs路径进行相关规则的转换，而无需更改底层元数据。
支持在waggle-dance-federation.yml中，为单个HMS指定是否启用该hook。

## 路径转换范围
仅对table及partition的路径进行转换。
| Hook Type       | Enabled? |
|-----------------|----------|
| Table(s)        | true     |
| Partition(s)    | true     |
| Database(s)     | false    |
| Index(es)       | false    |
| Table Names     | false    |
| Partition Specs | false    |
| Index Names     | false    |

## 配置
| Property                                                           | Description                                                                             | Default                     |
|--------------------------------------------------------------------|-----------------------------------------------------------------------------------------|-----------------------------|
| hive-metastore-filter-hook                                         | 指定filter hook类，如路径转换钩子的类名为：com.hotels.bdp.waggledance.client.hooks.PathConvertMetastoreFilter |                             |
| configuration-properties.waggledance.hook.path.replacement.enabled | true or false,是否启用路径转换钩子，默认为false。                                                      | false |
 
### 配置路径转换规则：可配置多组
| Property                                                                       | Description                                                                            | Default                     |
|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|-----------------------------|
| configuration-properties.waggledance.hook.path.replacement.regex.$ruleName     | Defined regex patterns to check for replacement. Requires matching value.              | [] |
| configuration-properties.waggledance.hook.path.replacement.value.$ruleName     | Defined value patterns to check for replacement. Requires matching regex.              | [] |
| configuration-properties.waggledance.hook.path.replacement.capturegroups.$ruleName | (Optional) Comma delimited list of capture group indexes to use for regex replacement. | [1] |

## 举例
    primary-meta-store:
      name: dp6
      database-prefix: ''
      remote-meta-store-uris: thrift://host-hms:9083
      access-control-type: READ_AND_WRITE_AND_CREATE
      hive-metastore-filter-hook: com.hotels.bdp.waggledance.client.hooks.PathConvertMetastoreFilter
      configuration-properties:
        waggledance.hook.path.replacement.enabled: false
        waggledance.hook.path.replacement.regex.rbf: ^(hdfs://sharecluster/)(?:.*)
        waggledance.hook.path.replacement.capturegroups.rbf: 1
        waggledance.hook.path.replacement.value.rbf: hdfs://dp6/