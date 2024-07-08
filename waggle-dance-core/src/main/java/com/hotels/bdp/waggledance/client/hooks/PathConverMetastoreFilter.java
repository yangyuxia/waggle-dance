package com.hotels.bdp.waggledance.client.hooks;

import com.hotels.bdp.waggledance.client.hooks.convert.pathconvert.PathConverter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PathConverMetastoreFilter implements MetaStoreFilterHook {
    private final static Logger log = LoggerFactory.getLogger(PathConverMetastoreFilter.class);
    private final PathConverter converter;

    public PathConverMetastoreFilter(HiveConf conf) {
        converter = new PathConverter(conf);
    }

    public PathConverter getConverter() {
        return converter;
    }

    @Override
    public List<String> filterDatabases(List<String> dbList) {
        return dbList;
    }

    @Override
    public Database filterDatabase(Database dataBase) {
        return dataBase;
    }

    @Override
    public List<String> filterTableNames(String catalogName,String dbName, List<String> tableList) {
        return tableList;
    }

    @Override
    public List<PartitionSpec> filterPartitionSpecs(List<PartitionSpec> partitionSpecList) {
        return partitionSpecList;
    }

    @Override
    public List<String> filterPartitionNames(String catalogName,String dbName, String tblName, List<String> partitionNames) {
        return partitionNames;
    }

    @Override
    public List<Table> filterTables(List<Table> tableList) {
        for (Table table : tableList) {
            filterTable(table);
        }
        return tableList;
    }

    private String getQualifiedName(Table table) {
        return table.getDbName() + "." + table.getTableName();
    }

    @Override
    public Table filterTable(Table table) {
        try {
            converter.convertTable(table);
        } catch (Exception e) {
            log.error("Failed to convert table " + getQualifiedName(table), e);
        }
        return table;
    }

    @Override
    public List<Partition> filterPartitions(List<Partition> partitionList) {
        for (Partition partition : partitionList) {
            filterPartition(partition);
        }
        return partitionList;
    }

    @Override
    public Partition filterPartition(Partition partition) {
        try {
            converter.convertPartition(partition);
        } catch (Exception e) {
            log.error("Failed to convert partition " + getQualifiedName(partition), e);
        }
        return partition;
    }

    @Override
    public List<TableMeta> filterTableMetas(List<TableMeta> tableList) throws MetaException {
        return tableList;
    }

    private String getQualifiedName(Partition partition) {
        return partition.getDbName() + "." + partition.getTableName() + "." + partition.getValues();
    }
}
