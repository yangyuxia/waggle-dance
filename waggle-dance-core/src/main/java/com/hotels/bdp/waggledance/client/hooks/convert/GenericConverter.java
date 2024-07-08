package com.hotels.bdp.waggledance.client.hooks.convert;

import com.hotels.bdp.waggledance.client.hooks.convert.pathconvert.PathConversionConfiguration;
import lombok.Getter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

public abstract class GenericConverter {

    @Getter
    private final HiveConf hiveConf;

    protected GenericConverter(HiveConf configuration) {
        this.hiveConf = configuration;
    }

    /**
     * Apply a conversion for a given table.
     *
     * @param table Table to potentially alter.
     * @return true if table is altered, false otherwise.
     */
    public abstract boolean convertTable(Table table);

    /**
     * Apply a conversion for the given Partition.
     *
     * @param partition Partition to potentially alter.
     * @return true if partition is altered, false otherwise.
     */
    public abstract boolean convertPartition(Partition partition);

    /**
     * Apply a conversion for the given StorageDescriptor.
     *
     * @param sd StorageDescriptor to potentially alter.
     * @return true if SD is altered, false otherwise.
     */
    public abstract boolean convertStorageDescriptor(StorageDescriptor sd);
}
