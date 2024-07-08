package com.hotels.bdp.waggledance.client.hooks.convert.pathconvert;

import com.hotels.bdp.waggledance.client.hooks.convert.GenericConverter;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

@Slf4j
public class PathConverter extends GenericConverter{
    public static final String SD_INFO_PATH_PARAMETER = "path";
    public static final String TABLE_AVRO_SCHEMA_URL_PARAMETER = "avro.schema.url";

    private final PathConversionConfiguration pathConversionConfiguration;

    public PathConverter(HiveConf hiveConf) {
        super(hiveConf);
        this.pathConversionConfiguration=new PathConversionConfiguration(hiveConf);
    }

    public PathConversionConfiguration getPathConversionConfiguration() {
        return this.pathConversionConfiguration;
    }

    /**
     * Converts a path for the given Table.
     *
     * @param table Table location to potentially alter.
     * @return true if table location is altered, false otherwise.
     */
    @Override
    public boolean convertTable(Table table) {
        if (!pathConversionConfiguration.isPathConversionEnabled()) {
            log.trace("PathConversion is disabled. Skipping path conversion for table.");
            return false;
        }
        boolean tableConverted = false;
        if (table.isSetParameters()) {
            String parameterPath = table.getParameters().get(TABLE_AVRO_SCHEMA_URL_PARAMETER);
            if (!Strings.isNullOrEmpty(parameterPath)) {
                String newParameterPath = convert(parameterPath);
                Map<String, String> parameters = new HashMap<>(table.getParameters());
                parameters.put(TABLE_AVRO_SCHEMA_URL_PARAMETER, newParameterPath);
                table.setParameters(parameters);
                tableConverted |= !parameterPath.equals(newParameterPath);
            }
        }

        StorageDescriptor sd = table.getSd();
        log.debug("Examining table location: {}", sd.getLocation());
        tableConverted |= convertStorageDescriptor(sd, Warehouse.getQualifiedName(table));
        return tableConverted;
    }

    /**
     * Converts a path for the given Partition.
     *
     * @param partition Partition location to potentially alter.
     * @return true if partition location is altered, false otherwise.
     */
    @Override
    public boolean convertPartition(Partition partition) {
        if (!pathConversionConfiguration.isPathConversionEnabled()) {
            log.trace("PathConversion is disabled. Skipping path conversion for partition.");
            return false;
        }

        StorageDescriptor sd = partition.getSd();
        log.debug("Examining partition location: {}", sd.getLocation());
        return convertStorageDescriptor(sd, Warehouse.getQualifiedName(partition));
    }

    @Override
    public boolean convertStorageDescriptor(StorageDescriptor sd) {
        return convertStorageDescriptor(sd, "");
    }

    private boolean convertStorageDescriptor(StorageDescriptor sd, String qualifiedTableName) {
        boolean pathConverted = false;
        String currentLocation = sd.getLocation();
        if (!Strings.isNullOrEmpty(currentLocation)) {
            sd.setLocation(convert(currentLocation));
            log.info("Switching storage location {} to {}.", currentLocation, sd.getLocation());
            pathConverted = !currentLocation.equals(sd.getLocation());
        } else {
            log.info("Switching storage location not possible empty/null location, table: {}", qualifiedTableName);
        }
        if (sd.isSetSerdeInfo() && sd.getSerdeInfo().isSetParameters()) {
            String parameterPath = sd.getSerdeInfo().getParameters().get(SD_INFO_PATH_PARAMETER);
            if (!Strings.isNullOrEmpty(parameterPath)) {
                String newParameterPath = convert(parameterPath);
                Map<String, String> parameters = new HashMap<>(sd.getSerdeInfo().getParameters());
                parameters.put(SD_INFO_PATH_PARAMETER, newParameterPath);
                sd.getSerdeInfo().setParameters(parameters);
                pathConverted |= !parameterPath.equals(newParameterPath);
            }
        }
        return pathConverted;
    }

    private String convert(String location) {
        String newLocation = location;
        boolean pathConverted = false;
        for (PathConversionRule pathConversion : pathConversionConfiguration.getPathConversionRules()) {
            Matcher matcher = pathConversion.pathPattern.matcher(newLocation);
            if (matcher.find()) {
                StringBuilder newLocationBuilder = new StringBuilder(newLocation);
                int offset = 0;

                for (Integer captureGroup : pathConversion.captureGroups) {
                    if (hasCaptureGroup(matcher, captureGroup, newLocation)) {
                        newLocationBuilder
                                .replace(matcher.start(captureGroup) + offset, matcher.end(captureGroup) + offset,
                                        pathConversion.replacementValue);
                        offset += pathConversion.replacementValue.length() - matcher.group(captureGroup).length();
                        pathConverted = true;
                    }
                }

                if (pathConverted) {
                    newLocation = newLocationBuilder.toString();
                }
            }
        }
        return newLocation;
    }

    private boolean hasCaptureGroup(Matcher matcher, int groupNumber, String location) {
        try {
            matcher.group(groupNumber);
            return true;
        } catch (IndexOutOfBoundsException ex) {
            log.warn("No capture group number {} found on location[{}]", groupNumber, location);
            return false;
        }
    }
}
