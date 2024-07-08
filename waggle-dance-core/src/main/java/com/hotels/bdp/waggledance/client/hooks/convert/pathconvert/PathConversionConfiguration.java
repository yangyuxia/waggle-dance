package com.hotels.bdp.waggledance.client.hooks.convert.pathconvert;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.BooleanUtils;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
@Slf4j
public class PathConversionConfiguration {
    private static final String PATH_REPLACEMENT_PREFIX = "hive.metastore.hooks.path.replacement";
    public static final String PATH_REPLACEMENT_ENABLED = format("%s.enabled", PATH_REPLACEMENT_PREFIX);
    public static final String PATH_REPLACEMENT_REGEX = format("%s.regex", PATH_REPLACEMENT_PREFIX);
    public static final String PATH_REPLACEMENT_VALUES = format("%s.value", PATH_REPLACEMENT_PREFIX);
    private static final String PATH_REPLACEMENT_GROUPS = format("%s.capturegroups", PATH_REPLACEMENT_PREFIX);

    private final boolean pathConversionEnabled;
    private final List<PathConversionRule> pathConversionRules;
    private final Properties properties;

    public PathConversionConfiguration(HiveConf conf) {
        properties=conf.getAllProperties();
        pathConversionEnabled = BooleanUtils.toBoolean(properties.getProperty(PATH_REPLACEMENT_ENABLED, "false"));
        pathConversionRules = initializePathReplacements();
    }

    private List<PathConversionRule> initializePathReplacements() {
        List<PathConversionRule> pathConversionRules = new ArrayList<>();
        for (Object key : properties.keySet()) {
            String currentPropertyName = (String) key;

            if (currentPropertyName.startsWith(PATH_REPLACEMENT_REGEX)) {
                String valuePropertyName = currentPropertyName.replace(PATH_REPLACEMENT_REGEX, PATH_REPLACEMENT_VALUES);
                String value = properties.getProperty(valuePropertyName);

                if (value == null) {
                    log.warn("Non-existent value property for PathMatchProperty[{}]. " +
                                    "This will not be replaced, please reconfigure Apiary Metastore Filter in hive-site.xml",
                            currentPropertyName);
                    continue;
                }

                String captureGroupPropertyName = currentPropertyName.replace(PATH_REPLACEMENT_REGEX, PATH_REPLACEMENT_GROUPS);
                List<Integer> captureGroups = getCaptureGroups(captureGroupPropertyName);

                Pattern pattern = Pattern.compile(properties.getProperty(currentPropertyName));
                pathConversionRules.add(new PathConversionRule(pattern, value, captureGroups));
                log.debug("Tracking PathMatchProperty[{}] for path conversion.", currentPropertyName);
            }
        }
        return ImmutableList.copyOf(pathConversionRules);
    }

    public boolean isPathConversionEnabled() {
        return pathConversionEnabled;
    }

    public List<PathConversionRule> getPathConversionRules() {
        return pathConversionRules;
    }

    private List<Integer> getCaptureGroups(String propertyName) {
        String captureGroups = properties.getProperty(propertyName, "1");
        return Arrays.stream(captureGroups.split(",")).map(Integer::parseInt).collect(Collectors.toList());
    }
}
