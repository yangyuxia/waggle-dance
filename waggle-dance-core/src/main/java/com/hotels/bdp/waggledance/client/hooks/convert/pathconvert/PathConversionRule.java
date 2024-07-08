package com.hotels.bdp.waggledance.client.hooks.convert.pathconvert;

import lombok.AllArgsConstructor;

import java.util.List;
import java.util.regex.Pattern;

@AllArgsConstructor
public class PathConversionRule {
    public Pattern pathPattern;
    public String replacementValue;
    public List<Integer> captureGroups;
}
