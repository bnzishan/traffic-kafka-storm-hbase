package com.nano.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2016/3/8.
 */
public class NanoUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NanoUtils.class);

    private NanoUtils() {
    }

    public static double toDouble(Object obj) {
        double l = 0;
        if (obj != null) {
            if (obj instanceof Number) {
                l = ((Number) obj).doubleValue();
            } else {
                LOG.warn("Could not coerce {} to Double", obj.getClass().getName());
            }
        }
        return l;
    }
}
