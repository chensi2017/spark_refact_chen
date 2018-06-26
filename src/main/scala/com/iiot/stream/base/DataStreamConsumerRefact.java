package com.iiot.stream.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.htiiot.resources.model.ThingBullet;
import com.htiiot.resources.utils.DeviceNumber;
import com.htiiot.store.model.Metric;
import com.iiot.alarmimpl.DoCheckData;
import com.iiot.alarmimpl.EventObj;
import com.iiot.stream.bean.DataPoint;
import com.iiot.stream.bean.MetricImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class DataStreamConsumerRefact {
    private static Logger logger = LoggerFactory.getLogger(com.iiot.alarmimpl.DataStreamConsumer.class);

    public DataStreamConsumerRefact() {
    }

    public long[] checkDataPoint(long componentId, String metricName, double metricValue) {
        DoCheckData docheckdata = new DoCheckData();
        Set<String> set = new HashSet();
        List<String> list = new ArrayList();
        String evname = "event_" + componentId + "_" + metricName;
        String statusname = "subscription_eventid_150";
        docheckdata.getKey(statusname);
        String str = docheckdata.getKey(evname);
        docheckdata.close();
        if (str != null && str.length() >= 2) {
            str = str.substring(1, str.length() - 1);
            if (str.indexOf("},{") > -1) {
                String[] ss = str.split("},");

                for(int i = 0; i < ss.length; ++i) {
                    if (i == ss.length - 1) {
                        set.add(ss[i]);
                    } else {
                        set.add(ss[i] + "}");
                    }
                }
            } else {
                set.add(str);
            }

            if (set.isEmpty()) {
                return null;
            } else {
                Iterator it = set.iterator();

                while(it.hasNext()) {
                    String result = (String)it.next();
                    String id = "";
                    String op = "";
                    String min = "";
                    String max = "";
                    ObjectMapper mapper = new ObjectMapper();

                    try {
                        EventObj evt = (EventObj)mapper.readValue(result, EventObj.class);
                        id = evt.getId();
                        op = evt.getOp();
                        min = evt.getMin();
                        max = evt.getMax();
                    } catch (IOException var27) {
                        var27.printStackTrace();
                        logger.debug("can not get properties from json");
                    }

                    double mindb;
                    double maxdb;
                    try {
                        mindb = Double.parseDouble(min);
                        maxdb = Double.parseDouble(max);
                    } catch (Exception var26) {
                        var26.printStackTrace();
                        logger.debug("can not Double.parseDouble(min/max)");
                        return null;
                    }

                    boolean is = subCheckDataPoint(metricValue, op, mindb, maxdb);
                    if (is) {
                        list.add(id);
                    }
                }

                long[] ids = new long[list.size()];

                for(int i = 0; i < ids.length; ++i) {
                    ids[i] = Long.parseLong((String)list.get(i));
                }

                return ids;
            }
        } else {
//            logger.debug("can not find-- " + evname + " --in redis");
            return null;
        }
    }

    public long[] checkDataPoint(DataPoint point) {
        DeviceNumber deviceNumber = point.getDeviceNumber();
        byte[] componentIdb = deviceNumber.getComponentId();
        ByteBuffer bf = ByteBuffer.allocate(8);
        bf.put(componentIdb);
        bf.flip();
        long componentId = bf.getLong();
        Metric metric = point.getMetric();
        String metricName = metric.getName();
        double metricValue = metric.getValue().doubleValue();
        return this.checkDataPoint(componentId, metricName, metricValue);
    }

    public static boolean subCheckDataPoint(double metricValue, String op, double mindb, double maxdb) {
        if (!"=".equals(op) && !"0".equals(op)) {
            if (!">".equals(op) && !"1".equals(op)) {
                if (!"<".equals(op) && !"2".equals(op)) {
                    if (!">=".equals(op) && !"3".equals(op)) {
                        if (!"<=".equals(op) && !"4".equals(op)) {
                            if (!"[]".equals(op) && !"5".equals(op)) {
                                if (!"[)".equals(op) && !"6".equals(op)) {
                                    if (!"(]".equals(op) && !"7".equals(op)) {
                                        if (!"()".equals(op) && !"8".equals(op)) {
                                            return false;
                                        } else {
                                            return metricValue > mindb && metricValue < maxdb;
                                        }
                                    } else {
                                        return metricValue > mindb && metricValue <= maxdb;
                                    }
                                } else {
                                    return metricValue >= mindb && metricValue < maxdb;
                                }
                            } else {
                                return metricValue >= mindb && metricValue <= maxdb;
                            }
                        } else {
                            return metricValue <= mindb;
                        }
                    } else {
                        return metricValue >= mindb;
                    }
                } else {
                    return metricValue < mindb;
                }
            } else {
                return metricValue > mindb;
            }
        } else {
            return metricValue == mindb;
        }
    }

    public static String getIetm(String json, String item) {
        String s = "";
        ObjectMapper mapper = new ObjectMapper();

        try {
            EventObj evt = (EventObj)mapper.readValue(json, EventObj.class);
            s = evt.getId();
        } catch (IOException var6) {
            var6.printStackTrace();
        }

        return s;
    }

    public void init() {
    }

    public void destroy() {
    }

    public long getVersion() {
        return 0L;
    }

    public ThingBullet[] checkDataPointToThingBullet(DataPoint point) {
        DeviceNumber deviceNumber = point.getDeviceNumber();
        long ts = point.getTs();
        Metric metric = point.getMetric();
//        String metricName = metric.getName();
        double metricValue = metric.getValue().doubleValue();
        Set<String> set = new HashSet();
        List<ThingBullet> list = new ArrayList();

        //acquire alarm threshold
        String str = ((MetricImpl)point.getMetric()).getThreshold();

        if (str != null && str.length() >= 2) {
            str = str.substring(1, str.length() - 1);
            if (str.indexOf("},{") > -1) {
                String[] ss = str.split("},");

                for(int i = 0; i < ss.length; ++i) {
                    if (i == ss.length - 1) {
                        set.add(ss[i]);
                    } else {
                        set.add(ss[i] + "}");
                    }
                }
            } else {
                set.add(str);
            }

            if (set.isEmpty()) {
                return null;
            } else {
                Iterator it = set.iterator();

                while(it.hasNext()) {
                    String result = (String)it.next();
                    String id = "";
                    String op = "";
                    String min = "";
                    String max = "";
                    ObjectMapper mapper = new ObjectMapper();

                    try {
                        EventObj evt = (EventObj)mapper.readValue(result, EventObj.class);
                        id = evt.getId();
                        op = evt.getOp();
                        min = evt.getMin();
                        max = evt.getMax();
                    } catch (IOException var35) {
                        var35.printStackTrace();
                        logger.debug("can not get properties from json");
                    }

                    double mindb;
                    double maxdb;
                    try {
                        mindb = Double.parseDouble(min);
                        maxdb = Double.parseDouble(max);
                    } catch (Exception var34) {
                        var34.printStackTrace();
                        logger.debug("can not Double.parseDouble(min/max)");
                        return null;
                    }

                    boolean is = subCheckDataPoint(metricValue, op, mindb, maxdb);
                    String statusid = "";
                    if (is && !"1".equals(statusid) || !is && "1".equals(statusid)) {
                        ThingBullet tb = new ThingBullet();
                        tb.setDn(deviceNumber);
                        tb.setTs(ts);
                        tb.setValue(metricValue);
                        tb.setEventId(Long.parseLong(id));
                        if (is) {
                            tb.setStatus((short)1);
                        } else {
                            tb.setStatus((short)2);
                        }

                        list.add(tb);
                    }
                }

                ThingBullet[] ids = new ThingBullet[list.size()];

                for(int i = 0; i < ids.length; ++i) {
                    ids[i] = (ThingBullet)list.get(i);
                }

                return ids;
            }
        } else {
//            logger.info("can not find-- event_" + ((MetricImpl)point.getMetric()).getMetricId() + "_" + point.getMetric().getName() + " --in redis");
            return null;
        }
    }

    public String checkStatusinfo(String id) {
        DoCheckData docheckdata = new DoCheckData();
        String statusname = "subscription_eventid_" + id;
        String status = docheckdata.getKey(statusname);
        docheckdata.close();
        return status;
    }
}
