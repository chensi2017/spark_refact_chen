package com.iiot.stream.bean;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.htiiot.store.model.DataPointDeserializer;
import com.htiiot.store.model.DataPointSerializer;
import com.htiiot.resources.utils.DeviceNumber;
import com.htiiot.store.model.Metric;
import org.apache.commons.lang.StringUtils;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@JsonSerialize(using = DataPointSerializer.class)
@JsonDeserialize(using = DataPointDeserializer.class)
public class DataPoint implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = -3626511735222931717L;

    private DeviceNumber dn;

    private long ts;

    private Map<String, String> tags = new HashMap<String, String>();

    private Metric metric;

    public DeviceNumber getDeviceNumber() {
        return dn;
    }

    public void setDeviceNumber(DeviceNumber dn) {
        this.dn = dn;
        updateInternalTags();
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void addTag(String tagK, String TagV) {
        this.tags.put(tagK, TagV);
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Metric getMetric() {
        return metric;
    }

    public void setMetric(Metric metric) {
        this.metric = metric;
    }

    /**
     * check if data point is valid
     *
     * @return true or false
     */
    public boolean valid() {
        return ts > 0L && StringUtils.isNotEmpty(this.metric.getName());
    }

    /**
     * add internal tags for datapoint with thingid and ...
     */
    private void updateInternalTags() {
        if (this.dn != null) {
            this.addTag("thingid", com.htiiot.store.model.DataPoint.bytesToInt(this.dn.getThingId()) + "");
            this.addTag("compid", com.htiiot.store.model.DataPoint.bytesToLong(this.dn.getComponentId()) + "");
            this.addTag("tenantid", com.htiiot.store.model.DataPoint.bytesToInt(this.dn.getTenantId()) + "");
        }

    }

    public static int bytesToInt(byte[] b)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(b);
        buffer.flip();
        return buffer.getInt();
    }

    public static byte[] intToBytes(int x)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(x);
        return buffer.array();
    }

    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public String convertTime(long time){
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date);
    }

    @Override
    public String toString() {
        return "DataPoint [dn=" + dn + ", ts=" + convertTime(ts) + ", tags=" + tags + ", metric=" + metric + "]";
    }
}
