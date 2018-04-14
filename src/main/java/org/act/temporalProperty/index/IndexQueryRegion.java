package org.act.temporalProperty.index;

import org.act.temporalProperty.index.rtree.RTreeRange;
import org.act.temporalProperty.util.Slice;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by song on 2018-01-18.
 */
public class IndexQueryRegion {
    private int timeMin;
    private int timeMax;
    private List<PropertyValueInterval> pValues = new ArrayList<>();

    public IndexQueryRegion(int timeMin, int timeMax) {
        this.timeMin = timeMin;
        this.timeMax = timeMax;
    }

    public int getTimeMin() {
        return timeMin;
    }

    public int getTimeMax() {
        return timeMax;
    }

    public void add(PropertyValueInterval propertyValueInterval){
        this.pValues.add(propertyValueInterval);
    }

    public List<PropertyValueInterval> getPropertyValueIntervals() {
        return pValues;
    }
}
