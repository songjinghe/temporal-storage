package edu.buaa.act.temporal.impl;

import edu.buaa.act.temporal.ValueAtTime;

import java.util.Comparator;

/**
 * Created by song on 2018-01-10.
 */
public abstract class ValueOperator implements Comparator<ValueAtTime>
{
    public abstract ValueAtTime avg(ValueAtTime value, ValueAtTime value1);
}
