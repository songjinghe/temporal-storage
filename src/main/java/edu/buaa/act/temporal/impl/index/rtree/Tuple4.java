package edu.buaa.act.temporal.impl.index.rtree;

import java.util.Comparator;

/**
 * Created by song on 2018-01-09.
 */
public abstract class Tuple4<
        A extends Comparable<A>,
        B extends Comparable<B>,
        C extends Comparable<C>,
        D extends Comparable<D>> implements Comparable<Tuple4>
{
    private A a;
    private B b;
    private C c;
    private D d;

    public A getA()
    {
        return a;
    }

    public void setA(A a)
    {
        this.a = a;
    }

    public B getB()
    {
        return b;
    }

    public void setB(B b)
    {
        this.b = b;
    }

    public C getC()
    {
        return c;
    }

    public void setC(C c)
    {
        this.c = c;
    }

    public D getD()
    {
        return d;
    }

    public void setD(D d)
    {
        this.d = d;
    }

    @Override
    public Comparator<edu.buaa.act.temporal.impl.index.rtree.Tuple4<A, B, C, D>> getComparator(int dimIndex)
    {
        switch (dimIndex)
        {
            case 0:
                return Comparator.comparing(edu.buaa.act.temporal.impl.index.rtree.Tuple4::getA);
            case 1:
                return Comparator.comparing(edu.buaa.act.temporal.impl.index.rtree.Tuple4::getB);
            case 2:
                return Comparator.comparing(edu.buaa.act.temporal.impl.index.rtree.Tuple4::getC);
            default:
                return Comparator.comparing(edu.buaa.act.temporal.impl.index.rtree.Tuple4::getD);
        }
    }

    @Override
    public int dimensionCount()
    {
        return 4;
    }

    @Override
    public void updateMin(edu.buaa.act.temporal.impl.index.rtree.Tuple4<A, B, C, D> value)
    {
        if (this.a.compareTo(value.getA()) > 0)
        {
            this.a = value.getA();
        }
        if (this.b.compareTo(value.getB()) > 0)
        {
            this.b = value.getB();
        }
        if (this.c.compareTo(value.getC()) > 0)
        {
            this.c = value.getC();
        }
        if (this.d.compareTo(value.getD()) > 0)
        {
            this.d = value.getD();
        }
    }

    @Override
    public void updateMax(edu.buaa.act.temporal.impl.index.rtree.Tuple4<A, B, C, D> value)
    {
        if (this.a.compareTo(value.getA()) < 0)
        {
            this.a = value.getA();
        }
        if (this.b.compareTo(value.getB()) < 0)
        {
            this.b = value.getB();
        }
        if (this.c.compareTo(value.getC()) < 0)
        {
            this.c = value.getC();
        }
        if (this.d.compareTo(value.getD()) < 0)
        {
            this.d = value.getD();
        }
    }

    @Override
    public boolean lessEqualThan(edu.buaa.act.temporal.impl.index.rtree.Tuple4<A, B, C, D> t, int dimIndex)
    {
        switch (dimIndex)
        {
            case 0:
                return this.a.compareTo(t.getA()) <= 0;
            case 1:
                return this.b.compareTo(t.getB()) <= 0;
            case 2:
                return this.c.compareTo(t.getC()) <= 0;
            default:
                return this.d.compareTo(t.getD()) <= 0;
        }
    }


}
