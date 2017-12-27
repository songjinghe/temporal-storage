package edu.buaa.act.temporal.impl;

/**
 * Created by song on 17-12-19.
 */
public enum TemporalPropertyType
{
    INVALID(-1), INT(0), LONG(1), BYTE(2), SHORT(3), FLOAT(4), DOUBLE(5), STRING(6);

    private int id=-1;

    TemporalPropertyType(int id){
        this.id = id;
    }

    int getId(){
        return this.id;
    }

    public static TemporalPropertyType getById(int i)
    {
        switch(i)
        {
            case -1: return INVALID;
            case 0: return INT;
            case 1: return LONG;
            case 2: return BYTE;
            case 3: return SHORT;
            case 4: return FLOAT;
            case 5: return DOUBLE;
            case 6: return STRING;
            default:
                throw new RuntimeException("No such id!");
        }
    }
}
