package com.alipay.oceanbase.ibatis.entity;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang.builder.EqualsBuilder;

/**
 * 
 * 
 * @author liangjieli
 * @version $Id: Example.java, v 0.1 Mar 7, 2013 11:06:50 AM liangjieli Exp $
 */
public class Example implements Serializable {

    /**  */
    private static final long serialVersionUID = 5806890150997712793L;
    private int               c1;
    private int               c2;
    private String            c3;
    private Date              c4;
    private float             c5;

    public int getC1() {
        return c1;
    }

    public void setC1(int c1) {
        this.c1 = c1;
    }

    public int getC2() {
        return c2;
    }

    public void setC2(int c2) {
        this.c2 = c2;
    }

    public String getC3() {
        return c3;
    }

    public void setC3(String c3) {
        this.c3 = c3;
    }

    public Date getC4() {
        return c4;
    }

    public void setC4(Date c4) {
        this.c4 = c4;
    }

    public float getC5() {
        return c5;
    }

    public void setC5(float c5) {
        this.c5 = c5;
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

}
