package com.alipay.oceanbase.ibatis;

import java.util.Date;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alipay.oceanbase.ibatis.dao.ExampleDAOImpl;
import com.alipay.oceanbase.ibatis.entity.Example;

/**
 * 
 * 
 * @author liangjieli
 * @version $Id: ObIbatisExample.java, v 0.1 Mar 7, 2013 11:19:41 AM liangjieli Exp $
 */
public class ObIbatisExample {

    public static void main(String[] args) {
        ApplicationContext context = new ClassPathXmlApplicationContext(
            "classpath:applicationContext.xml");
        ExampleDAOImpl exampleDAO = (ExampleDAOImpl) context.getBean("exampleDAOImpl");

        Example example = new Example();
        example.setC1(1);
        example.setC2(1);
        example.setC3("c3");
        example.setC4(new Date());
        example.setC5(3.14F);

        exampleDAO.insert(example);

        System.out.println("insert success");

        Example _example = exampleDAO.findById(example.getC1());
        if (null == _example){
          System.err.println("get _example failed");
          System.exit(1);
        }

        exampleDAO.delete(_example);
        Example __example = exampleDAO.findById(example.getC1());
        if (null != __example){
          System.err.println("__example is not null");
          System.exit(1);
        }

        System.out.println("delete succes");

        System.exit(0);
    }
}
