package com.alipay.oceanbase.ibatis.dao;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.orm.ibatis.support.SqlMapClientDaoSupport;

import com.alipay.oceanbase.ibatis.entity.Example;

/**
 * 
 * 
 * @author liangjieli
 * @version $Id: ExampleDAO.java, v 0.1 Mar 7, 2013 11:15:24 AM liangjieli Exp $
 */
public class ExampleDAOImpl extends SqlMapClientDaoSupport {

    private static final Logger logger = Logger.getLogger(ExampleDAOImpl.class);

    public Example insert(Example t) {
        this.getSqlMapClientTemplate().insert("example_insert", t);
        return t;
    }

    public int delete(Example t) {
        if (t == null) {
            logger.warn("delete null error!");
            return 0;
        }

        return this.deleteById(t.getC1());
    }

    public int deleteById(int id) {
        if (id <= 0) {
            logger.warn("id must greater than 0");
            return 0;
        }
        return this.getSqlMapClientTemplate().delete("example_delete", id);
    }

    public int update(Example t) {
        if (t == null) {
            logger.warn("update null error!");
            return 0;
        }
        return this.getSqlMapClientTemplate().update("example_update", t);
    }

    public Example findById(int id) {
        if (id <= 0) {
            logger.warn("id must greater than 0");
        }
        return (Example) this.getSqlMapClientTemplate().queryForObject("example_select_byid", id);
    }

    @SuppressWarnings("unchecked")
    public List<Example> findAll() {
        return this.getSqlMapClientTemplate().queryForList("example_select_bycondition");
    }

}
