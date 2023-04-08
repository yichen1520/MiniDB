package com.WangTeng.MiniDB.meta.factory;

import com.WangTeng.MiniDB.meta.Table;

public class RelFactory {

    private static RelFactory relFactory;

    static {
        relFactory = new RelFactory();
    }

    public static RelFactory getInstance(){
        return relFactory;
    }

    public Table newRelation(String name){
        Table table = new Table();
        return table;
    }
}
