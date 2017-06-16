package com.daxin.stream.cep;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * Created by Daxin on 2017/6/16.
 */
public class GenTime {

    public static void main(String[] args) {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd=HH:mm:ss:SSS");

        long current = System.currentTimeMillis();

        String[] words ={"Hello","World","Spark","Hadoop"};

        for(int i =1 ;i<=18;i++){


            System.out.println(format.format(new Date(current+i*10000*3))+" "+ UUID.randomUUID().toString());
        }



    }
}
