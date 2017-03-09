#!/bin/bash

echo $(pwd)

sqlplus /nolog
conn sys/sys as sysdba;
startup;
conn LOGMINER/LOGMINER@orcl as sysdba;

var = 1
while[$var -le 1000]
do
   curId = $($var + 7680)
   INSERT INTO EMP VALUES ($curId,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
   commit;
   var = $(($var+1))

done

