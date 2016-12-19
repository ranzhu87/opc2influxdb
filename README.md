opc2influxdb根据配置文件读取opcserver中的数据并存储至influxdb中
配置文件如下：
[opc]  
opcaddrs=127.0.0.1:7766,127.0.0.1:7766
opcnames=Matrikon.OPC.Simulation.1,KEPware.KEPServerEX.V4
opclists=[Random.Int4,  ,],[*.PLC.Tag_*]

[influxdbs] 
influxdbnames=pump,motor
influxdbaddrs=127.0.0.1:8086,127.0.0.1:8086


[timer] 
readtimer=10,20


其中opc配置opc的地址（IP:PORT）名称以及读取的数据名（可以配置为单个数据名如Random.Int4,也可以采用*自动匹配多个数据，opc之间用中括号和逗号隔开，可以配置读取多个数据。

influxdb配置数据库地址（IP:PORT），以及数据库名称，也就是每个opc对应的数据库名称。

timer配置读取和存储周期，单位为秒