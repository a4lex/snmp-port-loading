# snmp-port-loading
Monitoring port loading of switches and subinterfaces of routers


rrdtool fetch rath_to_rrd  AVERAGE -r 300 -s -1h | awk '{printf("%s,%8.2f,%8.2f \n",strftime("%c",$1),$2,$3) } '
