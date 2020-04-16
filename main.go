package main

import (
	"fmt"
	"log"
	"math/big"
	"os"
	"os/exec"
	"snmp-port-loading/gosnmp"
	h "snmp-port-loading/wrapers"
	"sync"
	"time"
)

const (
	logPath   = "my-log.log"
	logDetail = h.ERROR + h.INFO + h.DEBUG + h.FUNC + h.MYSQL
	// DB
	dbHost  = "tcp(127.0.0.1:3306)"
	dbUser  = "root"
	dbPass  = "password"
	dbTable = "database"
	dbDSN   = "" // "?allowOldPasswords=true"

	//SNMP
	snmpDefaultRO = "load"
	snmpVersion   = "v2c"
	snmpRetries   = 3
	snmpTimeout   = 1
	snmpMaxOids   = 20
	snmpPeriod    = 300

	// OIds
	oidSysName   = "1.3.6.1.2.1.1.5.0"
	oidInOctets  = ".1.3.6.1.2.1.31.1.1.1.6."
	oidOutOctets = ".1.3.6.1.2.1.31.1.1.1.10."

	// SQL Query
	sqlSourceList = "" +
		"SELECT s.name AS name, i.snmpid AS snmpid FROM source s, interface i " +
		"WHERE s.routerip=INET_ATON('%s') AND i.routerip=INET_ATON('%s') " +
		"AND s.ifname=i.ifname AND s.ref='ifname'" +
		" UNION " +
		"SELECT s.name AS name, i.snmpid AS snmpid FROM source s, interface i " +
		"WHERE s.routerip=INET_ATON('%s') AND i.routerip=INET_ATON('%s') " +
		"AND s.ifip=i.ifip AND s.ref='ifip'"
	sqlDevicesList = "" +
		"SELECT INET_NTOA(s.routerip) AS routerip, IFNULL(d.snmp_ro, '%s') AS snmp_ro " +
		"FROM source s LEFT JOIN device d ON s.routerip = d.ip GROUP BY s.routerip"
	sqlUpdateSource1 = "" +
		"UPDATE source SET input_p=input, output_p=output, input='%d', output='%d', changed_mod_p=changed_mod, " +
		"changed_mod=FROM_UNIXTIME(%d*FLOOR(UNIX_TIMESTAMP(NOW())/%d)), " +
		"contract_remain=IF((MONTH(NOW())<>MONTH(DATE(changed_mod_p))),DAY(LAST_DAY(NOW()))*3600*24*0.05, " +
		"IF(IF((%d-input_p)>(%d-output_p),(%d-input_p),(%d-output_p))/(%d)*8>contract_speed,contract_remain-%d,contract_remain)), " +
		"changed=NOW(), flag='1' WHERE name='%s' LIMIT 1"
	sqlUpdateSource2 = "" +
		"UPDATE source SET " +
		"input_day=input_day+IF(input>input_p,IF(input_p>0,input-input_p,0),0), " +
		"output_day=output_day+IF(output>output_p,IF(output_p>0,output-output_p,0),0), " +
		"input_period=input_period+IF(input>input_p,IF(input_p>0,input-input_p,0),0), " +
		"output_period=output_period+IF(output>output_p,IF(output_p>0,output-output_p,0),0), " +
		"changed=NOW(), flag='0' WHERE name='%s'"
	sqlUpdateGroup = "" +
		"UPDATE source_gr_contract AS t2 INNER JOIN " +
		" (SELECT g.id,if(sum((input - input_p)/(300)*8)>sum((output - output_p" +
		" )/(300)*8) ,sum((input - input_p)/(300)*8) ,sum((output - output_p)/(300)*8)) AS sm_sp " +
		" FROM source AS s INNER JOIN source_gr_contract AS g ON s.group_id = g.id " +
		" WHERE g.id >0 GROUP BY g.id) t1 ON t1.id = t2.id SET t2.last_speed = sm_sp," +
		"t2.contract_remain = if(month(last_upd) <> month(Now()) ," +
		"(DAY(LAST_DAY(NOW()))*3600*24*0.05 )," +
		" if( contract_speed > sm_sp, contract_remain, contract_remain -300 )),t2.last_upd = Now()"
)

// TODO think about singleton and app-container
var l h.MyLogger
var mysql h.MySQL

func main() {

	//
	// Init logs
	//

	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	l = h.InitLog(f, logDetail)

	//
	// Init MySQL connection
	//

	mysql, err = h.DBConnect(&l, dbHost, dbUser, dbPass, dbTable, dbDSN)
	if err != nil {
		panic(err)
	}
	mysql.SetMaxIdleConns(100)

	//
	// Start
	//

	funcName := fmt.Sprintf("main")
	start := time.Now().Unix()
	l.Printf(h.FUNC, "Start: %s - %d", funcName, start)

	var wg sync.WaitGroup
	queryChannel := make(chan string, 1)
	go queryQueue(&wg, queryChannel)

	//
	// Business logic
	//

	list := mysql.DBSelectList(fmt.Sprintf(sqlDevicesList, snmpDefaultRO))

	for _, row := range list {
		l.Printf(h.DEBUG, "Process host: %s, community: %s", row["routerip"], row["snmp_ro"])

		wg.Add(1)

		go func(wg *sync.WaitGroup, queryChannel chan string, ip, snmpRO string) {
			defer wg.Done()

			// create instance of snmp-connection
			snmpInst := getSnmpCon(ip, snmpRO)
			if err := snmpInst.Connect(); err != nil {
				l.Printf(h.INFO, "Host %s got connect error: %v", ip, err)
				return
			}
			defer snmpInst.Conn.Close()

			// fetch required source indexes
			sourceList := mysql.DBSelectList(fmt.Sprintf(sqlSourceList, ip, ip, ip, ip))

			// generate slice of oids
			oids := make([]string, 0, len(sourceList)*2)
			for _, source := range sourceList {
				oids = append(oids,
					fmt.Sprintf("%s%s", oidInOctets, source["snmpid"]),
					fmt.Sprintf("%s%s", oidOutOctets, source["snmpid"]),
				)
			}

			// this is war
			from, to := 0, 0
			oidValue := make(map[string]*big.Int)

			for from = 0; from < len(oids); from += snmpMaxOids {

				if to = from + snmpMaxOids; to > len(oids) {
					to = len(oids)
				}

				result, err := snmpInst.Get(oids[from:to])
				if err != nil {
					l.Printf(h.ERROR, "Host %s do not responce on GET request, error: %v", ip, err)
					// continue
					return
				}

				for _, pdu := range result.Variables {
					switch pdu.Type {
					case gosnmp.OctetString:
						l.Printf(h.ERROR, "Accep string responce, but assumed int, %s:%s", ip, pdu.Name)
					default:
						oidValue[pdu.Name] = gosnmp.ToBigInt(pdu.Value)
					}
				}
			}

			// update iface counters
			for _, source := range sourceList {
				inOctets := oidValue[fmt.Sprintf("%s%s", oidInOctets, source["snmpid"])]
				outOctets := oidValue[fmt.Sprintf("%s%s", oidOutOctets, source["snmpid"])]

				queryChannel <- fmt.Sprintf(sqlUpdateSource1, inOctets, outOctets, snmpPeriod, snmpPeriod,
					inOctets, outOctets, inOctets, outOctets, snmpPeriod, snmpPeriod, source["name"])

				queryChannel <- fmt.Sprintf(sqlUpdateSource2, source["name"])
			}

		}(&wg, queryChannel, row["routerip"], row["snmp_ro"])

	}

	wg.Wait()

	//
	// Legacy
	//

	mysql.DBQuery(sqlUpdateGroup)

	// TODO we need rewrite this scripts
	l.Printf(h.INFO, "Run: %s", "/var/www/html/snmp/bin/update_db_rrd.pl")
	exec.Command("/var/www/html/snmp/bin/update_db_rrd.pl").Run()

	l.Printf(h.INFO, "Run: %s", "/home/diriger/snmp/update_period.pl")
	exec.Command("/home/diriger/snmp/update_period.pl").Run()

	l.Printf(h.FUNC, "Stop: %s - %d, diration: %d", funcName, time.Now().Unix(), time.Now().Unix()-start)
}

func queryQueue(wg *sync.WaitGroup, queryChannel chan string) {
	defer wg.Done()
	for {
		select {
		case query := <-queryChannel:
			mysql.DBQuery(query)
		}
	}
	// runtime.Gosched()
}

func getSnmpCon(ip, snmpRO string) *gosnmp.GoSNMP {
	return &gosnmp.GoSNMP{
		Target:             ip,
		Port:               161,
		Community:          snmpRO,
		MaxOids:            snmpMaxOids,
		Retries:            snmpRetries,
		Version:            gosnmp.Version2c,
		Timeout:            time.Duration(snmpTimeout * time.Second),
		ExponentialTimeout: true,
	}
}
