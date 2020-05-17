package wrapers

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	//
	_ "github.com/go-sql-driver/mysql"
)

// MySQL is wraper for sql.DB
type MySQL struct {
	*sql.DB
	logger *MyLogger
}

// DBConnect - open connection to database
func DBConnect(logger *MyLogger, dbHost, dbUser, dbPass, dbName, dsnParams string, dbMaxIdle, dbMaxOpen int) (MySQL, error) {

	mysql, err := sql.Open(
		"mysql",
		fmt.Sprintf("%s:%s@%s/%s%s", dbUser, dbPass, dbHost, dbName, dsnParams),
	)

	if err != nil {
		return MySQL{nil, nil}, fmt.Errorf("failed connect to DB, err: %s", err)
	}

	mysql.SetMaxIdleConns(dbMaxIdle)
	mysql.SetMaxOpenConns(dbMaxOpen)
	mysql.SetConnMaxLifetime(time.Minute)

	logger.Printf(INFO, "Connected to MySQL[%s]: MaxIdleConns: %d, MaxOpenConns: %d", dbName, dbMaxIdle, dbMaxOpen)

	return MySQL{mysql, logger}, nil
}

// DBDisconnect - close connection to database
func (mysql MySQL) DBDisconnect() {
	mysql.Close()
}

// DBReady -- check if dbConnection is alive -- not work yet
func (mysql MySQL) DBReady() bool {
	if err := mysql.Ping(); err == nil {
		mysql.Close()
		return false
	}
	return true
}

// DBQuery - do single query to database
func (mysql MySQL) DBQuery(query string, args ...interface{}) (affectedRows int64) {

	result, err := mysql.Exec(query, args...)

	if err != nil {
		mysql.logger.Printf(ERROR, "Query: \"%s\" %v - FAILED %s", query, args, err)
	} else {
		if affectedRows, err = result.RowsAffected(); err != nil {
			mysql.logger.Printf(ERROR, "Query: \"%s\" %v - FAILED %s", query, args, err)
		} else {
			mysql.logger.Printf(MYSQL, "Query: \"%s\" %v - SUCCESS, affected %d rows", query, args, affectedRows)
		}
	}
	return
}

// DBSelectRow - select list from database
func (mysql MySQL) DBSelectRow(query string, args ...interface{}) (result map[string]string) {
	row, err := mysql.Query(query, args...)

	if err != nil {
		mysql.logger.Printf(ERROR, "Query: \"%s\" %v - FAILED %s", query, args, err)
		return
	}

	defer row.Close()

	result = make(map[string]string)
	columns, _ := row.Columns()

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for row.Next() {

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := row.Scan(valuePtrs...); err != nil {
			mysql.logger.Printf(ERROR, "Query: \"%s\" %v - FAILED %s", query, args, err)
			return
		}

		for i, colName := range columns {
			// val := valuePtrs[i].(*interface{})
			// result[colName] = *val
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			result[colName] = fmt.Sprintf("%s", v)
		}

		break
	}

	mysql.logger.Printf(MYSQL, "Query: \"%s\" %v - SUCCESS", query, args)

	return
}

// DBSelectList - select list from database
func (mysql MySQL) DBSelectList(query string, args ...interface{}) (result []map[string]string) {
	rows, err := mysql.Query(query, args...)

	if err != nil {
		mysql.logger.Printf(ERROR, "Query: \"%s\" %v - FAILED %s", query, args, err)
		return
	}

	defer rows.Close()

	columns, _ := rows.Columns()

	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			mysql.logger.Printf(ERROR, "Query: \"%s\" %v - FAILED %s", query, args, err)
			return []map[string]string{}
		}

		row := make(map[string]string)

		for i, colName := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			row[colName] = fmt.Sprintf("%s", v)
		}

		result = append(result, row)
	}

	mysql.logger.Printf(MYSQL, "Query: \"%s\" %v - SUCCESS, fetched %d rows", query, args, len(result))

	return
}

// GenParams - generate given count of params
func (mysql MySQL) GenParams(count int) (params string) {
	for i := 0; i < count; i++ {
		params += "?, "
	}
	return strings.TrimRight(params, ", ")
}

// InitQueryQueue - create thread for queue
func (mysql MySQL) InitQueryQueue() (queryChannel chan string) {

	// wg.Add(1)
	queryChannel = make(chan string, 1)
	go mysql.queryQueue(queryChannel)

	return
}

func (mysql MySQL) queryQueue(queryChannel chan string) {

	funcName := "queryQueue"
	start := time.Now().Unix()
	mysql.logger.Printf(FUNC, "Start: %s", funcName)

	for query := range queryChannel {
		mysql.DBQuery(query)
	}

	mysql.logger.Printf(FUNC, "Stop: %s, diration: %d sec", funcName, time.Now().Unix()-start)
}

// // DBSelectRow - select list from database
// func (mysql MySQL) DBSelectRow(query string, args ...interface{}) (result map[string]string) {
//      row, err := mysql.Query(query, args...)

//      if err != nil {
//              mysql.logger.Printf(MYSQL, "Query: \"%s\" %v - FAILED %s", query, args, err)
//              return
//      }
//      columns, _ := row.Columns()
//      result = map[string]string{}

//      count := len(columns)
//      values := make([]interface{}, count)
//      valuePtrs := make([]interface{}, count)

//      for row.Next() {

//              for i := range columns {
//                      valuePtrs[i] = &values[i]
//              }

//              row.Scan(valuePtrs...)

//              for i, col := range columns {
//                      var v interface{}
//                      val := values[i]
//                      b, ok := val.([]byte)
//                      if ok {
//                              v = string(b)
//                      } else {
//                              v = val
//                      }
//                      result[col] = fmt.Sprintf("%s", v)
//              }

//              break
//      }

//      mysql.logger.Printf(MYSQL, "Query: \"%s\" %v - SUCCESS", query, args)

//      return
// }

// // DBSelectList - select list from database
// func (mysql MySQL) DBSelectList(query string, args ...interface{}) (result []map[string]string) {
//      rows, err := mysql.Query(query, args...)

//      if err != nil {
//              mysql.logger.Printf(MYSQL, "Query: \"%s\" %v - FAILED %s", query, args, err)
//              return
//      }
//      columns, _ := rows.Columns()

//      count := len(columns)
//      values := make([]interface{}, count)
//      valuePtrs := make([]interface{}, count)

//      for rows.Next() {
//              for i := range columns {
//                      valuePtrs[i] = &values[i]
//              }
//              rows.Scan(valuePtrs...)
//              row := map[string]string{}

//              for i, col := range columns {
//                      var v interface{}
//                      val := values[i]
//                      b, ok := val.([]byte)
//                      if ok {
//                              v = string(b)
//                      } else {
//                              v = val
//                      }
//                      row[col] = fmt.Sprintf("%s", v)
//              }
//              result = append(result, row)
//      }

//      mysql.logger.Printf(MYSQL, "Query: \"%s\" %v - SUCCESS, fetched %d rows", query, args, len(result))

//      return
// }
