package wrapers

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// MySQL is wraper for sql.DB
type MySQL struct {
	*sql.DB
	logger *MyLogger
}

// DBConnect - open connection to database
func DBConnect(logger *MyLogger, dbHost, dbUser, dbPass, dbName, dsnParams string) (MySQL, error) {

	mysql, err := sql.Open(
		"mysql",
		fmt.Sprintf("%s:%s@%s/%s%s", dbUser, dbPass, dbHost, dbName, dsnParams),
	)

	if err != nil {
		return MySQL{nil, nil}, fmt.Errorf("failed connect to DB, err: %s", err)
	}

	return MySQL{mysql, logger}, nil
}

// DBDisconnect - close connection to database
func (mysql MySQL) DBDisconnect() {
	mysql.Close()
}

// DBQuery - do single query to database
func (mysql MySQL) DBQuery(query string) (affectedRows int64) {

	result, err := mysql.Exec(query)

	if err != nil {
		mysql.logger.Printf(MYSQL, "Query: \"%s\" - FAILED %s", query, err)
	} else {
		if affectedRows, err = result.RowsAffected(); err != nil {
			mysql.logger.Printf(MYSQL, "Query: \"%s\" - FAILED %s", query, err)
		} else {
			mysql.logger.Printf(MYSQL, "Query: \"%s\" - SUCCESS, affected %d rows", query, affectedRows)
		}
	}
	return
}

// DBSelectRow - select list from database
func (mysql MySQL) DBSelectRow(query string) (result map[string]string) {
	row, err := mysql.Query(query)

	if err != nil {
		mysql.logger.Printf(MYSQL, "Query: \"%s\" - FAILED %s", query, err)
		return
	}
	columns, _ := row.Columns()
	result = map[string]string{}

	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for row.Next() {

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		row.Scan(valuePtrs...)

		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			result[col] = fmt.Sprintf("%s", v)
		}

		break
	}

	mysql.logger.Printf(MYSQL, "Query: \"%s\" - SUCCESS", query)

	return
}

// DBSelectList - select list from database
func (mysql MySQL) DBSelectList(query string) (result []map[string]string) {
	rows, err := mysql.Query(query)

	if err != nil {
		mysql.logger.Printf(MYSQL, "Query: \"%s\" - FAILED %s", query, err)
		return
	}
	columns, _ := rows.Columns()

	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		row := map[string]string{}

		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			row[col] = fmt.Sprintf("%s", v)
		}
		result = append(result, row)
	}

	mysql.logger.Printf(MYSQL, "Query: \"%s\" - SUCCESS, fetched %d rows", query, len(result))

	return
}
