package wrapers

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// MyLogger TODO
type MyLogger struct {
	*log.Logger
	mu      *sync.Mutex
	verbose int
}

// FATAL	fatal exeption message
// ERROR	critical message
// INFO		non critical message
// MYSQL 	MySQL query message
// FUNC		stop/start function message
// DEBUG	debug message
const (
	FATAL int = 1 << iota
	ERROR
	INFO
	MYSQL
	FUNC
	DEBUG
)

var strErrorVerbose = map[int]string{
	FATAL: `FATAL`,
	ERROR: `ERROR`,
	INFO:  `INFO`,
	MYSQL: `MYSQL`,
	FUNC:  `FUNC`,
	DEBUG: `DEBUG`,
}

// InitLog create MyLog example - retrun MyLogger
func InitLog(file *os.File, verbose int) (l MyLogger) {
	l = MyLogger{&log.Logger{}, &sync.Mutex{}, verbose}
	l.SetOutput(file)

	return
}

// Printf override log.Printf -	print string message with date and verbose level
func (l MyLogger) Printf(level int, str string, v ...interface{}) {
	if level&l.verbose == level {
		l.mu.Lock()
		l.Logger.Printf("%s [%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), strErrorVerbose[level], fmt.Sprintf(str, v...))
		l.mu.Unlock()

		fmt.Printf("%s [%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), strErrorVerbose[level], fmt.Sprintf(str, v...))
	}
}
