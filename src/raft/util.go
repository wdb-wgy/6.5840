package raft

// import "log"

// Debugging
// const Debug = false

// func DPrintf(format string, a ...interface{}) {
// 	if Debug {
// 		log.Printf(format, a...)
// 	}
// }

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string
const (
	// dClient  logTopic = "CLNT" 
	// dCommit  logTopic = "CMIT"
	// dDrop    logTopic = "DROP"
	// dError   logTopic = "ERRO"
	// dInfo    logTopic = "INFO"
	// dLeader  logTopic = "LEAD"
	// dLog     logTopic = "LOG1"
	// dLog2    logTopic = "LOG2"
	// dPersist logTopic = "PERS"
	// dSnap    logTopic = "SNAP"
	// dTerm    logTopic = "TERM"
	// dTest    logTopic = "TEST"
	// dTimer   logTopic = "TIMR"
	// dTrace   logTopic = "TRCE"
	dLeader  logTopic = "LEAD"
	dEntry   logTopic = "ENTR" // 发送日志
	dLog     logTopic = "LOGS" // 本机日志
	dWarn    logTopic = "WARN"
	dVote    logTopic = "VOTE" // 投票使用
	dCommand logTopic = "ADDC" // 添加命令
	dHeart   logTopic = "HEAR" // 发送心跳
	rHeart   logTopic = "HREP" // 心跳返回
	dapply   logTopic = "APPL" // 应用日志
	dCommit  logTopic = "CMIT" // 提交命令
)

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}


func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}