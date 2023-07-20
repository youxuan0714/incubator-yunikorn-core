package monitor

import (
	"os"
	"time"
)

const (
	deviationCellName   = "B"
	TimeStampLetter     = "A"
	migsheet            = "mig"
	deviationsheet      = "deviation"
	memorysheet         = "memory"
	cpusheet            = "cpu"
	memfilepath         = "/tmp/mem.xlsx"
	cpufilepath         = "/tmp/cpu.xlsx"
	utilizationfiltpath = "/tmp/utiliztion.xlsx"
	tenantsfiltpath     = "/tmp/tenants.xlsx"
	fairness            = "tenants"
	appNum              = 300
)

var excelCol []string = []string{"B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}
var excelColForUtilization []string = []string{"B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}

func SubTimeAndTranslateToSeoncd(current, base time.Time) uint64 {
	return uint64(current.Sub(base).Seconds())
}

func SubTimeAndTranslateToMiliSecond(current, base time.Time) uint64 {
	return uint64(current.Sub(base).Milliseconds())
}

func DeleteExistedFile(filePath string) {
	if _, err := os.Stat(filePath); err == nil {
		_ = os.Remove(filePath)
	}
}
