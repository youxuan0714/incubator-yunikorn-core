package monitor

import (
	"os"
	"time"
)

const (
	bias                         = "A"
	timestampLetterOfUitlization = "B"
	TimeStampLetter              = "A"
	migsheet                     = "mig"

	utilizationfiltpath = "/tmp/utiliztion.xlsx"
	tenantsfiltpath     = "/tmp/tenants.xlsx"
	fairness            = "tenants"
	appNum              = 16
)

var excelCol []string = []string{"B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}
var excelColForUtilization []string = []string{"C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}

func SubTimeAndTranslateToUint64(current, base time.Time) uint64 {
	return uint64(current.Sub(base).Seconds())
}

func SubTimeAndTranslateToMiliSecondUint64(current, base time.Time) uint64 {
	return uint64(current.Sub(base).Milliseconds())
}

func DeleteExistedFile(filePath string) {
	if _, err := os.Stat(filePath); err == nil {
		_ = os.Remove(filePath)
	}
}
