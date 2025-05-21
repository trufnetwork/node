package benchmark

import (
	"fmt"
	"log" // Using standard log for general messages if t is nil
	"testing"
	"time"
)

// LogPhaseEnter logs the entry into a specific phase or function.
// If t is nil, it uses the standard log package.
func LogPhaseEnter(t *testing.T, phaseName string, detailsFormat string, args ...interface{}) {
	detailMsg := ""
	if detailsFormat != "" {
		detailMsg = fmt.Sprintf(detailsFormat, args...)
	}

	logMessage := fmt.Sprintf("[BENCHMARK_TRACE] Entering phase: %s", phaseName)
	if detailMsg != "" {
		logMessage = fmt.Sprintf("%s. Details: %s", logMessage, detailMsg)
	}

	if t != nil {
		t.Log(logMessage)
	} else {
		log.Println(logMessage) // Fallback to standard logger
	}
}

// LogPhaseExit logs the exit from a specific phase or function, including duration.
// If t is nil, it uses the standard log package.
func LogPhaseExit(t *testing.T, start time.Time, phaseName string, detailsFormat string, args ...interface{}) {
	duration := time.Since(start)
	detailMsg := ""
	if detailsFormat != "" {
		detailMsg = fmt.Sprintf(detailsFormat, args...)
	}

	logMessage := fmt.Sprintf("[BENCHMARK_TRACE] Exiting phase: %s. Duration: %v", phaseName, duration)
	if detailMsg != "" {
		logMessage = fmt.Sprintf("%s. Details: %s", logMessage, detailMsg)
	}

	if t != nil {
		t.Log(logMessage)
	} else {
		log.Println(logMessage) // Fallback to standard logger
	}
}

// LogInfo provides a general informational log line, using t.Log if available, otherwise standard log.
func LogInfo(t *testing.T, format string, args ...interface{}) {
	logMessage := fmt.Sprintf("[BENCHMARK_INFO] %s", fmt.Sprintf(format, args...))
	if t != nil {
		t.Log(logMessage)
	} else {
		log.Println(logMessage)
	}
}
