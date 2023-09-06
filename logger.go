package EasyGoQ

import "fmt"

type ILogger interface {
	Tracef(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Panicf(format string, args ...interface{})
}

type defaultLogger struct {
}

func NewDefaultLogger() ILogger {
	return &defaultLogger{}
}
func (d *defaultLogger) WriteLog(level string, format string, args ...interface{}) {
	fmt.Printf("["+level+"] "+KeyWordLogging+" "+format, args)
}
func (d *defaultLogger) Debugf(format string, args ...interface{}) {
	d.WriteLog("DEBUG", format, args)
}

func (d *defaultLogger) Infof(format string, args ...interface{}) {
	d.WriteLog("INFO", format, args)
}

func (d *defaultLogger) Warnf(format string, args ...interface{}) {
	d.WriteLog("WARN", format, args)
}

func (d *defaultLogger) Warningf(format string, args ...interface{}) {
	d.WriteLog("WARN", format, args)
}

func (d *defaultLogger) Errorf(format string, args ...interface{}) {
	d.WriteLog("ERROR", format, args)
}

func (d *defaultLogger) Fatalf(format string, args ...interface{}) {
	d.WriteLog("FATAL", format, args)
}

func (d *defaultLogger) Panicf(format string, args ...interface{}) {
	d.WriteLog("PANIC", format, args)
}

func (d *defaultLogger) Tracef(format string, args ...interface{}) {
	d.WriteLog("TRACE", format, args)
}
