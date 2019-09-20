package errors

import "net/http"

//go:generate stringer -type=Module,Severity

type Module uint8
type Severity uint8
type Kind int
type Op string

const (
	ModuleInternal Module = iota
	ModuleCluster
	ModuleStorage
	ModuleBroker
	ModuleProtocol
	ModuleDriver

	DefaultModule = ModuleInternal

	//Severities are taken from the Logrus package for compatiblity
	// PanicLevel level, highest level of severity. Logs and then calls panic with the
	// message passed to Debug, Info, ...
	SeverityPanic Severity = iota
	// FatalLevel level. Logs and then calls `logger.Exit(1)`. It will exit even if the
	// logging level is set to Panic.
	SeverityFatal
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	SeverityError
	// WarnLevel level. Non-critical entries that deserve eyes.
	SeverityWarn
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	SeverityInfo
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	SeverityDebug
	// TraceLevel level. Designates finer-grained informational events than the Debug.
	SeverityTrace

	DefaultSeverity = SeverityError

	KindNotFound = http.StatusNotFound
)

type DejaError struct {
	Severity Severity
	Message  string
	//The actor in which occurred
	Module Module
	//Operation, usually the function name in which occurred
	Operation  Op
	Kind       Kind
	Details    map[string]string
	WrappedErr error
}

func (d DejaError) Error() string {
	return d.Message
}

func (o Op) String() string {
	return string(o)
}

func DeconstructStackTrace(err error) []string {
	if err == nil {
		return nil
	}
	var result []string
	for e, ok := err.(DejaError); ok; {
		result = append(result, e.Module.String()+"."+e.Operation.String())

		err = e.WrappedErr
		e, ok = err.(DejaError)
	}

	return result
}
