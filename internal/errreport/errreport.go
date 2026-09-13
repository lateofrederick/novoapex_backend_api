// Package errreport carries exceptions raised while serving a request to
// error tracking, the way Nest's global SentryFilter saw every exception a
// guard, pipe or handler threw. It is a leaf package so the auth guard and the
// validation pipe can report without depending on the HTTP kernel.
package errreport

import "net/http"

// Reporter is implemented by response writers that forward a request's
// exceptions to error tracking (observability's Sentry middleware).
type Reporter interface {
	ReportException(err error)
}

// Report hands err to the first Reporter in w's wrapper chain. Without error
// tracking there is none, and Report does nothing.
func Report(w http.ResponseWriter, err error) {
	for w != nil {
		if rep, ok := w.(Reporter); ok {
			rep.ReportException(err)
			return
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return
		}
		w = u.Unwrap()
	}
}

// Exception is an HTTP exception reported under a Nest exception class name
// (UnauthorizedException, ZodValidationException, ...), so issues group and
// read the way they did.
type Exception struct {
	Name    string
	Status  int
	Message string
}

func (e *Exception) Error() string { return e.Message }

func (e *Exception) StatusCode() int { return e.Status }

func (e *Exception) ExceptionName() string { return e.Name }
