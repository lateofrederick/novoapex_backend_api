package queue

import (
	"crypto/tls"
)

func TLSServerConfig(host string) *tls.Config {
	return tlsConfigFor(host)
}
