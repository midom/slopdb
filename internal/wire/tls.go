// Package wire contains shared transport configuration for SQL protocol servers.
package wire

import (
	"crypto/tls"
	"errors"
)

// TLSConfig clones the caller configuration and CA pool, applies a TLS 1.2
// minimum, and makes verified client certificates imply required TLS.
func TLSConfig(tlsConfigInput *tls.Config, requireTLS bool) (*tls.Config, bool, error) {
	if tlsConfigInput != nil {
		tlsConfig := tlsConfigInput.Clone()
		if len(tlsConfig.Certificates) == 0 && tlsConfig.GetCertificate == nil {
			return nil, false, errors.New("TLS requires a server certificate")
		}
		if tlsConfig.GetConfigForClient != nil {
			return nil, false, errors.New("TLS does not support GetConfigForClient; use GetCertificate")
		}
		if tlsConfig.MinVersion < tls.VersionTLS12 {
			tlsConfig.MinVersion = tls.VersionTLS12
		}
		if tlsConfig.MaxVersion != 0 && tlsConfig.MaxVersion < tlsConfig.MinVersion {
			return nil, false, errors.New("TLS maximum version is below its minimum version")
		}
		switch tlsConfig.ClientAuth {
		case tls.NoClientCert, tls.RequestClientCert, tls.VerifyClientCertIfGiven:
		case tls.RequireAndVerifyClientCert:
			if tlsConfig.ClientCAs == nil {
				return nil, false, errors.New("client certificate authentication requires an explicit CA pool")
			}
			requireTLS = true
		default:
			return nil, false, errors.New("client certificate authentication must verify the client certificate")
		}
		if tlsConfig.ClientCAs != nil {
			tlsConfig.ClientCAs = tlsConfig.ClientCAs.Clone()
		}
		tlsConfigInput = tlsConfig
	}
	if requireTLS && tlsConfigInput == nil {
		return nil, false, errors.New("RequireTLS requires TLSConfig")
	}
	return tlsConfigInput, requireTLS, nil
}
