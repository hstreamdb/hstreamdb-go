package security

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTLSConfig(t *testing.T) {
	certFile := filepath.Join(filepath.Dir(t.TempDir()), "cert.pem")
	defer os.Remove(certFile)
	keyFile := filepath.Join(filepath.Dir(t.TempDir()), "key.pem")
	defer os.Remove(keyFile)

	assert.Nil(t, os.WriteFile(certFile, []byte(cert), 0666))
	assert.Nil(t, os.WriteFile(keyFile, []byte(key), 0666))

	security := TLSAuth{
		ClusterSSLCA:   certFile,
		ClusterSSLCert: certFile,
		ClusterSSLKey:  keyFile,
	}

	tlsConfig, err := security.ToTLSConfig()
	assert.Nil(t, err)
	assert.NotNil(t, tlsConfig)
}

var cert = `-----BEGIN CERTIFICATE-----
MIIDdDCCAlygAwIBAgIUVzaBSst4PFkEdp+mmjkCK57EuBMwDQYJKoZIhvcNAQEL
BQAwZzELMAkGA1UEBhMCU08xDjAMBgNVBAgMBUVhcnRoMREwDwYDVQQHDAhNb3Vu
dGFpbjESMBAGA1UECgwJSFN0cmVhbURCMQ0wCwYDVQQLDARFTVFYMRIwEAYDVQQD
DAlsb2NhbGhvc3QwHhcNMjIxMDI0MDc0NzA1WhcNMzIxMDIxMDc0NzA1WjBnMQsw
CQYDVQQGEwJTTzEOMAwGA1UECAwFRWFydGgxETAPBgNVBAcMCE1vdW50YWluMRIw
EAYDVQQKDAlIU3RyZWFtREIxDTALBgNVBAsMBEVNUVgxEjAQBgNVBAMMCWxvY2Fs
aG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALsNPcOjOhNuSw3m
zrdcPFAwa403YgLpJSTBOWeT6CoNYUfukeXmRh9VTieoAgfxR7nFmkw3wJdufj86
U6v5CRr1CoUKTLsYKR5Uym5P43rGHyOyCWjw6y+FIAnzV3kc3VghOgmAxe8OZEz0
86ZxXbyYD92DIzfUHvUL6n6lzd+NKg54wlEYRli7JiqeXzqieImeIUceK3LHfZa7
QQOVnsu1E67MgFJNVkMTryu8sqZYGj2gJSPEIkj+uHpbnJ3BOp0QkPhR/Iyk8GNK
UXbRdIoGOkSj/QeFqjPAAIjY6nCyy4n3Ul61j4dIBgDCwHdY0FU8MMxNlgh/AODt
TJibB0sCAwEAAaMYMBYwFAYDVR0RBA0wC4IJbG9jYWxob3N0MA0GCSqGSIb3DQEB
CwUAA4IBAQB7H9yqUoopZLvlfHow61QqpXmOZ1hAHHkAqUvbgbvSaIRZxHtmpy2Z
r348nnbSrqzg/tiZLzLcjA4UtTFbXy+EBbcYgkUfEbNvaRQkcnbS10yJW2IiOCjQ
om5oGROur2eaOxFJpiZK/fsX2SeBr7QLlXw6luGf8k0cbqgIebj1kr3+uuhzk+jr
1fP5pSHqsDyChkHLz3pcJRJIPx/fF0jz2vQR8y1nF0XGvyB4BZPEwOk7ykTEtSkP
6TBxLTK3djqr1ooUbILGhOlGERZH/dp3KPls+k5/5XJL6wzGviAdrKxIENSL/DRh
47pSkN8L5pB27sCXJkKq8kuywR/vThSi
-----END CERTIFICATE-----
`

var key = `-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEAuw09w6M6E25LDebOt1w8UDBrjTdiAuklJME5Z5PoKg1hR+6R
5eZGH1VOJ6gCB/FHucWaTDfAl25+PzpTq/kJGvUKhQpMuxgpHlTKbk/jesYfI7IJ
aPDrL4UgCfNXeRzdWCE6CYDF7w5kTPTzpnFdvJgP3YMjN9Qe9QvqfqXN340qDnjC
URhGWLsmKp5fOqJ4iZ4hRx4rcsd9lrtBA5Wey7UTrsyAUk1WQxOvK7yyplgaPaAl
I8QiSP64elucncE6nRCQ+FH8jKTwY0pRdtF0igY6RKP9B4WqM8AAiNjqcLLLifdS
XrWPh0gGAMLAd1jQVTwwzE2WCH8A4O1MmJsHSwIDAQABAoIBAQCL/WT0Pf0A/dJ+
HPcok4cwHIzhq0lXFFYQI+xqcG+YyVq9EvduL+DbImTTmRGsEA+2IQVRdYhVzQP/
Hg/w7Pi7jBjLaOH0RCjB4oN9+5wsDorPlDcmqufZMLDBwbT9l3SwuG1PH2kDeKno
TorrTnzw4H0/Mx3wcniYvMpK7pwdaaiLizAkWUrus1SNbSf6b+G42xq/GjQTUEQx
rYqKfLytTV3mWqngXBDYNaQ6lqUSHJ+fCvlngalHF7ux4egael4SRfosVJNZYFyp
+oCDAt+5XPjYSQIh1ZmH1HilYhYF1gU0SIIOZZxlHOw0P+F+W0nEQKU3sIz32Kd6
Ou8Q/EhBAoGBAOmBpFlrn8hozg+/uU0F840rI9LGRBVreh1HXbTniIHfVpiabjnh
7nzXJWi32kCtkcPL3yWQz7I8SpF5Hq+1OU4oeIqbwRjHz4wMaK0zqgJcrCWeClDW
/ETc307U7zc9mAIZIOplBjW8buSjUyrT1lZqg3NAUReqFopvRfjaPqfVAoGBAM0S
AjzONKTPqJelN7fEPN1glQLU67CpCdxlUcrIoONqgBOsurg0oTvGK9c3UyTN947L
Y2xKiR+Kwb/8nZ6i9EZpzMes3g3+b6UsNk7NgzWRihZghpP09CVXo8VmxvEqGzBF
d0wBU+84J+pItD3WTPIXGDn5mtiZa5kLN0qpjKKfAoGBALS4k5xpgXa4NpNVzXJh
YsglVUpyDIuPbSlv7IRwleP+GKVvEFRYGh4g3WEYuiBItYIWLZSDaVwG8ad5WFih
J8Ln67NLLL2jF2zKxdEXJUNCujE7CbzbRs99Ko92mdXlI9qr3B/NJGs+dY0bIukI
nuCu+yGd66UnDN8Dk+Iv90IpAoGASKA6HqLAziWlBZ/DrddX3ucXntsdZYFYNq/D
WdfX8vKCzXT848Pv3iV+3UeAsKYpKG8rlOyIx5zypTWn9MitTkql9YlpUvdrPsn7
qcjq0QvsJaSlxLyZwqfZHk4Z4ssQufXAHDDUMwJmUHjTLJs6wPog4Fdf2ZnsG1V3
jX+iiO0CgYEAwlWdlKORfQAx/BzBlSPFfXDY1mOe4XITNPt+aL++rRp/BBLbdtpI
IEGbO6VjJMON4IKP2Twn6M1TMhXb6dSLkqs7jFz4q1//hChWhr/G2zHhPytO2W5l
ExUrGOJL7jkSpmohlINu8vv7HiMFTobw7IooObUT3zSSm8cz7rc9KeE=
-----END RSA PRIVATE KEY-----
`
