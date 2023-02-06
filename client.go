package influxdb_client

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"
)

type ContentEncoding string

const (
	DefaultEncoding ContentEncoding = ""
	GzipEncoding    ContentEncoding = "gzip"
)

type HTTPConfig struct {
	// Addr should be of the form "http://host:port"
	// or "http://[ipv6-host%zone]:port".
	Addr string

	// Username is the influxdb username, optional.
	Username string

	// Password is the influxdb password, optional.
	Password string

	// UserAgent is the http User Agent, defaults to "InfluxDBClient".
	UserAgent string

	// Timeout for influxdb writes, defaults to no timeout.
	Timeout time.Duration

	// InsecureSkipVerify gets passed to the http client, if true, it will
	// skip https certificate verification. Defaults to false.
	InsecureSkipVerify bool

	// TLSConfig allows the user to set their own TLS config for the HTTP
	// Client. If set, this option overrides InsecureSkipVerify.
	TLSConfig *tls.Config

	// Proxy configures the Proxy function on the HTTP client.
	Proxy func(req *http.Request) (*url.URL, error)

	// WriteEncoding specifies the encoding of write request
	WriteEncoding ContentEncoding
}

type InfluxdbClient interface {
	// Ping checks that status of cluster, and will always return 0 time and no
	// error for UDP clients.
	Ping(timeout time.Duration) (time.Duration, string, error)

	// Write takes a BatchPoints object and writes all Points to InfluxDB.
	Write(point *Point) error

	// Write takes a BatchPoints object and writes all Points to InfluxDB.
	BatchWrite(batchPoint *BatchPoint) error
}

type influxdbClient struct {
	HTTPConfig

	bodyBuf *bytes.Buffer
	urlBuf  *bytes.Buffer

	basicHeader string
}

func NewInfluxdbClient(conf HTTPConfig) InfluxdbClient {
	c := &influxdbClient{
		HTTPConfig: conf,
		bodyBuf:    &bytes.Buffer{},
		urlBuf:     &bytes.Buffer{},
	}

	if conf.Username != "" {
		c.urlBuf.Reset()
		c.urlBuf.WriteString(conf.Username)
		c.urlBuf.WriteByte(':')
		c.urlBuf.WriteString(conf.Password)

		c.basicHeader = base64.StdEncoding.EncodeToString(c.urlBuf.Bytes())
	}

	return c
}

func (i *influxdbClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	return 0, "", nil
}

func (i *influxdbClient) Write(point *Point) error {
	b := i.bodyBuf
	b.Reset()

	u := i.Addr
	urlBuf := i.urlBuf
	urlBuf.Reset()

	urlBuf.WriteString(u)
	urlBuf.WriteString("/write?db=")
	urlBuf.WriteString(point.GetDatabase())
	urlBuf.WriteString("&rp=")
	urlBuf.WriteString(point.GetRetentionPolicy())
	urlBuf.WriteString("&precision=")
	urlBuf.WriteString(point.GetPrecision())
	urlBuf.WriteString("&consistency=")
	urlBuf.WriteString(point.GetWriteConsistency())

	path := BytesToStr(urlBuf.Bytes())

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethod(http.MethodPost)
	req.SetRequestURI(path)

	b.WriteString(point.measurement)
	b.Write(point.tagBuf.Bytes())
	b.WriteString(" ")
	b.Write(point.fieldBuf.Bytes())
	b.WriteString(" ")
	b.WriteString(strconv.FormatInt(point.timestamp, 10))
	b.WriteString("\n")

	if i.WriteEncoding != DefaultEncoding {
		req.Header.Set("Content-Encoding", string(i.WriteEncoding))
	}

	req.SetBodyRaw(b.Bytes())

	req.Header.Set("Content-Type", "")
	req.Header.Set("Connection", "close")
	req.Header.Set("User-Agent", i.UserAgent)
	if i.Username != "" {
		req.Header.Set("Authorization", "Basic "+i.basicHeader)
	}

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := fasthttp.Do(req, resp); err != nil {
		return err
	}

	if resp.StatusCode() != http.StatusNoContent && resp.StatusCode() != http.StatusOK {
		return fmt.Errorf(BytesToStr(resp.Body()))
	}

	return nil
}

func (i *influxdbClient) BatchWrite(batchPoint *BatchPoint) error {
	u := i.Addr
	urlBuf := i.urlBuf
	urlBuf.Reset()

	urlBuf.WriteString(u)
	urlBuf.WriteString("/write?db=")
	urlBuf.WriteString(batchPoint.GetDatabase())
	urlBuf.WriteString("&rp=")
	urlBuf.WriteString(batchPoint.GetRetentionPolicy())
	urlBuf.WriteString("&precision=")
	urlBuf.WriteString(batchPoint.GetPrecision())
	urlBuf.WriteString("&consistency=")
	urlBuf.WriteString(batchPoint.GetWriteConsistency())

	path := BytesToStr(urlBuf.Bytes())

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.Header.SetMethod(http.MethodPost)
	req.SetRequestURI(path)

	if i.WriteEncoding != DefaultEncoding {
		req.Header.Set("Content-Encoding", string(i.WriteEncoding))
	}

	req.SetBodyRaw(batchPoint.mainBuf.Bytes())

	req.Header.Set("Content-Type", "")
	req.Header.Set("Connection", "close")
	req.Header.Set("User-Agent", i.UserAgent)
	if i.Username != "" {
		req.Header.Set("Authorization", "Basic "+i.basicHeader)
	}

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := fasthttp.Do(req, resp); err != nil {
		return err
	}

	if resp.StatusCode() != http.StatusNoContent && resp.StatusCode() != http.StatusOK {
		return fmt.Errorf(BytesToStr(resp.Body()))
	}

	return nil
}
