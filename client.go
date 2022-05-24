package influxdb_client

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
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
}

type influxdbClient struct {
	HTTPConfig

	buf bytes.Buffer
}

func NewInfluxdbClient(conf HTTPConfig) InfluxdbClient {
	return &influxdbClient{
		HTTPConfig: conf,
		buf:        bytes.Buffer{},
	}
}

func (i *influxdbClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	return 0, "", nil
}

func (i *influxdbClient) Write(point *Point) error {
	b := i.buf
	b.Reset()

	var w io.Writer
	if i.WriteEncoding == GzipEncoding {
		w = gzip.NewWriter(&b)
	} else {
		w = &b
	}

	// gzip writer should be closed to flush data into underlying buffer
	if c, ok := w.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}

	u := i.Addr
	urlBuf := bytes.Buffer{}
	urlBuf.WriteString(u)
	urlBuf.WriteString("/write?db=")
	urlBuf.WriteString(point.GetDatabase())
	urlBuf.WriteString("&rp=")
	urlBuf.WriteString(point.GetRetentionPolicy())
	urlBuf.WriteString("&precision=")
	urlBuf.WriteString(point.GetPrecision())
	urlBuf.WriteString("&consistency=")
	urlBuf.WriteString(point.GetWriteConsistency())

	path := urlBuf.String()

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
	req.Header.Set("User-Agent", i.UserAgent)
	if i.Username != "" {
		basicAuth := base64.StdEncoding.EncodeToString(StrToBytes(i.Username + ":" + i.Password))
		req.Header.Set("Authorization", "Basic "+basicAuth)
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
