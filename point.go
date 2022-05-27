package influxdb_client

import (
	"bytes"
	"strconv"
)

/*
weather,location=us-midwest temperature=82 1465839830100400200
  |    -------------------- --------------  |
  |             |             |             |
  |             |             |             |
+-----------+--------+-+---------+-+---------+
|measurement|,tag_set| |field_set| |timestamp|
+-----------+--------+-+---------+-+---------+
*/
type PointConfig struct {
	// Precision is the write precision of the points, defaults to "ns".
	Precision string

	// Database is the database to write points to.
	Database string

	// RetentionPolicy is the retention policy of the points.
	RetentionPolicy string

	// Write consistency is the number of servers required to confirm write.
	WriteConsistency string
}

func (p *PointConfig) GetPrecision() string {
	return p.Precision
}

func (p *PointConfig) SetPrecision(precision string) {
	p.Precision = precision
}

func (p *PointConfig) GetDatabase() string {
	return p.Database
}

func (p *PointConfig) SetDatabase(db string) {
	p.Database = db
}

func (p *PointConfig) GetRetentionPolicy() string {
	return p.RetentionPolicy
}

func (p *PointConfig) SetRetentionPolicy(rp string) {
	p.RetentionPolicy = rp
}

func (p *PointConfig) GetWriteConsistency() string {
	return p.WriteConsistency
}

func (p *PointConfig) SetWriteConsistency(writeConsistency string) {
	p.WriteConsistency = writeConsistency
}

type pointBase struct {
	tagBuf   *bytes.Buffer
	fieldBuf *bytes.Buffer

	timestamp   int64
	measurement string
}

func (p *pointBase) Reset() {
	p.tagBuf.Reset()
	p.fieldBuf.Reset()
}

func (p *pointBase) AppendTag(key, value []byte) {
	p.tagBuf.WriteString(",")

	p.tagBuf.Write(key)
	p.tagBuf.WriteString("=")
	p.tagBuf.Write(value)
}

func (p *pointBase) AppendField(key, value []byte, quotaed bool) {
	if p.fieldBuf.Len() != 0 {
		p.fieldBuf.WriteString(",")
	}

	p.fieldBuf.Write(key)
	p.fieldBuf.WriteString("=")
	if quotaed {
		p.fieldBuf.WriteString("\"")
	}
	p.fieldBuf.Write(value)
	if quotaed {
		p.fieldBuf.WriteString("\"")
	}
}

func (p *pointBase) SetMeasurement(measurementName string) {
	p.measurement = measurementName
}

func (p *pointBase) SetTime(ts int64) {
	p.timestamp = ts
}

type Point struct {
	*pointBase

	PointConfig
}

func NewPoint(conf PointConfig) *Point {
	if conf.Precision == "" {
		conf.Precision = "ns"
	}

	return &Point{
		pointBase: &pointBase{
			tagBuf:   &bytes.Buffer{},
			fieldBuf: &bytes.Buffer{},
		},

		PointConfig: conf,
	}
}

type BatchPoint struct {
	PointConfig

	*pointBase

	mainBuf *bytes.Buffer
}

func NewBatchPoint(conf PointConfig) *BatchPoint {
	if conf.Precision == "" {
		conf.Precision = "ns"
	}

	return &BatchPoint{
		PointConfig: conf,

		mainBuf: &bytes.Buffer{},
		pointBase: &pointBase{
			tagBuf:   &bytes.Buffer{},
			fieldBuf: &bytes.Buffer{},
		},
	}
}

func (b *BatchPoint) Reset() {
	b.mainBuf.Reset()
	b.pointBase.Reset()

}

func (b *BatchPoint) NewLine() {
	b.mainBuf.WriteString(b.measurement)
	b.mainBuf.Write(b.tagBuf.Bytes())
	b.mainBuf.WriteString(" ")
	b.mainBuf.Write(b.fieldBuf.Bytes())
	b.mainBuf.WriteString(" ")
	b.mainBuf.WriteString(strconv.FormatInt(b.timestamp, 10))
	b.mainBuf.WriteString("\n")

	b.pointBase.Reset()
}
