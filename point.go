package influxdb_client

import "bytes"

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

type Point struct {
	tagBuf   bytes.Buffer
	fieldBuf bytes.Buffer

	timestamp   int64
	measurement string
	PointConfig
}

func NewPoint(pointConfig PointConfig) *Point {
	if pointConfig.Precision == "" {
		pointConfig.Precision = "ns"
	}

	return &Point{
		tagBuf:      bytes.Buffer{},
		PointConfig: pointConfig,
	}
}

func (p *Point) Reset() {
	p.tagBuf.Reset()
	p.fieldBuf.Reset()
}

/*
weather,location=us-midwest temperature=82 1465839830100400200
  |    -------------------- --------------  |
  |             |             |             |
  |             |             |             |
+-----------+--------+-+---------+-+---------+
|measurement|,tag_set| |field_set| |timestamp|
+-----------+--------+-+---------+-+---------+
*/

func (p *Point) AppendTag(key, value []byte) {
	p.tagBuf.WriteString(",")

	p.tagBuf.Write(key)
	p.tagBuf.WriteString("=")
	p.tagBuf.Write(value)
}

func (p *Point) AppendField(key, value []byte, quotaed bool) {
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

func (p *Point) SetMeasurement(measurementName string) {
	p.measurement = measurementName
}

func (p *Point) SetTime(ts int64) {
	p.timestamp = ts
}

type BatchPoint struct {
}
