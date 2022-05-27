package influxdb_client

import (
	"testing"
)

func TestInsertInfluxdb(t *testing.T) {
	point := NewPoint(PointConfig{
		Database:  "data",
		Precision: "ns",
	})

	point.SetMeasurement("aaa")
	point.AppendTag([]byte("aaa"), []byte("aaa"))
	point.AppendField([]byte("bbb"), []byte("bbb"), true)

	c := NewInfluxdbClient(HTTPConfig{
		Addr:     "http://127.0.0.1:8086",
		Username: "admin",
		Password: "admin",
	})

	if err := c.Write(point); err != nil {
		t.Error(err)
	}
}

func TestBatchInsertInfluxdb(t *testing.T) {
	point := NewBatchPoint(PointConfig{
		Database:  "data",
		Precision: "ns",
	})

	point.SetMeasurement("aaa")
	point.AppendTag([]byte("aaa"), []byte("aaa"))
	point.AppendField([]byte("bbb"), []byte("bbb"), true)
	point.NewLine()

	point.SetMeasurement("aaa")
	point.AppendTag([]byte("cc"), []byte("aaa"))
	point.AppendField([]byte("ddd"), []byte("bbb"), true)
	point.NewLine()

	c := NewInfluxdbClient(HTTPConfig{
		Addr:     "http://127.0.0.1:8086",
		Username: "admin",
		Password: "admin",
	})

	if err := c.BatchWrite(point); err != nil {
		t.Error(err)
	}
}

func BenchmarkInsertInfluxdb(b *testing.B) {
	point := NewPoint(PointConfig{
		Database:  "data",
		Precision: "ns",
	})

	c := NewInfluxdbClient(HTTPConfig{
		Addr:     "http://127.0.0.1:8086",
		Username: "admin",
		Password: "admin",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		point.Reset()

		point.SetMeasurement("aaa")
		point.AppendTag([]byte("aaa"), []byte("aaa"))
		point.AppendField([]byte("bbb"), []byte("bbb"), true)

		if err := c.Write(point); err != nil {
			b.Error(err)
			return
		}
	}
}
