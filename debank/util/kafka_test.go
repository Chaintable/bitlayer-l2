package util

import (
	"context"
	"testing"
)

func TestKafkaReader(t *testing.T) {
	reader := NewKafkaReader([]string{"b-1.chaintablenodexpi.pz2h9z.c4.kafka.ap-northeast-1.amazonaws.com:9092"}, "nodex_pipeline_test", "")
	reader.SetOffset(0)
	lag, a := reader.ReadLag(context.Background())
	if a != nil {
		t.Error(a)
	}
	t.Log(lag)

	msg, err := GetLastBlockNotice(reader)
	if err != nil {
		t.Error(err)
	}
	t.Log(msg)
}
