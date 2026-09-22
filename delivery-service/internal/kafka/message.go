package kafka

import (
	"strconv"
	"strings"

	kafkaGo "github.com/segmentio/kafka-go"
)

func MessageID(msg kafkaGo.Message) string {
	for _, h := range msg.Headers {
		if strings.EqualFold(h.Key, "message-id") {
			return string(h.Value)
		}
	}
	return msg.Topic + ":" + strconv.Itoa(msg.Partition) + ":" + strconv.FormatInt(msg.Offset, 10)
}
