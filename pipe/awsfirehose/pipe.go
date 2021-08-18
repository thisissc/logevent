package awsfirehose

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/thisissc/awsclient"
	"github.com/thisissc/logevent"
)

type LogEventPipe struct {
	AWSProfile        string
	AWSFirehoseStream string
}

func NewLogEventPipe(profile, streamName string) logevent.LogEventPipe {
	return &LogEventPipe{
		AWSProfile:        profile,
		AWSFirehoseStream: streamName,
	}
}

func jsonEncode(l logevent.LogEvent) []byte {
	payload, _ := json.Marshal(l)
	payload = append(payload, byte(10)) // append "\n"
	return payload
}

func (p *LogEventPipe) Send(le logevent.LogEvent) error {
	payload := jsonEncode(le)
	err := awsclient.Send2Firehose(p.AWSProfile, p.AWSFirehoseStream, payload)
	if err != nil {
		return errors.Wrap(err, "Send to firehose error")
	}

	return nil
}
