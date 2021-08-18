package aliyunsls

import (
	"fmt"
	"reflect"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/thisissc/logevent"
)

type LogEventPipe struct {
	ProjectName  string
	LogstoreName string
}

func NewLogEventPipe(projectName, logstoreName string) logevent.LogEventPipe {
	return &LogEventPipe{
		ProjectName:  projectName,
		LogstoreName: logstoreName,
	}
}

func (p *LogEventPipe) Send(l logevent.LogEvent) error {
	logs := []*sls.Log{}
	content := []*sls.LogContent{}

	leType := reflect.TypeOf(l)
	leValue := reflect.ValueOf(l)
	for i := 0; i < leType.NumField(); i++ {
		field := leType.Field(i)
		fieldName := field.Tag.Get("json")

		switch field.Type.Kind() {
		case reflect.String:
			content = append(content, &sls.LogContent{
				Key:   proto.String(fieldName),
				Value: proto.String(leValue.Field(i).String()),
			})
		case reflect.Int32:
			content = append(content, &sls.LogContent{
				Key:   proto.String(fieldName),
				Value: proto.String(fmt.Sprintf("%d", leValue.Field(i).Int())),
			})
		}
	}

	slsLog := &sls.Log{
		Time:     proto.Uint32(uint32(time.Now().Unix())),
		Contents: content,
	}
	logs = append(logs, slsLog)

	loggroup := &sls.LogGroup{
		Topic:  proto.String(""),
		Source: proto.String(l.IP),
		Logs:   logs,
	}
	err := SlsClient.PutLogs(p.ProjectName, p.LogstoreName, loggroup)
	if err != nil {
		return errors.Wrap(err, "SlsClient put logs error")
	}
	return nil
}
