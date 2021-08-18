package logevent

import (
	"time"
)

type LogEvent struct {
	CreateTime     int32  `json:"createTime" parquet:"name=createtime, type=INT32"`
	Year           int32  `json:"year" parquet:"name=year, type=INT32"`
	Month          int32  `json:"month" parquet:"name=month, type=INT32"`
	Day            int32  `json:"day" parquet:"name=day, type=INT32"`
	Hour           int32  `json:"hour" parquet:"name=hour, type=INT32"`
	Second         int32  `json:"second" parquet:"name=second, type=INT32"`
	Minute         int32  `json:"minute" parquet:"name=minute, type=INT32"`
	IP             string `json:"ip" parquet:"name=ip, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Referer        string `json:"referer" parquet:"name=referer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	UA             string `json:"ua" parquet:"name=ua, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Uid            string `json:"uid" parquet:"name=uid, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Did            string `json:"did" parquet:"name=did, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Os             string `json:"os" parquet:"name=os, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Channel        string `json:"channel" parquet:"name=channel, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Project        string `json:"project" parquet:"name=project, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ProjectVersion int32  `json:"projectVersion" parquet:"name=projectversion, type=INT32"`
	Page           string `json:"page" parquet:"name=page, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Event          string `json:"event" parquet:"name=event, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Target         string `json:"target" parquet:"name=target, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Argument       string `json:"argument" parquet:"name=argument, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func (l *LogEvent) SetCreateTime(t time.Time) {
	loc, _ := time.LoadLocation("PRC")
	t = t.In(loc)
	yr, mo, da := t.Date()
	ho, mi, se := t.Clock()

	l.CreateTime = int32(t.Unix())
	l.Year = int32(yr)
	l.Month = int32(mo)
	l.Day = int32(da)
	l.Hour = int32(ho)
	l.Minute = int32(mi)
	l.Second = int32(se)
}

type LogEventPipe interface {
	Send(le LogEvent) error
}
