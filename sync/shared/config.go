package shared

//Config represents service codfiguration
type Config struct {
	ScheduleURL          string
	ScheduleURLRefreshMs int
	Debug                bool
	MaxHistory           int //max history run stats
}
