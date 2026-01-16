package oslog

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/OpenSlides/openslides-go/environment"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// level=info  DEBUG, INFO, WARN, ERROR
// msg="..."
// metric="..."
// Dev und prod unterscheiden
//

var isDev bool

func InitLog(lookup environment.Environmenter) {
	devmode, _ := strconv.ParseBool(environment.EnvDevelopment.Value(lookup))

	timeFormat := "2006-01-02T15:04:05:000"

	if devmode {
		isDev = true
		timeFormat = "15:04:05"
	}

	zerolog.TimeFieldFormat = timeFormat
}

func Debug(format string, a ...any) {
	log.Debug().Msg(fmt.Sprintf(format, a...))
}

func Info(format string, a ...any) {
	log.Info().Msg(fmt.Sprintf(format, a...))
}

func Warn(format string, a ...any) {
	log.Warn().Msg(fmt.Sprintf(format, a...))
}

func Error(format string, a ...any) {
	log.Error().Msg(fmt.Sprintf(format, a...))
}

func Metric(metric json.RawMessage) {
	log.Info().RawJSON("metric", metric).Msg("")
}
