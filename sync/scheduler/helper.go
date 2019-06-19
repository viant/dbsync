package scheduler

import (
	"github.com/viant/toolbox/url"
	"strings"
)

//URLToID converts URL to ID
func uRLToID(URL string) string {
	segments := strings.Split(url.NewResource(URL).ParsedURL.Path, "/")
	if len(segments) > 3 {
		segments = segments[len(segments)-3:]
	}
	pathBasedID := strings.Join(segments, ":")
	pathBasedID = strings.Replace(pathBasedID, "\\", "-", len(pathBasedID))
	pathBasedID = strings.Replace(pathBasedID, " ", "_", len(pathBasedID))
	return pathBasedID
}
