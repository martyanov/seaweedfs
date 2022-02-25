package util

const (
	VolumeSizeLimitGB = 30
)

var (
	VERSION = "dev"
)

func Version() string {
	return VERSION
}
