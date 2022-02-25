//go:build elastic && gocdk
// +build elastic,gocdk

package command

//set true if gtags are set
func init() {
	isFullVersion = true
}
