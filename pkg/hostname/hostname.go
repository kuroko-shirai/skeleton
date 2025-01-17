package hostname

import "os"

var _hostname string = ""

func GetHostName() string {
	if _hostname != "" {
		return _hostname
	}

	var err error
	if _hostname, err = os.Hostname(); err == nil {
		return _hostname
	}

	return "unknown"
}
