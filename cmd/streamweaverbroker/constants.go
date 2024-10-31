package streamweaverbroker

import "os"

// DEFAULT_CONFIG_FILE_PATH is the default path to the configuration file
var DEFAULT_CONFIG_FILE_PATH = os.Getenv("HOME") + "/.streamweaver/config.yaml"
