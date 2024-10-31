package process

import (
	"fmt"
	"os"
)

func CreatePIDFile(pidFile string, pid int) error {
	// Remove the pid file if it already exists
	if _, err := os.Stat(pidFile); err == nil {
		if err := os.Remove(pidFile); err != nil {
			return err
		}
	}

	// Create the pid file
	err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", pid)), 0644)
	if err != nil {
		return err
	}

	return nil
}

func RemovePIDFile(pidFile string) error {
	if err := os.Remove(pidFile); err != nil {
		return err
	}

	return nil
}
