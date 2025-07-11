package ceph

import (
	"bytes"
	"os/exec"
)

func execute(command string, args ...string) ([]byte, []byte, error) {
	cmd := exec.Command(command, args...)

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	return stdoutBuf.Bytes(), stderrBuf.Bytes(), err
}

func runRBDCommand(args ...string) ([]byte, []byte, error) {
	return execute("rbd", args...)
}
