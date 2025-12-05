package ceph

import (
	"bytes"
	"os/exec"
)

type Command interface {
	execute(command ...string) ([]byte, []byte, error)
}

type commandImpl struct {
}

func newCommand() Command {
	return &commandImpl{}
}

func (c *commandImpl) execute(command ...string) ([]byte, []byte, error) {
	cmd := exec.Command(command[0], command[1:]...)

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err := cmd.Run()
	return stdoutBuf.Bytes(), stderrBuf.Bytes(), err
}
