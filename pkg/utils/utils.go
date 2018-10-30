package utils

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os/exec"
	"strings"
)

// RunCommand runs a command and returns the stdout if successful
func RunCommand(name string, args ...string) ([]byte, error) {
	log.Println("exec", name, args)
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(name, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// return stderr if available
		if s := strings.TrimSpace(stderr.String()); s != "" {
			return nil, errors.New(s)
		}

		// otherwise return error
		return nil, err
	}

	// return stdout
	return stdout.Bytes(), nil
}

// TODO: return stderr ?
// 		 log stderr and only return error ?
// TODO: o *bufio.Writer -> io.Writer
func RunCommandOutTOFile(name string, o io.Writer, args ...string) ([]byte, error) {
	log.Println("exec", name, args)

	var stderr bytes.Buffer
	cmd := exec.Command(name, args...)
	cmd.Stdout = o
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// return stderr if available
		if s := strings.TrimSpace(stderr.String()); s != "" {
			return nil, errors.New(s)
		}

		// otherwise return error
		return nil, err
	}

	// return stdout
	return stderr.Bytes(), nil
}

// RunCommand runs a command and returns the stdout if successful
func RunCommandWithStdin(name string, i io.Reader, args ...string) ([]byte, error) {
	log.Println("exec", name, args)
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(name, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Stdin = i

	if err := cmd.Run(); err != nil {
		// return stderr if available
		if s := strings.TrimSpace(stderr.String()); s != "" {
			return nil, errors.New(s)
		}

		// otherwise return error
		return nil, err
	}

	// return stdout
	return stdout.Bytes(), nil
}
