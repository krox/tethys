package engine

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

type UCIEngine struct {
	path string
	args []string

	cmd   *exec.Cmd
	stdin io.WriteCloser
	out   *bufio.Reader
}

func NewUCIEngine(path string, args []string) *UCIEngine {
	return &UCIEngine{path: path, args: args}
}

func (e *UCIEngine) Start(ctx context.Context) error {
	e.cmd = exec.CommandContext(ctx, e.path, e.args...)
	stdout, err := e.cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := e.cmd.StderrPipe()
	if err != nil {
		return err
	}
	stdin, err := e.cmd.StdinPipe()
	if err != nil {
		return err
	}
	e.stdin = stdin
	e.out = bufio.NewReader(io.MultiReader(stdout, stderr))

	if err := e.cmd.Start(); err != nil {
		return err
	}

	if err := e.Send("uci"); err != nil {
		return err
	}
	if _, err := e.ReadUntilPrefix(ctx, "uciok", 5*time.Second); err != nil {
		return err
	}

	return nil
}

func (e *UCIEngine) Close() error {
	if e.cmd == nil {
		return nil
	}
	if e.stdin != nil {
		_ = e.Send("quit")
		_ = e.stdin.Close()
	}

	done := make(chan error, 1)
	go func() { done <- e.cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		if e.cmd.Process != nil {
			_ = e.cmd.Process.Kill()
		}
		return <-done
	}
}

func (e *UCIEngine) Send(line string) error {
	if e.stdin == nil {
		return fmt.Errorf("engine not started")
	}
	_, err := io.WriteString(e.stdin, line+"\n")
	return err
}

func (e *UCIEngine) ReadLine() (string, error) {
	if e.out == nil {
		return "", fmt.Errorf("engine not started")
	}
	return e.out.ReadString('\n')
}

func (e *UCIEngine) ReadUntilPrefix(ctx context.Context, prefix string, timeout time.Duration) (string, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
			return "", fmt.Errorf("timeout waiting for %q", prefix)
		default:
			line, err := e.out.ReadString('\n')
			if err != nil {
				return "", err
			}
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, prefix) {
				return line, nil
			}
		}
	}
}

func (e *UCIEngine) IsReady(ctx context.Context) error {
	if err := e.Send("isready"); err != nil {
		return err
	}
	_, err := e.ReadUntilPrefix(ctx, "readyok", 5*time.Second)
	return err
}

func (e *UCIEngine) NewGame(ctx context.Context) error {
	if err := e.Send("ucinewgame"); err != nil {
		return err
	}
	return e.IsReady(ctx)
}

func (e *UCIEngine) BestMoveMovetime(ctx context.Context, movesUCI []string, movetimeMS int) (string, error) {
	pos := "position startpos"
	if len(movesUCI) > 0 {
		pos += " moves " + strings.Join(movesUCI, " ")
	}
	if err := e.Send(pos); err != nil {
		return "", err
	}
	if err := e.Send(fmt.Sprintf("go movetime %d", movetimeMS)); err != nil {
		return "", err
	}

	for {
		line, err := e.out.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "bestmove ") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				return parts[1], nil
			}
			return "", fmt.Errorf("malformed bestmove: %q", line)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}
	}
}
