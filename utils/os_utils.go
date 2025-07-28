package utils

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func WaitTerminate() <-chan os.Signal {
	c := make(chan os.Signal, 3)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	return c
}

func RedirectFile(from, to *os.File) {
	err := syscall.Dup2(int(to.Fd()), int(from.Fd()))
	if err != nil {
		log.Fatalf("Failed to redirect stderr: %v", err)
	}
}
