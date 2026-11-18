package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"tensorforge/internal/platform"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, e := platform.EnvConfig()
	if e != nil {
		slog.Error("configuration", "error", e)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	a, e := platform.New(ctx, cfg)
	if e != nil {
		slog.Error("startup", "error", e)
		os.Exit(1)
	}
	defer a.Close()
	a.Run(ctx)
	srv := &http.Server{Addr: cfg.Addr, Handler: a.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		srv.Shutdown(c)
	}()
	slog.Info("listening", "address", cfg.Addr)
	if e = srv.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		slog.Error("server", "error", e)
		os.Exit(1)
	}
}
