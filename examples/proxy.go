package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	fmt.Printf("PID is %d\n", os.Getpid())

	// Go creates splice pipes internally and doesn't expose them; it requests
	// 1 MiB (internal/poll.maxSpliceSize), but the kernel caps that at
	// fs.pipe-max-size (and stays at the 64 KiB default if the request fails).
	var pipeMax int
	if b, err := os.ReadFile("/proc/sys/fs/pipe-max-size"); err == nil {
		fmt.Sscan(string(b), &pipeMax)
	}
	fmt.Printf("GOMAXPROCS: %d, splice pipe: requests %d bytes, fs.pipe-max-size = %d bytes (per direction, 2 per connection)\n",
		runtime.GOMAXPROCS(0), 1<<20, pipeMax)

	listenAddr, upstreamAddr := parseArgs()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := serve(ctx, listenAddr, upstreamAddr); err != nil {
			log.Printf("Serve failed: %v", err)
		}
	}()

	<-sigChan
	fmt.Println("Received Ctrl + C, shutting down")
	cancel()

	time.Sleep(100 * time.Millisecond)
}

func parseArgs() (string, string) {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: %s <listen_port> <upstream_port>\n", os.Args[0])
		os.Exit(2)
	}
	listenPort, err := strconv.ParseUint(os.Args[1], 10, 16)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid listen port: %v\n", err)
		os.Exit(2)
	}
	upstreamPort, err := strconv.ParseUint(os.Args[2], 10, 16)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid upstream port: %v\n", err)
		os.Exit(2)
	}
	return fmt.Sprintf("0.0.0.0:%d", listenPort), fmt.Sprintf("127.0.0.1:%d", upstreamPort)
}

func serve(ctx context.Context, listenAddr, upstreamAddr string) error {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}
	defer listener.Close()

	fmt.Printf("Listening on %s\n", listenAddr)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if tcpListener, ok := listener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Now().Add(100 * time.Millisecond))
		}

		conn, err := listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				log.Printf("Temporary accept error: %v", err)
				continue
			}
			return fmt.Errorf("failed to accept: %w", err)
		}

		remoteAddr := conn.RemoteAddr()
		fmt.Printf("Process incoming connection from %s\n", remoteAddr)

		go forwarding(conn, upstreamAddr)
	}
}

func forwarding(stream1 net.Conn, upstreamAddr string) error {
	defer stream1.Close()

	stream2, err := net.Dial("tcp", upstreamAddr)
	if err != nil {
		log.Printf("Failed to connect to remote server: %v", err)
		return err
	}
	defer stream2.Close()

	startTime := time.Now()

	result, err := copyBidirectional(stream1, stream2)

	elapsed := time.Since(startTime)

	fmt.Printf("Forwarded traffic: %+v, avg: %.4f B/s\n", result, float64((result.BytesForward+result.BytesReverse))/elapsed.Seconds())

	if err != nil {
		log.Printf("Failed to copy data: %v", err)
		return err
	}

	return nil
}

type TrafficStats struct {
	BytesForward uint64
	BytesReverse uint64
}

func (t TrafficStats) String() string {
	return fmt.Sprintf("TrafficStats { bytes_forward: %d, bytes_reverse: %d }",
		t.BytesForward, t.BytesReverse)
}

func copyBidirectional(conn1, conn2 net.Conn) (*TrafficStats, error) {
	var stats TrafficStats
	var wg sync.WaitGroup
	var err1, err2 error

	wg.Add(2)

	go func() {
		defer wg.Done()
		defer conn2.Close()
		n, err := io.Copy(conn2, conn1)
		stats.BytesForward = uint64(n)
		err1 = err
	}()

	go func() {
		defer wg.Done()
		defer conn1.Close()
		n, err := io.Copy(conn1, conn2)
		stats.BytesReverse = uint64(n)
		err2 = err
	}()

	wg.Wait()

	if err1 != nil && err1 != io.EOF {
		return &stats, err1
	}
	if err2 != nil && err2 != io.EOF {
		return &stats, err2
	}

	return &stats, nil
}
