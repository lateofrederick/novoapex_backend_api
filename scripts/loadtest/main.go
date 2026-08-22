package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"
)

type workerResult struct {
	durations []time.Duration
	statuses  map[int]int
	failures  int
}

func main() {
	n := flag.Int("n", 1000, "total number of requests to issue")
	c := flag.Int("c", 10, "number of concurrent workers")
	target := flag.String("target", "http://127.0.0.1:3977/health/live", "target URL")
	method := flag.String("method", http.MethodGet, "HTTP method")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request timeout")
	warmup := flag.Int("warmup", 100, "warmup requests discarded before measuring (0 disables)")
	flag.Parse()

	if *n <= 0 {
		fatal("-n must be > 0")
	}
	if *c <= 0 {
		fatal("-c must be > 0")
	}
	parsed, err := url.Parse(*target)
	schemeOK := parsed != nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
	if err != nil || !schemeOK {
		fatal("-target must be an absolute http(s) URL")
	}

	client := &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			MaxIdleConns:          *c,
			MaxIdleConnsPerHost:   *c,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}

	runWarmup(client, *method, *target, *warmup)

	workers := *c
	if workers > *n {
		workers = *n
	}
	results := make([]workerResult, workers)
	var wg sync.WaitGroup

	base := *n / workers
	extra := *n % workers
	started := time.Now()
	for i := 0; i < workers; i++ {
		count := base
		if i < extra {
			count++
		}
		wg.Add(1)
		go func(res *workerResult, count int) {
			defer wg.Done()
			res.statuses = make(map[int]int)
			res.durations = make([]time.Duration, 0, count)
			for j := 0; j < count; j++ {
				begin := time.Now()
				code, err := fire(client, *method, *target)
				took := time.Since(begin)
				if err != nil {
					res.failures++
					continue
				}
				res.durations = append(res.durations, took)
				res.statuses[code]++
			}
		}(&results[i], count)
	}
	wg.Wait()
	elapsed := time.Since(started)

	totalFailures, okResponses, nonOK := 0, 0, 0
	allDurations := make([]time.Duration, 0, *n)
	statusCounts := make(map[int]int)
	for i := range results {
		totalFailures += results[i].failures
		for code, cnt := range results[i].statuses {
			statusCounts[code] += cnt
			if code >= 200 && code < 300 {
				okResponses += cnt
			} else {
				nonOK += cnt
			}
		}
		allDurations = append(allDurations, results[i].durations...)
	}
	sort.Slice(allDurations, func(i, j int) bool { return allDurations[i] < allDurations[j] })

	fmt.Printf("target        %s\n", *target)
	fmt.Printf("requests      %d (ok %d, non-2xx %d, failed %d)\n", *n, okResponses, nonOK, totalFailures)
	fmt.Printf("concurrency   %d\n", workers)
	fmt.Printf("wall time     %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput    %.1f req/s\n", float64(*n)/elapsed.Seconds())
	if len(allDurations) > 0 {
		fmt.Printf("latency min   %s\n", allDurations[0].Round(time.Microsecond))
		fmt.Printf("latency p50   %s\n", percentile(allDurations, 50).Round(time.Microsecond))
		fmt.Printf("latency p95   %s\n", percentile(allDurations, 95).Round(time.Microsecond))
		fmt.Printf("latency p99   %s\n", percentile(allDurations, 99).Round(time.Microsecond))
		fmt.Printf("latency max   %s\n", allDurations[len(allDurations)-1].Round(time.Microsecond))
	}
	codes := make([]int, 0, len(statusCounts))
	for code := range statusCounts {
		codes = append(codes, code)
	}
	sort.Ints(codes)
	for _, code := range codes {
		fmt.Printf("status %-11d %d\n", code, statusCounts[code])
	}
	if totalFailures == *n {
		fatal("every request failed; is the server running?")
	}
}

func runWarmup(client *http.Client, method, target string, warmup int) {
	for i := 0; i < warmup; i++ {
		if _, err := fire(client, method, target); err != nil {
			fatalf("warmup request %d failed: %v", i+1, err)
		}
	}
}

func fire(client *http.Client, method, target string) (int, error) {
	req, err := http.NewRequest(method, target, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

func percentile(sorted []time.Duration, p int) time.Duration {
	rank := (p*len(sorted) + 99) / 100
	idx := rank - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "loadtest:", msg)
	os.Exit(1)
}

func fatalf(format string, args ...any) {
	fatal(fmt.Sprintf(format, args...))
}
