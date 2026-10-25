package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slopdb/storage"
	"time"
)

type remoteMemory struct {
	TotalAlloc, Mallocs uint64
	NumGC               uint32
	PauseTotalNs        uint64
}
type remoteCounters struct {
	Stats      storage.LSMStats
	CPUSeconds float64
	MemStats   remoteMemory
}

func readRemoteCounters(address string) (*remoteCounters, error) {
	if address == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server metrics: %s", response.Status)
	}
	var counters remoteCounters
	if err = json.NewDecoder(response.Body).Decode(&counters); err != nil {
		return nil, err
	}
	return &counters, nil
}
