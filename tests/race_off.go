//go:build !race

package tests

// RaceEnabled is false when built or tested without the -race flag.
const RaceEnabled = false
