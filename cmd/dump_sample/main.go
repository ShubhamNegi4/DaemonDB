// dump_sample runs the seed and all index inspectors, writing all output to
// cmd/sample_run_output.txt. Run from repo root: go run ./cmd/dump_sample
package main

import (
	"DaemonDB/bplustree"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

const (
	baseDir   = "databases/demp"
	outputFile = "cmd/sample_run_output.txt"
)

func main() {
	outPath := outputFile
	// If run from cmd/dump_sample, output next to binary
	if _, err := os.Stat("cmd"); os.IsNotExist(err) {
		outPath = "sample_run_output.txt"
	}

	f, err := os.Create(outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create output file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// Clean previous run so seed starts fresh
	os.RemoveAll(baseDir)

	// 1) Run seed: capture stdout/stderr to file
	fmt.Fprintln(f, "========== SEED (create DB demp, tables, inserts, selects) ==========")
	cmd := exec.Command("go", "run", "./cmd/seed")
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Dir = repoRoot()
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(f, "seed exited with error: %v\n", err)
	}

	// 2) Dump each primary key index
	for _, name := range []string{"students_primary", "courses_primary", "grades_primary"} {
		path := filepath.Join(baseDir, "indexes", name+".idx")
		fmt.Fprintf(f, "\n========== INSPECT %s.idx ==========\n", name)
		if err := bplus.InspectIndexFileTo(f, path); err != nil {
			fmt.Fprintf(f, "inspect error: %v\n", err)
		}
	}

	fmt.Printf("Output written to %s\n", outPath)
}

func repoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return dir
		}
		dir = parent
	}
}
