// Build-time notice inventory for the actual Linux ingress dependency graph.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	if run() != nil {
		fmt.Fprintln(os.Stderr, "Missing ingress dependency license")
		os.Exit(1)
	}
}
func run() error {
	cmd := exec.Command("go", "list", "-deps", "-json", "./cmd/ingress")
	if arch := os.Getenv("TARGETARCH"); arch != "" {
		cmd.Env = append(os.Environ(), "GOARCH="+arch, "GOOS=linux")
	}
	raw, e := cmd.Output()
	if e != nil {
		return e
	}
	d := json.NewDecoder(strings.NewReader(string(raw)))
	type module struct {
		Path, Version, Dir string
		Main               bool
	}
	modules := map[string]module{}
	for {
		var item struct{ Module *module }
		e = d.Decode(&item)
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		if item.Module != nil && !item.Module.Main {
			modules[item.Module.Path] = *item.Module
		}
	}
	goroot, e := exec.Command("go", "env", "GOROOT").Output()
	if e != nil {
		return e
	}
	license, e := os.ReadFile(filepath.Join(strings.TrimSpace(string(goroot)), "LICENSE"))
	if e != nil {
		return e
	}
	fmt.Printf("Go runtime\n%s\n", license)
	names := []string{}
	for name := range modules {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		m := modules[name]
		entries, e := os.ReadDir(m.Dir)
		if e != nil {
			return e
		}
		found := false
		fmt.Printf("\n%s %s\n", name, m.Version)
		for _, entry := range entries {
			base := strings.ToUpper(strings.Split(entry.Name(), ".")[0])
			if entry.IsDir() {
				continue
			}
			switch base {
			case "LICENSE", "LICENCE", "COPYING", "NOTICE", "COPYRIGHT", "PATENTS":
				raw, e := os.ReadFile(filepath.Join(m.Dir, entry.Name()))
				if e != nil {
					return e
				}
				fmt.Printf("%s\n%s\n", entry.Name(), raw)
				if base == "LICENSE" || base == "LICENCE" || base == "COPYING" {
					found = true
				}
			}
		}
		if !found {
			return fmt.Errorf("no license for %s", name)
		}
	}
	return nil
}
