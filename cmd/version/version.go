package version

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"text/template"

	"github.com/spf13/cobra"

	"github.com/kwilteam/kwil-db/app/shared/display"
)

// Template field labels - centralized to follow no-magic-values rule
const (
	VersionLabel   = "Version:"
	CommitLabel    = "Git commit:"
	BuiltLabel     = "Built:"
	GoVersionLabel = "Go version:"
	OSArchLabel    = "OS/Arch:"
)

var versionTemplate = `
 ` + VersionLabel + `	{{.Version}}
 ` + CommitLabel + `	{{.GitCommit}}
 ` + BuiltLabel + `		{{.BuildTime}}
 ` + GoVersionLabel + `	{{.GoVersion}}
 ` + OSArchLabel + `	{{.Os}}/{{.Arch}}`

type versionInfo struct {
	// build-time info
	Version   string `json:"version"`
	GitCommit string `json:"git_commit"`
	BuildTime string `json:"build_time"`
	// client machine info
	GoVersion string `json:"go_version"`
	Os        string `json:"os"`
	Arch      string `json:"arch"`
}

type respVersionInfo struct {
	Info *versionInfo
}

func (v *respVersionInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.Info)
}

func (v *respVersionInfo) MarshalText() ([]byte, error) {
	tmpl := template.New("version")
	// load different template according to the opts.format
	tmpl, err := tmpl.Parse(versionTemplate)
	if err != nil {
		return []byte(""), fmt.Errorf("template parsing error: %w", err)
	}

	var buf bytes.Buffer

	err = tmpl.Execute(&buf, v.Info)
	if err != nil {
		return []byte(""), fmt.Errorf("template executing error: %w", err)
	}

	bs, err := io.ReadAll(&buf)
	if err != nil {
		return []byte(""), err
	}

	return bs, nil
}

func NewVersionCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "version",
		Short: "Display the application version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp := &respVersionInfo{
				Info: &versionInfo{
					Version:   getVersion(),
					GitCommit: getCommit(),
					BuildTime: getBuildTimeDisplay(),
					GoVersion: runtime.Version(),
					Os:        runtime.GOOS,
					Arch:      runtime.GOARCH,
				},
			}

			return display.PrintCmd(cmd, resp)
		},
	}

	return cmd
}
