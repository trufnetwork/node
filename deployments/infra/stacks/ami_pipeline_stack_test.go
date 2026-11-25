package stacks

import (
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/assertions"
	"github.com/aws/jsii-runtime-go"
	"gopkg.in/yaml.v3"
)

func TestValidateNodeTag(t *testing.T) {
	for _, tag := range []string{"v2.6.8", "2.6.8", "v2.7.0-rc.1", "sha-9b23167"} {
		if err := validateNodeTag(tag); err != nil {
			t.Errorf("validateNodeTag(%q) = %v, want nil", tag, err)
		}
	}
	for _, tag := range []string{"", "latest", "-v2.6.8", ".v2", "v2.6.8; reboot", "v2.6.8\n", "a b", "a/b", strings.Repeat("a", 129)} {
		if err := validateNodeTag(tag); err == nil {
			t.Errorf("validateNodeTag(%q) = nil, want an error", tag)
		}
	}
}

// The AMI's node services run the release in TN_NODE_TAG, with no fallback.
func TestComposeTemplateRunsNodeTag(t *testing.T) {
	var compose struct {
		Services map[string]struct {
			Image string `yaml:"image"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(dockerComposeTemplate), &compose); err != nil {
		t.Fatal(err)
	}
	const want = "ghcr.io/trufnetwork/node:${TN_NODE_TAG:?run tn-node-update <release tag> to set it in /opt/tn/.env}"
	for _, name := range []string{"tn-postgres-roles", "tn-node"} {
		if got := compose.Services[name].Image; got != want {
			t.Errorf("service %s runs %q, want %q", name, got, want)
		}
	}

	if exec.Command("docker", "compose", "version").Run() != nil {
		t.Log("docker compose is not installed; skipping the interpolation check")
		return
	}
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "docker-compose.yml"), dockerComposeTemplate, 0o644)
	images := func(env string) (string, error) {
		writeFile(t, filepath.Join(dir, ".env"), env, 0o600)
		cmd := exec.Command("docker", "compose", "config", "--images")
		cmd.Dir = dir
		cmd.Env = withoutNodeTag(os.Environ())
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	out, err := images("TN_NODE_TAG=v2.6.8\nCHAIN_ID=tn-v2.1\nNETWORK_TYPE=mainnet\n")
	if err != nil || strings.Count(out, "ghcr.io/trufnetwork/node:v2.6.8\n") != 2 {
		t.Errorf("with TN_NODE_TAG=v2.6.8 in .env, docker compose config --images: %v\n%s", err, out)
	}
	out, err = images("CHAIN_ID=tn-v2.1\nNETWORK_TYPE=mainnet\n")
	if err == nil || !strings.Contains(out, "required variable TN_NODE_TAG is missing a value") {
		t.Errorf("with no TN_NODE_TAG in .env, docker compose config should fail, got: %v\n%s", err, out)
	}
}

// The AMI components write the node tag, the compose file and the two
// commands, and each stays under Image Builder's limit on component data.
func TestAMIComponents(t *testing.T) {
	ctx := map[string]interface{}{"stage": "dev", contextNodeTag: "v2.6.8"}
	app := awscdk.NewApp(&awscdk.AppProps{Context: &ctx})
	stack, _ := AmiPipelineStack(app, "AmiTest", &AmiPipelineStackProps{})

	data := map[string]string{} // component name to its data
	components := *assertions.Template_FromStack(stack, nil).FindResources(jsii.String("AWS::ImageBuilder::Component"), nil)
	for _, c := range components {
		props := (*c)["Properties"].(map[string]interface{})
		name, _ := props["Name"].(string)
		d, _ := props["Data"].(string)
		data[name] = d
		t.Logf("component %s has %d characters of data", name, len(d))
		if len(d) > 16000 {
			t.Errorf("component %s has %d characters of data, Image Builder takes at most 16000", name, len(d))
		}
	}

	if !strings.Contains(data["tn-config-setup-dev"], `echo "v2.6.8" | sudo tee /opt/tn/ami-node-tag > /dev/null`) {
		t.Error("tn-config-setup-dev does not write the node tag to /opt/tn/ami-node-tag")
	}
	for _, f := range []struct{ component, path, content string }{
		{"tn-config-setup-dev", "/opt/tn/docker-compose.yml", dockerComposeTemplate},
		{"tn-commands-setup-dev", "/usr/local/bin/tn-node-configure", tnNodeConfigureScript},
		{"tn-commands-setup-dev", "/usr/local/bin/tn-node-update", tnNodeUpdateScript},
	} {
		m := regexp.MustCompile(`echo "([A-Za-z0-9+/=]+)" \| base64 -d[^\n]*` + regexp.QuoteMeta(f.path)).FindStringSubmatch(data[f.component])
		if m == nil {
			t.Errorf("%s does not write %s", f.component, f.path)
			continue
		}
		if got, err := base64.StdEncoding.DecodeString(m[1]); err != nil || string(got) != f.content {
			t.Errorf("%s writes %s with other content (decode error: %v)", f.component, f.path, err)
		}
	}
}

// fakeHost is an AMI's /opt/tn with fake sudo, docker, systemctl, chown and curl.
type fakeHost struct {
	dir, bin, log string
}

func newFakeHost(t *testing.T) *fakeHost {
	t.Helper()
	h := &fakeHost{dir: t.TempDir(), bin: t.TempDir()}
	h.log = filepath.Join(h.bin, "docker.log")
	writeFile(t, filepath.Join(h.dir, "ami-node-tag"), "v2.6.8\n", 0o644)
	fakes := map[string]string{
		// runs the command as the current user
		"sudo": `if [ "$1" = -u ]; then shift 2; fi; exec "$@"`,
		// records each call with the TN_NODE_TAG it gets from the environment and from .env
		"docker": `echo "$* | env=${TN_NODE_TAG-} .env=$(grep '^TN_NODE_TAG=' "$TN_DIR/.env" 2>/dev/null | cut -d= -f2)" >> "$FAKE_LOG"
case "$*" in
  "compose pull"*) exit "${FAKE_PULL_EXIT:-0}" ;;
  "compose ps"*) echo tn-node ;;
esac`,
		"systemctl": `[ "$1" = is-active ] && echo active; exit 0`,
		"chown":     `exit 0`,
		"curl":      `echo 203.0.113.7`,
	}
	for name, body := range fakes {
		writeFile(t, filepath.Join(h.bin, name), "#!/bin/bash\n"+body+"\n", 0o755)
	}
	return h
}

// run runs a script as root would on the AMI, and returns its output and the
// docker calls it made.
func (h *fakeHost) run(t *testing.T, script string, env []string, args ...string) (string, []string, error) {
	t.Helper()
	path := filepath.Join(h.bin, "script.sh")
	writeFile(t, path, script, 0o755)
	_ = os.Remove(h.log)
	cmd := exec.Command("bash", append([]string{path}, args...)...)
	cmd.Env = append(withoutNodeTag(os.Environ()),
		"PATH="+h.bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"TN_DIR="+h.dir, "FAKE_LOG="+h.log)
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	var calls []string
	if raw, readErr := os.ReadFile(h.log); readErr == nil {
		calls = strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	}
	return string(out), calls, err
}

func (h *fakeHost) env(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(h.dir, ".env"))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestNodeUpdateScript(t *testing.T) {
	const configured = "CHAIN_ID=tn-v2.1\nTN_NODE_TAG=v2.6.8\nNETWORK_TYPE=mainnet\nTN_PRIVATE_KEY=1111\n"
	cases := []struct {
		name    string
		env     string // .env before the run; empty means no .env
		args    []string
		pullErr bool
		wantErr bool
		calls   []string
		wantEnv string // .env after the run; empty means unchanged
	}{
		{
			name: "a new tag is pulled, then set, then run",
			env:  configured, args: []string{"v2.6.9"},
			calls: []string{
				"compose pull tn-postgres-roles tn-node | env=v2.6.9 .env=v2.6.8",
				"compose up -d tn-node | env= .env=v2.6.9",
			},
			wantEnv: "CHAIN_ID=tn-v2.1\nTN_NODE_TAG=v2.6.9\nNETWORK_TYPE=mainnet\nTN_PRIVATE_KEY=1111\n",
		},
		{
			name: "no tag keeps the current one",
			env:  configured,
			calls: []string{
				"compose pull tn-postgres-roles tn-node | env=v2.6.8 .env=v2.6.8",
				"compose up -d tn-node | env= .env=v2.6.8",
			},
		},
		{
			name: "a tag with no image leaves .env and the node alone",
			env:  configured, args: []string{"v9.9.9"}, pullErr: true, wantErr: true,
			calls: []string{"compose pull tn-postgres-roles tn-node | env=v9.9.9 .env=v2.6.8"},
		},
		{
			name: "a .env without a tag gets the one passed",
			env:  "CHAIN_ID=tn-v2.1\n", args: []string{"v2.6.9"},
			calls: []string{
				"compose pull tn-postgres-roles tn-node | env=v2.6.9 .env=",
				"compose up -d tn-node | env= .env=v2.6.9",
			},
			wantEnv: "CHAIN_ID=tn-v2.1\nTN_NODE_TAG=v2.6.9\n",
		},
		{name: "a .env without a tag needs one passed", env: "CHAIN_ID=tn-v2.1\n", wantErr: true},
		{name: "latest is refused", env: configured, args: []string{"latest"}, wantErr: true},
		{name: "an empty tag is refused", env: configured, args: []string{""}, wantErr: true},
		{name: "a tag with a command in it is refused", env: configured, args: []string{"v2.6.9;reboot"}, wantErr: true},
		{name: "a tag that reads as a flag is refused", env: configured, args: []string{"--force"}, wantErr: true},
		{name: "two arguments are refused", env: configured, args: []string{"v2.6.9", "v2.6.10"}, wantErr: true},
		{name: "a node that is not configured is refused", args: []string{"v2.6.9"}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newFakeHost(t)
			if tc.env != "" {
				writeFile(t, filepath.Join(h.dir, ".env"), tc.env, 0o600)
			}
			var env []string
			if tc.pullErr {
				env = append(env, "FAKE_PULL_EXIT=1")
			}
			out, calls, err := h.run(t, tnNodeUpdateScript, env, tc.args...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, want error %v; output:\n%s", err, tc.wantErr, out)
			}
			if strings.Join(calls, "\n") != strings.Join(tc.calls, "\n") {
				t.Errorf("docker calls:\n%s\nwant:\n%s", strings.Join(calls, "\n"), strings.Join(tc.calls, "\n"))
			}
			if tc.env == "" {
				return
			}
			want := tc.wantEnv
			if want == "" {
				want = tc.env
			}
			if got := h.env(t); got != want {
				t.Errorf(".env after the run:\n%s\nwant:\n%s", got, want)
			}
			if info, err := os.Stat(filepath.Join(h.dir, ".env")); err != nil || info.Mode().Perm() != 0o600 {
				t.Errorf(".env mode: %v, %v; want 0600", info.Mode().Perm(), err)
			}
		})
	}
}

// tn-node-configure sets the AMI's release, and a reconfigure keeps the release
// tn-node-update set.
func TestNodeConfigureScriptKeepsNodeTag(t *testing.T) {
	h := newFakeHost(t)
	key := strings.Repeat("1", 64)

	if out, _, err := h.run(t, tnNodeConfigureScript, nil, "--private-key", key); err != nil {
		t.Fatalf("configure: %v\n%s", err, out)
	}
	if got, want := h.env(t), "TN_NODE_TAG=v2.6.8\nCHAIN_ID=tn-v2.1\nNETWORK_TYPE=mainnet\nTN_PRIVATE_KEY="+key+"\n"; got != want {
		t.Fatalf(".env after configure:\n%s\nwant:\n%s", got, want)
	}

	if out, _, err := h.run(t, tnNodeUpdateScript, nil, "v2.6.9"); err != nil {
		t.Fatalf("update: %v\n%s", err, out)
	}
	out, _, err := h.run(t, tnNodeConfigureScript, nil, "--enable-mcp")
	if err != nil {
		t.Fatalf("reconfigure: %v\n%s", err, out)
	}
	if got, want := h.env(t), "TN_NODE_TAG=v2.6.9\nCHAIN_ID=tn-v2.1\nNETWORK_TYPE=mainnet\nCOMPOSE_PROFILES=mcp\nTN_PRIVATE_KEY="+key+"\n"; got != want {
		t.Errorf(".env after reconfigure:\n%s\nwant:\n%s", got, want)
	}
	if !strings.Contains(out, "MCP: http://203.0.113.7:8000/sse") {
		t.Errorf("reconfigure output has no MCP address:\n%s", out)
	}
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

// withoutNodeTag drops TN_NODE_TAG, which would override .env for compose.
func withoutNodeTag(env []string) []string {
	var out []string
	for _, kv := range env {
		if !strings.HasPrefix(kv, "TN_NODE_TAG=") {
			out = append(out, kv)
		}
	}
	return out
}
