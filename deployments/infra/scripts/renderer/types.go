package renderer

// TemplateName represents a known template filename.
type TemplateName string

// Constants for known template filenames.
const (
	TplInstallDocker   TemplateName = "install_docker.sh.tmpl"
	TplConfigureDocker TemplateName = "configure_docker.sh.tmpl"
	TplTnDBStartup     TemplateName = "tn_db_startup.sh.tmpl"
	TplObserverStart   TemplateName = "observer_start.sh.tmpl"
)

// TnStartupData holds the data required by the TplTnDBStartup template.
type TnStartupData struct {
	Region           string
	RepoURI          string
	ImageURI         string
	ComposePath      string
	TnDataPath       string
	PostgresDataPath string
	EnvVars          map[string]string // Original map
	SortedEnvKeys    []string          // Sorted keys for deterministic iteration
}

// ParameterDescriptor defines the structure for parameters needed by observer_start.sh.tmpl.
// This is defined locally to avoid import cycles.
type ParameterDescriptor struct {
	EnvName        string
	EnvValue       string // Used if IsSSMParameter is false
	IsSSMParameter bool
	SSMPath        string // Used if IsSSMParameter is true
	IsSecure       bool   // Used if IsSSMParameter is true
}

// ObserverStartData holds the data required by the TplObserverStart template.
type ObserverStartData struct {
	ObserverDir string
	Prefix      string
	Params      []ParameterDescriptor // Use local ParameterDescriptor type
}
