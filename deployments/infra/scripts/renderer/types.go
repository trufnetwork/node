package renderer

// TemplateName represents a known template filename.
type TemplateName string

// Constants for known template filenames.
const (
	TplInstallDocker   TemplateName = "install_docker.sh.tmpl"
	TplConfigureDocker TemplateName = "configure_docker.sh.tmpl"
	TplTnDBStartup     TemplateName = "tn_db_startup.sh.tmpl"
	TplObserverStart   TemplateName = "observer_start.sh.tmpl"
	TplNodeKeyFetch    TemplateName = "node_key_fetch.sh.tmpl"
	TplKgwStart        TemplateName = "kgw_start.sh.tmpl"
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
	AwsRegion   string
	ObserverDir string
	Prefix      string
	Params      []ParameterDescriptor // Use local ParameterDescriptor type
}

// NodeKeyFetchData holds the data required by the TplNodeKeyFetch template.
type NodeKeyFetchData struct {
	Region        string // AWS Region of the parameter
	ParameterName string // SSM SecureString holding the nodekey.json content
	KeyFilePath   string // Path of the nodekey.json file kwild reads
	Owner         string // User and group that own the key file
}

// KgwStartData holds the data required by the TplKgwStart template.
type KgwStartData struct {
	Region                 string // AWS Region of the parameter
	SessionSecretParameter string // SSM SecureString holding the session secret
	ComposePath            string // Path to the gateway compose file
	ProjectName            string // Compose project name
}
