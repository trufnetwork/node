package node

// LogConfig holds logging configuration.
type LogConfig struct {
	Level      string   `json:"level,omitempty"`
	Format     string   `json:"format,omitempty"`
	Output     []string `json:"output,omitempty"`
	FileRollKB int      `json:"file_roll_size,omitempty"`
	RetainMax  int      `json:"retain_max_rolls,omitempty"`
}

// GenesisConfig holds genesis configuration.
type GenesisConfig struct {
	// Path inside the container/instance where the genesis file will be
	Path string `json:"path"`
}

// TelemetryConfig holds telemetry configuration.
type TelemetryConfig struct {
	Enable   bool   `json:"enable,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
}

// P2PConfig holds P2P network configuration.
type P2PConfig struct {
	ListenPort        int      `json:"listen_port,omitempty"`
	PEX               bool     `json:"pex,omitempty"`
	Bootnodes         []string `json:"bootnodes"` // Node-specific
	TargetConnections int      `json:"target_connections,omitempty"`
	External          string   `json:"external"` // Node-specific
}

// ConsensusConfig holds consensus parameters.
type ConsensusConfig struct {
	ProposeTimeout        string `json:"propose_timeout,omitempty"`
	EmptyBlockTimeout     string `json:"empty_block_timeout,omitempty"`
	BlockProposalInterval string `json:"block_proposal_interval,omitempty"`
	BlockAnnInterval      string `json:"block_ann_interval,omitempty"`
}

// DBConfig holds database connection configuration.
type DBConfig struct {
	Host        string `json:"host,omitempty"`
	Port        int    `json:"port,omitempty"`
	User        string `json:"user,omitempty"`
	Pass        string `json:"pass,omitempty"` // Consider Secrets Manager
	Name        string `json:"name,omitempty"`
	ReadTimeout string `json:"read_timeout,omitempty"`
	MaxConns    int    `json:"max_connections,omitempty"`
}

// RPCConfig holds RPC server configuration.
type RPCConfig struct {
	Port             int    `json:"port,omitempty"`
	BroadcastTimeout string `json:"broadcast_tx_timeout,omitempty"`
	RequestTimeout   string `json:"timeout,omitempty"`
	MaxReqSize       int    `json:"max_req_size,omitempty"`
	Private          bool   `json:"private,omitempty"`
	Compression      bool   `json:"compression,omitempty"`
}

// AdminConfig holds admin RPC configuration.
type AdminConfig struct {
	Listen string `json:"listen,omitempty"`
}

// SnapshotsConfig holds snapshot configuration.
type SnapshotsConfig struct {
	Enable          bool `json:"enable,omitempty"`
	RecurringHeight int  `json:"recurring_height,omitempty"`
	MaxSnapshots    int  `json:"max_snapshots,omitempty"`
}

// StateSyncConfig holds state sync configuration.
type StateSyncConfig struct {
	Enable           bool     `json:"enable,omitempty"`
	TrustedProviders []string `json:"trusted_providers,omitempty"`
	DiscoveryTime    string   `json:"discovery_time,omitempty"`
	MaxRetries       int      `json:"max_retries,omitempty"`
	PsqlPath         string   `json:"psql_path,omitempty"`
}

// MigrationsConfig holds database migration configuration.
type MigrationsConfig struct {
	Enable      bool   `json:"enable,omitempty"`
	MigrateFrom string `json:"migrate_from,omitempty"`
}

// CheckpointConfig holds checkpoint configuration.
type CheckpointConfig struct {
	Height int    `json:"height,omitempty"`
	Hash   string `json:"hash,omitempty"`
}

// Values holds the complete configuration data used to render the kwild-config.tmpl template.
type Values struct {
	Log        LogConfig        `json:"log"`
	Genesis    GenesisConfig    `json:"genesis"`
	Telemetry  TelemetryConfig  `json:"telemetry"`
	P2P        P2PConfig        `json:"p2p"`
	Consensus  ConsensusConfig  `json:"consensus"`
	DB         DBConfig         `json:"db"`
	RPC        RPCConfig        `json:"rpc"`
	Admin      AdminConfig      `json:"admin"`
	Snapshots  SnapshotsConfig  `json:"snapshots"`
	StateSync  StateSyncConfig  `json:"state_sync"`
	Migrations MigrationsConfig `json:"migrations"`
	Checkpoint CheckpointConfig `json:"checkpoint"`
	// Add other top-level fields if present in template
}

// NewDefaultValues returns a Values struct populated with the default values
// corresponding to the defaults used in the kwild-config.tmpl template.
func NewDefaultValues() Values {
	return Values{
		Log: LogConfig{
			Level:      "info",
			Format:     "plain",
			Output:     []string{"stdout", "kwild.log"},
			FileRollKB: 10000,
			RetainMax:  0,
		},
		// Genesis path is node-specific, set later.
		Telemetry: TelemetryConfig{
			Enable:   false,
			Endpoint: "127.0.0.1:4318",
		},
		P2P: P2PConfig{
			ListenPort:        6600,
			PEX:               true,
			Bootnodes:         []string{}, // Node-specific, set later.
			TargetConnections: 20,
			External:          "", // Node-specific, set later.
		},
		Consensus: ConsensusConfig{
			ProposeTimeout:        "1s",
			EmptyBlockTimeout:     "1m0s",
			BlockProposalInterval: "1s",
			BlockAnnInterval:      "3s",
		},
		DB: DBConfig{
			Host:        "kwil-postgres",
			Port:        5432,
			User:        "kwild",
			Pass:        "", // Default empty, consider Secrets Manager
			Name:        "kwild",
			ReadTimeout: "45s",
			MaxConns:    60,
		},
		RPC: RPCConfig{
			Port:             8484,
			BroadcastTimeout: "15s",
			RequestTimeout:   "20s",
			MaxReqSize:       6000000,
			Private:          false,
			Compression:      true,
		},
		Admin: AdminConfig{
			Listen: "/tmp/kwild.socket",
		},
		Snapshots: SnapshotsConfig{
			Enable:          false,
			RecurringHeight: 14400,
			MaxSnapshots:    3,
		},
		StateSync: StateSyncConfig{
			Enable:           false,
			TrustedProviders: []string{}, // Defaults to empty list
			DiscoveryTime:    "15s",
			MaxRetries:       3,
			PsqlPath:         "psql",
		},
		Migrations: MigrationsConfig{
			Enable:      false,
			MigrateFrom: "",
		},
		Checkpoint: CheckpointConfig{
			Height: 0,
			Hash:   "",
		},
	}
}
