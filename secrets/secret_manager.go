package secrets

type SecretLocation struct {
	Path    string            `json:"path,omitempty"`
	Key     string            `json:"key"`
	Options map[string]string `json:"options,omitempty"`
}

type SecretManager interface {
	GetSecret(location SecretLocation) (string, error)
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}
