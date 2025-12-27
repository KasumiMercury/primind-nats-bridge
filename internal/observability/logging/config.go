package logging

type ServiceInfo struct {
	Name     string // K_SERVICE or configured service name
	Version  string // semver, injected via ldflags
	Revision string // K_REVISION, empty for non-GCP
}

type Environment string

const (
	EnvProd Environment = "prod"
	EnvStg  Environment = "stg"
	EnvDev  Environment = "dev"
)

type Module string
