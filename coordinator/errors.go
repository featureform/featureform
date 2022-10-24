package coordinator

import "fmt"

type JobDoesNotExistError struct {
	key string
}

func (m *JobDoesNotExistError) Error() string {
	return fmt.Sprintf("Coordinator Job No Longer Exists: %s", m.key)
}

type AuthTokenExpiredError struct{}

func (m AuthTokenExpiredError) Error() string {
	return fmt.Sprintf("etcd authentication token has expired")
}
