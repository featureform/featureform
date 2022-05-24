package runner

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/stoicperlman/fls"
	"io"
	"os"

	provider "github.com/featureform/serving/provider"
)

type DataColumn struct {
	Name string
}

type LineFile interface {
	Read() (string, error) //returns io.EOF
	Seek(index int64) error
}

type RegisterFileRunner struct {
	Offline   provider.OfflineStore
	FilePath  string
	ChunkSize int64
	ChunkIdx  int64
}

func (r *RegisterFileRunner) Run() (CompletionWatcher, error) {
	done := make(chan interface{})
	jobWatcher := &SyncWatcher{
		ResultSync:  &ResultSync{},
		DoneChannel: done,
	}
	go func() {
		if r.ChunkSize == 0 {
			jobWatcher.EndWatch(nil)
			return
		}
		f, err := os.OpenFile(r.FilePath, os.O_RDWR, 0660)
		if err != nil {
			jobWatcher.EndWatch(err)
		}
		defer f.Close()
		file := fls.LineFile(f)
		_, err = file.SeekLine(int64(r.ChunkIdx*r.ChunkSize), io.SeekStart)
		if err != nil {
			jobWatcher.EndWatch(err)
		}
		csvReader := csv.NewReader(file)
		csvReader.LazyQuotes = true
		for i := 0; int64(i) < r.ChunkSize; i++ {
			rec, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				jobWatcher.EndWatch(err)
			}
			//verify data columns uhh
			// do something with read line
			//ask sterling about offline normal table interface
			//copy to offline store logic here
			fmt.Printf("%+v\n", rec)
		}
		jobWatcher.EndWatch(nil)
	}()
	return jobWatcher, nil

}

func (r *RegisterFileRunner) SetIndex(index int) error {
	r.ChunkIdx = int64(index)
	return nil
}

type RegisterFileRunnerConfig struct {
	FilePath      string
	OfflineType   provider.Type
	OfflineConfig provider.SerializedConfig
	Schema        provider.SerializedTableSchema
	ChunkSize     int64
	ChunkIdx      int64
}

func (m *RegisterFileRunnerConfig) Serialize() (Config, error) {
	config, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (m *RegisterFileRunnerConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, m)
	if err != nil {
		return err
	}
	return nil
}

type DataSchema struct {
}

func (m *DataSchema) Serialize() (Config, error) {
	config, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (m *DataSchema) Deserialize(config Config) error {
	err := json.Unmarshal(config, m)
	if err != nil {
		return err
	}
	return nil
}

func RegisterFileRunnerFactory(config Config) (Runner, error) {
	runnerConfig := &RegisterFileRunnerConfig{}
	if err := runnerConfig.Deserialize(config); err != nil {
		return nil, fmt.Errorf("failed to deserialize materialize chunk runner config: %v", err)
	}
	offlineProvider, err := provider.Get(runnerConfig.OfflineType, runnerConfig.OfflineConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure offline provider: %v", err)
	}
	offlineStore, err := offlineProvider.AsOfflineStore()
	if err != nil {
		return nil, fmt.Errorf("failed to convert provider to offline store: %v", err)
	}
	_, err = os.OpenFile(runnerConfig.FilePath, os.O_RDWR, 0660)
	if err != nil {
		return nil, fmt.Errorf("no file present: %v", err)
	}
	schema := &DataSchema{}
	if err := schema.Deserialize(config); err != nil {
		return nil, fmt.Errorf("failed to deserialize schema: %v", err)
	}
	//see if data fits schema
	return &RegisterFileRunner{
		Offline:   offlineStore,
		FilePath:  runnerConfig.FilePath,
		ChunkSize: runnerConfig.ChunkSize,
		ChunkIdx:  runnerConfig.ChunkIdx,
	}, nil

}
