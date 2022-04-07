package runner

import (
	provider "github.com/featureform/serving/provider"
)

type TrainingSetRunner struct {
	Offline provider.OfflineStore
	Def     provider.TrainingSetDef
}

func (m TrainingSetRunner) Run() (CompletionWatcher, error) {
	done := make(chan interface{})
	trainingSetWatcher := &SyncWatcher{
		ResultSync:  &ResultSync{},
		DoneChannel: done,
	}
	go func() {
		if err := m.Offline.CreateTrainingSet(m.Def); err != nil {
			trainingSetWatcher.EndWatch(err)
			return
		}
		trainingSetWatcher.EndWatch(nil)
	}()
	return trainingSetWatcher, nil
}
