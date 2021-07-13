import subprocess
import time
import os
import uuid
import random
import gzip
import client.embeddinghub as eh
import threading
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod


class Benchmark(ABC):

    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def benchmark(self):
        pass

    def num_runs(self):
        return 100


class ThreadableBenchmark(Benchmark):

    def __init__(self):
        self.iter = self.next_args()
        self.channel = eh.EmbeddingHubClient.grpc_channel()

    def client(self):
        return eh.EmbeddingHubClient(self.channel)

    @abstractmethod
    def next_args(self):
        pass

    @abstractmethod
    def next_run(self, args):
        pass

    def benchmark(self):
        args = next(self.iter)
        self.next_run(args)


class CreateSpaceBenchmark(ThreadableBenchmark):

    def name(self):
        return "CreateSpaceBenchmark"

    def __init__(self):
        super().__init__()
        self.space_names = [uuid.uuid4() for _ in range(self.num_runs())]

    def next_args(self):
        for space in self.space_names:
            yield space

    def next_run(self, space):
        self.client().create_space(space, 3)

    def num_runs(self):
        return 100


class FreezeSpaceBenchmark(ThreadableBenchmark):

    def name(self):
        return "FreezeSpaceBenchmark"

    def __init__(self):
        super().__init__()
        self.space_names = [uuid.uuid4() for _ in range(self.num_runs())]
        for space in self.space_names:
            self.client().create_space(space, 3)

    def next_args(self):
        for space in self.space_names:
            yield space

    def next_run(self, space):
        self.client().freeze_space(space)

    def num_runs(self):
        return 100


class CreateSpaceBenchmark(ThreadableBenchmark):

    def name(self):
        return "CreateSpaceBenchmark"

    def __init__(self):
        super().__init__()
        self.space_names = [uuid.uuid4() for _ in range(self.num_runs())]

    def next_args(self):
        for space in self.space_names:
            yield space

    def next_run(self, space):
        self.client().create_space(space, 3)

    def num_runs(self):
        return 100


class NarrowKeysBenchmark(ThreadableBenchmark):

    def name(self):
        return "NarrowKeysBenchmark"

    def __init__(self):
        super().__init__()
        if self.num_runs() % 2 != 0:
            raise "Num runs must be an even number"
        self.space_name = uuid.uuid4()
        self.client().create_space(self.space_name, 3)
        num_keys = 5
        self.keys = [uuid.uuid4() for _ in range(num_keys)]
        self.key_idxs = [
            random.randrange(0, num_keys) for _ in range(self.num_runs() // 2)
        ]
        self.emb = [1, 2, 3]

    def next_args(self):
        for idx in self.key_idxs:
            yield ("set", self.keys[idx])
        for idx in self.key_idxs:
            yield ("get", self.keys[idx])

    def next_run(self, args):
        fn = args[0]
        key = args[1]
        if (fn == "set"):
            self.client().set(self.space_name, key, self.emb)
        else:
            self.client().get(self.space_name, key)

    def num_runs(self):
        return 10000


class SparseKeysBenchmark(ThreadableBenchmark):

    def name(self):
        return "SparseKeysBenchmark"

    def __init__(self):
        super().__init__()
        if self.num_runs() % 2 != 0:
            raise "Num runs must be an even number"
        self.space_name = uuid.uuid4()
        self.client().create_space(self.space_name, 3)
        self.keys = [uuid.uuid4() for _ in range(self.num_runs() // 2)]
        self.emb = [1, 2, 3]

    def next_args(self):
        for key in self.keys:
            yield ("set", key)
        for key in self.keys:
            yield ("get", key)

    def next_run(self, args):
        fn = args[0]
        key = args[1]
        if (fn == "set"):
            self.client().set(self.space_name, key, self.emb)
        else:
            self.client().get(self.space_name, key)

    def num_runs(self):
        return 10000


class BatchSparseKeysBenchmark(ThreadableBenchmark):

    def name(self):
        return "BatchSparseKeysBenchmark-batchsize-{}".format(self.batch_size)

    def __init__(self, batch_size, num_runs=None):
        super().__init__()
        self.num_runs_ = num_runs
        if self.num_runs() % 2 != 0:
            raise "Num runs must be an even number"
        self.space_name = uuid.uuid4()
        self.client().create_space(self.space_name, 3)
        self.emb = [1, 2, 3]
        keys = [uuid.uuid4() for _ in range(self.num_runs() // 2 * batch_size)]
        self.batch_size = batch_size
        self.key_batches = [
            keys[i:i + batch_size] for i in range(0, len(keys), batch_size)
        ]
        self.val_batches = [
            {key: self.emb for key in batch} for batch in self.key_batches
        ]

    def next_args(self):
        for val_batch in self.val_batches:
            yield ("multiset", val_batch)
        for key_batch in self.key_batches:
            yield ("multiget", key_batch)

    def next_run(self, args):
        fn = args[0]
        batch = args[1]
        if (fn == "multiset"):
            self.client().multiset(self.space_name, batch)
        else:
            self.client().get(self.space_name, batch)

    def num_runs(self):
        return self.num_runs_ or 100


class ANNBenchmark(ThreadableBenchmark):

    def name(self):
        return "ANNBenchmark"

    def __init__(self):
        super().__init__()
        self.space_name = uuid.uuid4()
        self.client().create_space(self.space_name, 50)
        self.keys = []
        emb_dict = {}
        with gzip.open("test/glove.6B.50d.txt.gz", "r") as f:
            for i, line in enumerate(f):
                if i == self.num_runs():
                    break
                vals = line.split()
                if len(vals) != 51:
                    continue
                key = vals[0]
                self.keys.append(key)
                embedding = [float(val) for val in vals[1:]]
                emb_dict[key] = embedding
            self.client().multiset(self.space_name, emb_dict)

    def next_args(self):
        for i, key in enumerate(self.keys):
            if i == self.num_runs():
                break
            yield key

    def next_run(self, key):
        self.client().nearest_neighbor(self.space_name, key, 20)

    def num_runs(self):
        return 10000


class MultithreadBenchmark(Benchmark):

    def __init__(self, benchmark, max_workers=10):
        self.singlethreaded = benchmark
        self.max_workers = max_workers
        self.threadpool = ThreadPoolExecutor(max_workers=self.max_workers)
        self.args_list = [args for args in self.singlethreaded.next_args()]

    def name(self):
        return "Multithreaded with {} workers: {}".format(
            self.max_workers, self.singlethreaded.name())

    def benchmark(self):
        with self.threadpool:
            # This forces any exceptions to fire.
            for _ in self.threadpool.map(self.singlethreaded.next_run,
                                         self.args_list):
                pass

    def num_runs(self):
        return 1


proc = subprocess.Popen(os.environ["TEST_SRCDIR"] +
                        "/__main__/embeddingstore/main")
time.sleep(1)

benchmark_classes = [
    ANNBenchmark,
    (BatchSparseKeysBenchmark, (3, 3334)),
    (BatchSparseKeysBenchmark, (100, 100)),
    (BatchSparseKeysBenchmark, (1000, 10)),
    CreateSpaceBenchmark,
    FreezeSpaceBenchmark,
    NarrowKeysBenchmark,
    SparseKeysBenchmark,
]

benchmarks = []
for benchmark_class in benchmark_classes:
    if type(benchmark_class) == tuple:
        cls = benchmark_class[0]
        args = benchmark_class[1]
        benchmarks.append(cls(*args))
        benchmarks.append(MultithreadBenchmark(cls(*args)))
    else:
        cls = benchmark_class
        benchmarks.append(cls())
        benchmarks.append(MultithreadBenchmark(cls()))

import timeit
for i, benchmark in enumerate(benchmarks):
    benchmark_str = "benchmarks[{}]".format(i)
    benchmark_fn = "{}.benchmark()".format(benchmark_str)
    benchmark_time = timeit.timeit(benchmark_fn,
                                   globals=globals(),
                                   number=benchmark.num_runs())
    print("Runs {}: {}: {:.2f} seconds".format(benchmark.num_runs(),
                                               benchmark.name(),
                                               benchmark_time),
          flush=True)
