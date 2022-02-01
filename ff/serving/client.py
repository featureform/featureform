import grpc
import numpy as np
import proto.serving_pb2
import proto.serving_pb2_grpc
import random


class Client:

	def __init__(self, host):
		channel = grpc.insecure_channel(host,
										options=(('grpc.enable_http_proxy',
												  0),))
		self._stub = proto.serving_pb2_grpc.FeatureStub(channel)

	def dataset(self, name, version):
		return Dataset(self._stub, name, version)

	def features(self, features, entities):
		req = proto.serving_pb2.FeatureServeRequest()
		for name, value in entities.items():
			entity_proto = req.entities.add()
			entity_proto.name = name
			entity_proto.value = value
		for (name, version) in features:
			feature_id = req.features.add()
			feature_id.name = name
			feature_id.version = version
		resp = self._stub.FeatureServe(req)
		return [parse_proto_value(val) for val in resp.values]


class Stream:
	def __init__(self, stub, name, version):
		req = proto.serving_pb2.TrainingDataRequest()
		req.id.name = name
		req.id.version = version
		self.name = name
		self.version = version
		self._stub = stub
		self._req = req
		self._iter = stub.TrainingData(req)

	def __iter__(self):
		return self

	def __next__(self):
		try:
			return Row(next(self._iter))
		except StopIteration:
			raise

	def restart(self):
		self._iter = self._stub.TrainingData(self._req)


class Repeat():
	def __init__(self, repeat_num, stream):
		self.repeat_num = repeat_num
		self.stream = stream

	def __iter__(self):
		return self

	def __next__(self):
		try:
			next_val = next(self.stream)
		except StopIteration:
			self.repeat_num -= 1
			if self.repeat_num > 0:
				self.stream.restart()
				next_val = next(self.stream)
			else:
				raise

		return next_val


class Shuffle:
	def __init__(self, buffer_size, stream):
		self.buffer_size = buffer_size
		self._shuffled_data_list = []
		self.stream = stream
		self.setup_buffer()

	def setup_buffer(self):
		try:
			for _ in range(self.buffer_size):
				self._shuffled_data_list.append(next(self.stream))
		except StopIteration:
			if len(self._shuffled_data_list) == 0:
				raise

	def __iter__(self):
		return self

	def __next__(self):
		if len(self._shuffled_data_list) == 0:
			# If theres nothing left in the buffer itll return None
			raise StopIteration
		random_index = random.randrange(len(self._shuffled_data_list))
		next_row = self._shuffled_data_list.pop(random_index)

		try:
			self._shuffled_data_list.append(next(self.stream))
		except StopIteration:
			# If theres stuff in the buffer but there is nothing new to add
			return next_row

		return next_row


class Batch:
	def __init__(self, batch_size, stream):
		self.batch_size = batch_size
		self.stream = stream

	def __iter__(self):
		return self

	def __next__(self):
		rows = []
		for _ in range(self.batch_size):
			try:
				next_row = next(self.stream)
				rows.append(next_row)
			except StopIteration:
				if len(rows) == 0:
					raise StopIteration
				else:
					return rows
		return rows
		

class Dataset:
	def __init__(self, stub, name, version):
		self.stream = Stream(stub, name, version)
		self.num_repeat = 0
		self.shuffle_buffer_size = 0
		self.batch_size = 0
		self.run_once = True


	def repeat(self, num):
		self.num_repeat = num

	def shuffle(self, buffer_size):
		self.shuffle_buffer_size = buffer_size
		
	def batch(self, batch_size):
		self.batch_size = batch_size

	def __iter__(self):
		return self

	def __next__(self):
		if self.run_once:
			self.run_once = False
			self.define_order()
		try:
			next_val = next(self.stream)
		except StopIteration:
			raise
		return next_val

	def define_order(self):
		if self.num_repeat: self.stream = Repeat(self.num_repeat, self.stream)
		if self.shuffle_buffer_size: self.stream = Shuffle(self.shuffle_buffer_size, self.stream)
		if self.batch_size: self.stream = Batch(self.batch_size, self.stream)
		return




class Row:

	def __init__(self, proto_row):
		features = np.array(
			[parse_proto_value(feature) for feature in proto_row.features])
		self._label = parse_proto_value(proto_row.label)
		self._row = np.append(features, self._label)

	def features(self):
		return self._row[:-1]

	def label(self):
		return self._label

	def to_numpy():
		return self._row()

	def __repr__(self):
		return "Features: {} , Label: {}".format(self.features(), self.label())


def parse_proto_value(value):
	""" parse_proto_value is used to parse the one of Value message
	"""
	return getattr(value, value.WhichOneof("value"))


client = Client("localhost:8080")
dataset = client.dataset("f1", "v1")
dataset.shuffle(15)
dataset.batch(5)
dataset.repeat(3)
for r in dataset:
	print(r)
print(client.features([("f1", "v1")], {"user": "a"}))
