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


class Dataset:

	def __init__(self, stub, name, version):
		req = proto.serving_pb2.TrainingDataRequest()
		req.id.name = name
		req.id.version = version
		self.name = name
		self.version = version
		self._stub = stub
		self._req = req
		self._iter = stub.TrainingData(req)
		self._batch_size = 0
		self._shuffled_data_list = []

		self._num_repeat = 1
		self._shuffle_data = False
		self._buffer_size = 8


	def batch(self, num):
		if num <= 0:
			raise ValueError("Batch must be a positive integer")
		self._batch_size = num
		return self

	def unbatch(self):
		self._batch_size = 0
		return self

	def repeat(self, num):
		""" Repeat the dataset NUM number of times """
		self._num_repeat = num

	def restart(self):
		self._num_repeat -= 1
		if self._num_repeat > 0:
			print("Iteration completed")
			self._iter = self._stub.TrainingData(self._req)
			self.shuffle(self._buffer_size)
		# else:
		# 	print("done")
		# 	quit()

	def shuffle(self, buffer_size):
		"""Shuffle the dataset with BUFFER_SIZE buffer size"""
		self._shuffle_data = True
		self._buffer_size = buffer_size
		try:
			for _ in range(buffer_size):
				self._shuffled_data_list.append(Row(next(self._iter)))
		except StopIteration:
			if len(self._shuffled_data_list) == 0:
				raise


	def shuffle_next(self):
		if len(self._shuffled_data_list) == 0:
			return
		random_index = random.randrange(len(self._shuffled_data_list))
		next_row = self._shuffled_data_list.pop(random_index)

		try:
			self._shuffled_data_list.append(Row(next(self._iter)))
		except StopIteration:
			return next_row

		return next_row


	def __iter__(self):
		return self

	def __next__(self):
		if self._batch_size == 0:
			if self._shuffle_data:
				next_row = self.shuffle_next()
				if next_row is not None:
					return next_row
				else:
					self.restart()
					if self._num_repeat == -1:
						quit()
					next_row = self.shuffle_next()
					return next_row
			else:
				try:
					return Row(next(self._iter))
				except StopIteration:
					raise	

		rows = []
		for _ in range(self._batch_size):
			if self._shuffle_data:
				next_row = self.shuffle_next()
				if next_row is None:
					self.restart()
					if self._num_repeat == -1:
						quit()
					return rows
				rows.append(next_row)
			else:
				try:
					rows.append(Row(next(self._iter)))
				except StopIteration:
					return rows
		return rows



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
# print([r for r in dataset])
# print("done basic loop")
dataset.repeat(4)
dataset.shuffle(3)
for r in dataset:
	print(r)
print(client.features([("f1", "v1")], {"user": "a"}))
