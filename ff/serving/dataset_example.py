import tensorflow as tf
import idx2numpy
import numpy as np
#load data and convert to numpy array, x is data, y is labels
image_file = 'dataset/train-images-idx3-ubyte'
x_train = idx2numpy.convert_from_file(image_file)
label_file = 'dataset/train-labels-idx1-ubyte'
y_train = idx2numpy.convert_from_file(label_file)
test_image_file = 'dataset/t10k-images-idx3-ubyte'
x_test = idx2numpy.convert_from_file(test_image_file)
test_label_file = 'dataset/t10k-labels-idx1-ubyte'
y_test = idx2numpy.convert_from_file(test_label_file)

imgs = ff.open_file(image_file, test_image_file)
features = imgs.union(test_imgs)



#build model with keras layers
model = tf.keras.models.Sequential([
    tf.keras.layers.Flatten(input_shape=(28, 28)),
    tf.keras.layers.Dense(128, activation='relu'),
    tf.keras.layers.Dropout(0.2),
    tf.keras.layers.Dense(10)
])

print("model summary", model.summary())
predictions = model(x_train[:1]).numpy()
# print('before',predictions)s
p = tf.nn.softmax(predictions).numpy()
# print('after',p)
loss_fn = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
print(loss_fn(y_train[:1], predictions).numpy())
model.compile(optimizer='adam',
loss=loss_fn,
metrics=['accuracy'])
history = model.fit(x_train, y_train, epochs=5)
# print('history', history)
performance = model.evaluate(x_test,  y_test, verbose=2)
print('performance', performance)
