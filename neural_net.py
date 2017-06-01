"""
John Jankowski
Neural Net Implementation
"""
import sys
import matplotlib.pyplot as plt
import numpy as np
import scipy.io
from sklearn import preprocessing
import matplotlib.pyplot as plt



def preprocess_data():
	# load data
	testData = scipy.io.loadmat("hw6_data_dist/letters_data.mat")["test_x"]
	data = scipy.io.loadmat("hw6_data_dist/letters_data.mat")["train_x"]
	labels = scipy.io.loadmat("hw6_data_dist/letters_data.mat")["train_y"]

	# shuffle and separate
	size = labels.shape[0]
	combined = np.concatenate((data, labels), axis=1)
	np.random.shuffle(combined)
	trainingSet = combined[ : int(size * .8)]
	validationSet = combined[int(size * .8) : ]
	trainingData = trainingSet[ : , 0 : -1]
	trainingLabels = trainingSet[ : , -1].reshape(-1, 1)
	validationData = validationSet[ : , 0 : -1]
	validationLabels = validationSet[ : , -1].reshape(-1, 1)

	# normalize
	scaler = preprocessing.StandardScaler().fit(trainingData)
	trainingData = np.concatenate((scaler.transform(trainingData), np.ones((trainingData.shape[0], 1))), axis=1)
	validationData = np.concatenate((scaler.transform(validationData), np.ones((validationData.shape[0], 1))), axis=1)
	testData = np.concatenate((scaler.transform(testData), np.ones((testData.shape[0], 1))), axis=1)

	# one-hot encode labels
	lb = preprocessing.LabelBinarizer().fit(trainingLabels)
	trainingLabels = lb.transform(trainingLabels).astype(float)
	trainingLabels[trainingLabels == 1.0] = .85
	trainingLabels[trainingLabels == 0.0] = .15
	validationLabels = lb.transform(validationLabels)

	# save and return preprocessed data
	np.savez("saved_data.npz", validationData=validationData, validationLabels=validationLabels, trainingData=trainingData, 
			 trainingLabels=trainingLabels, testData=testData)
	return validationData, validationLabels, trainingData, trainingLabels, testData

def calc_score(y, z):
	z[z > .99995] = .99995
	z[z < .00005] = .00005
	return np.average(np.sum(-(y * np.log(z)) - ((1 - y) * np.log(1 - z)), axis=0))


def s(x):
	return 1.0 / (1.0 + np.exp(-x))


def trainNeuralNetwork(X, y, V, W, V_learn_rate, W_learn_rate, decay_rate, scores, iterations):
	if scores != None:
		iters = iterations[-1]
	if scores == None:
		iters = 0
		scores = np.array([])
		iterations = np.array([])
	# initialize weight matrices from N(0, 0.01)
	if V == None:
		V = .1 * np.random.randn(200, 785)
		W = .1 * np.random.randn(26, 201)
	while iters < X.shape[0] * 2:
		i = iters % X.shape[0]
		if (i == 0) and (iters != 0):
			V_learn_rate = decay_rate * V_learn_rate
			W_learn_rate = decay_rate * W_learn_rate
			combined = np.concatenate((X, y), axis=1)
			np.random.shuffle(combined)
			X = combined[:, :786]
			y = combined[:, 786:]
			print("epoch complete")
			#np.savez("saved_weights.npz", V=V, W=W, scores=scores, iterations=iterations)
		sample = X[i]
		label = y[i]
		h = np.tanh(np.dot(V, sample))
		z = s(np.dot(W, np.append(h, 1.0)))
		grad_V = np.outer((np.dot(W[:, :-1].T, z - label) * (1 - h**2)), sample)
		grad_W = np.outer((z - label), np.append(h, 1.0))
		V = V - V_learn_rate * grad_V
		W = W - W_learn_rate * grad_W
		if iters % 300 == 0:
			scores = np.append(scores, calc_score(label, z))
			iterations = np.append(iterations, iters)
		iters += 1 
	return V, W, scores, iterations


def trainMiniBatchNeuralNetwork(X, y, V, W, V_learn_rate, W_learn_rate, decay_rate, scores, iterations):
	if scores != None:
		iters = iterations[-1]
	else:
		iters = 0
		scores = np.array([])
		iterations = np.array([])
	# initialize weight matrices from N(0, 0.01)
	if V == None:
		V = .1 * np.random.randn(200, 785)
		W = .1 * np.random.randn(26, 201)
	while iters < X.shape[0] * 10:
		i = iters % X.shape[0]
		if (i < 50) and (iters != 0):
			V_learn_rate = decay_rate * V_learn_rate
			W_learn_rate = decay_rate * W_learn_rate
			combined = np.concatenate((X, y), axis=1)
			np.random.shuffle(combined)
			X = combined[:, :785]
			y = combined[:, 785:]
			print("epoch complete")
			#np.savez("saved_weights.npz", V=V, W=W, scores=scores, iterations=iterations)
		if i + 50 <= X.shape[0]:
			samples = X[i:i+50].T
			labels = y[i:i+50].T
		else:
			extra = 50 - X.shape[0] + i
			samples = np.concatenate((X[i:], X[:extra]), axis=0).T
			labels = np.concatenate((y[i:], y[:extra]), axis=0).T
		h = np.tanh(np.dot(V, samples))
		z = s(np.dot(W, np.concatenate((h, np.ones((1, 50))), axis=0)))
		grad_V = np.dot(np.dot(W[:, :-1].T, z - labels) * (1 - h**2), samples.T) 
		grad_W = np.dot((z - labels), np.concatenate((h, np.ones((1, 50))), axis=0).T) 
		V = V - V_learn_rate * grad_V
		W = W - W_learn_rate * grad_W
		if iters % 5000 == 0:
			h = np.tanh(np.dot(V, X.T))
			z = s(np.dot(W, np.concatenate((h, np.ones((1, X.shape[0]))), axis=0)))
			scores = np.append(scores, calc_score(y.T, z))
			iterations = np.append(iterations, iters)
		iters += 50 
	return V, W, scores, iterations



def predictNeuralNetwork(images, V, W):
	predictions = []
	for i in images:
		h = np.tanh(np.dot(V, i))
		z = s(np.dot(W, np.append(h, 1.0)))
		predictions += [z]
	return predictions


def errorPercentage(predictions, labels):
	num_correct = 0.0
	size = len(predictions)
	for i in range(size):
		label_index = np.argmax(predictions[i])
		if labels[i][label_index] > .5:
			num_correct += 1.0
	return num_correct / (float(size))



"""
script starts here
Note: This was altered a lot based off of current task I was on so its a bit messy right now
"""


if len(sys.argv) < 3:
	print("first argument: 'load_data' to load preprocessed data, 'load_all' to load V and W as well, or 'new' to start over")
	print("second argument: 'train' to keep training, show training/validation error, and plot score, or 'predict' to predict the test data labels")
	sys.exit()
if len(sys.argv) > 4:
	print("Only type two arguments")
	sys.exit()
arg1 = sys.argv[1]
arg2 = sys.argv[2]
if arg1 == 'new':
	validationData, validationLabels, trainingData, trainingLabels, testData = preprocess_data()
	V, W, scores, iterations = None, None, None, None
if (arg1 == 'load_data') or (arg1 == 'load_all'):
	f = np.load("saved_data.npz")
	trainingData = f['trainingData']
	trainingLabels = f['trainingLabels']
	validationData = f['validationData']
	validationLabels = f['validationLabels']
	testData = f['testData']
	V, W, scores, iterations = None, None, None, None
if arg1 == 'load_all':
	f = np.load("saved_weights.npz")
	V = f['V']
	W = f['W']
	scores = f['scores']
	iterations = f['iterations']
if arg2 == 'train':
	v_rates = [.01]
	w_rates = [.001]
	decay_rates = [.9]
	for vr in v_rates:
		for wr in w_rates:
			for dr in decay_rates:
				V, W, scores, iterations = None, None, None, None
				V, W, scores, iterations = trainMiniBatchNeuralNetwork(trainingData, trainingLabels, V, W, vr, wr, dr, scores, iterations)
				train_preds = predictNeuralNetwork(trainingData, V, W)
				train_error = errorPercentage(train_preds, trainingLabels)
				valid_preds = predictNeuralNetwork(validationData, V, W)
				valid_error = errorPercentage(valid_preds, validationLabels)
				print({'vr':vr, 'wr':wr, 'dr':dr, 'terr':train_error, 'verr':valid_error})
if arg2 == 'predict':
	data = np.concatenate((trainingData, validationData), axis=0)
	labels = np.concatenate((trainingLabels, validationLabels), axis=0)
	V, W, scores, iterations = trainMiniBatchNeuralNetwork(data, labels, V, W, .01, .001, .9, scores, iterations)
	# train_preds = predictNeuralNetwork(trainingData, V, W)
	# train_error = errorPercentage(train_preds, trainingLabels)
	# valid_preds = predictNeuralNetwork(validationData, V, W)
	# valid_error = errorPercentage(valid_preds, validationLabels)
	# print("training error = " + str(train_error))
	# print("validation error = " + str(valid_error))
	plt.close("all")
	plt.figure()
	plt.plot(iterations, scores)
	plt.title("score over iterations")
	plt.show()
	predictions = predictNeuralNetwork(testData, V, W)
	f = open("pred.csv", "w")
	f.write("Id,Category\n")
	for i in range(len(predictions)):
		f.write(str(i+1) + ',' + str(np.argmax(predictions[i]) + 1) + '\n')
	f.close()
if arg2 == 'visualize':
	data = np.concatenate((trainingData, validationData), axis=0)
	labels = np.concatenate((trainingLabels, validationLabels), axis=0)
	alph = {0:'a', 1:'b', 2:'c', 3:'d', 4:'e', 5:'f', 6:'g', 7:'h', 8:'i', 9:'j', 10:'k', 11:'l', 12:'m', 13:'n', 14:'o', 15:'p', 16:'q', 17:'r', 18:'s', 19:'t', 20:'u', 21:'v', 22:'w', 23:'x', 24:'y', 25:'z'}
	V, W, scores, iterations = trainMiniBatchNeuralNetwork(data, labels, V, W, .01, .001, .9, scores, iterations)
	predictions = predictNeuralNetwork(data, V, W)
	i = 0
	correct = []
	c_label = []
	wrong = []
	w_label = []
	while (len(correct) < 5) or (len(wrong) < 5):
		label_index = np.argmax(predictions[i])
		if labels[i][label_index] > .5:
			correct += [data[i]]
			c_label += [alph[label_index]]
		else:
			wrong += [data[i]]
			w_label += [alph[label_index]]
		i+=1
	plt.close("all")
	plt.figure()
	plt.plot(iterations, scores)
	plt.title("score over iterations")
	plt.show()
	for i in range(5):
		plt.figure()
		im = plt.imshow(correct[i][:-1].reshape((28, 28)))
		plt.title("Correct, label = " + c_label[i])
		plt.show()
		plt.figure()
		plt.imshow(wrong[i][:-1].reshape((28, 28)))
		plt.title("Wrong, label = " + w_label[i])
		plt.show()










