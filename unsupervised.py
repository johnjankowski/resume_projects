"""
John Jankowski
unsupervised learning
"""
import sys
import numpy as np
import scipy.io
from sklearn import preprocessing
import matplotlib.pyplot as plt
from skimage.io import imread
import matplotlib.pyplot as plt
from scipy.misc import imsave



"""
Part 1: K-means clustering
"""
def load_mnist_data():
	data = scipy.io.loadmat("hw7_data/mnist_data/images.mat", mat_dtype=True)['images']
	return data.reshape((784, 60000)).T

def calc_score(data, clusters, means, k):
	score = np.sum(np.linalg.norm(data - means[clusters], axis=1), axis=0)
	return score

def step_one(data, clusters, k):
	means = []
	for i in range(k):
		cluster = data[np.where(clusters == i)]
		means += [np.sum(cluster, axis=0) / float(cluster.shape[0])]
	return np.array(means)

def step_two(data, clusters, means, k):
	sqrd_means = np.einsum('ij,ij->i', means, means)
	sqrd_data = np.einsum('ij,ij->i', data, data).reshape((-1, 1))
	dists = sqrd_data + sqrd_means - 2 * np.dot(data, means.T)
	# tie-breaking
	min_dists = np.where(dists == np.amin(dists, axis=1).reshape((-1, 1)))
	use_orig_inds = min_dists[0][np.where(clusters[min_dists[0]] == min_dists[1])]
	best_clusters = np.argmin(dists, axis=1)
	np.put(best_clusters, use_orig_inds, clusters[use_orig_inds])
	return best_clusters

def k_means(data, k):
	means = data[np.random.choice(data.shape[0], k, replace=False)]
	clusters = step_two(data, np.zeros(data.shape[0], dtype=np.int), means, k)
	prev_score = float('inf')
	score = calc_score(data, clusters, means, k)
	i = 0
	while prev_score - score > .00005:
		if i % 10 == 0:
			print("k = " + str(k) + " iteration " + str(i))
		means = step_one(data, clusters, k)
		clusters = step_two(data, clusters, means, k)
		prev_score = score
		score = calc_score(data, clusters, means, k)
		i += 1
	return means

"""
Part 2: Low-Rank Approximation 
"""

def load_face_data():
	return imread('hw7_data/low-rank_data/face.jpg', as_grey=True)

def load_sky_data():
	return imread('hw7_data/low-rank_data/sky.jpg', as_grey=True)

def approximate_img(image, rank):
	U, s, V = np.linalg.svd(image, full_matrices=False)
	return np.dot(np.dot(U[:, :rank], np.diag(s[:rank])), V[:rank])

	
"""
Part 3: Joke Recommender System
"""

def load_joke_tdata():
	data = scipy.io.loadmat("hw7_data/joke_data/joke_train.mat", mat_dtype=True)['train']
	nans = np.isnan(data)
	data[nans] = 0.0
	return data, nans

def load_joke_vdata():
	data = np.loadtxt("hw7_data/joke_data/validation.txt", delimiter=",", dtype=int)
	return data[:, :2], data[:, 2:]

def load_joke_qdata():
	data = np.loadtxt("hw7_data/joke_data/query.txt", delimiter=",", usecols=(1, 2), dtype=int)
	return data

def joke_svd(R, rank):
	U, s, V = np.linalg.svd(R, full_matrices=False)
	return U[:, :rank], V[:rank]

def latent_reg(U, V, R, nans, reg_term):
	prod = np.dot(U, V)
	prod[nans] = 0.0
	prev_score = float('inf')
	score = (np.linalg.norm(prod - R) ** 2) + reg_term * ((np.linalg.norm(U) ** 2) + (np.linalg.norm(V) ** 2))
	i = 0
	while i < 2000:
		U = np.dot(np.dot(R, V.T), np.linalg.inv((np.dot(V, V.T) + reg_term * np.identity(V.shape[0]))))
		V = np.dot(np.linalg.inv((np.dot(U.T, U) + reg_term * np.identity(U.shape[1]))), np.dot(U.T, R))
		prod = np.dot(U, V)
		prod[nans] = 0.0
		prev_score = score
		score = (np.linalg.norm(prod - R) ** 2) + reg_term * ((np.linalg.norm(U) ** 2) + (np.linalg.norm(V) ** 2))
		if i % 100 == 0:
			print("iter" + str(i))
		i += 1
	return np.dot(U, V)

"""
script starts here
Note: This was altered a lot based off of current task I was on so its a bit messy right now
"""

if len(sys.argv) != 2:
	print("need exactly one argument")
	sys.exit()
arg1 = sys.argv[1]
if arg1 == "1":
	data = load_mnist_data()
	for k in [5, 10, 20]:
		cluster_centers = k_means(data, k)
		for i in range(k):
			plt.imshow(cluster_centers[i].reshape((28, 28)))
			plt.savefig('k=' + str(k) + '_' + str(i) + '.png')

if arg1 == '2':
	face_data = load_face_data()
	sky_data = load_sky_data()

	for i in [5, 20, 100]:
		a = approximate_img(face_data, i).astype(np.uint8)
		imsave('prob2images/face_rank' + str(i) + '.png', a)

	mse_list = [np.linalg.norm(face_data - approximate_img(face_data, i)) for i in range(1, 101)]
	val_range = [i for i in range(1, 101)]
	plt.plot(val_range, mse_list)
	plt.title("MSE With Different Rank Approximations")
	plt.ylabel("MSE")
	plt.xlabel("Rank Approximation")
	plt.savefig('prob2images/MSE_graph.png')

	for i in [5, 20, 100]:
		a = approximate_img(sky_data, i).astype(np.uint8)
		imsave('prob2images/sky_rank' + str(i) + '.png', a)

if arg1 == '3':
	tdata, nans = load_joke_tdata()
	vdata, vlabels = load_joke_vdata()
	qdata = load_joke_qdata()

	# part a
	for i in [2, 5, 10, 20]:
		print('d = ' + str(i))
		U, V = joke_svd(tdata, i)
		preds = np.dot(U, V)
		correct = 0
		for j in range(vdata.shape[0]):
			user = vdata[j][0] - 1
			joke = vdata[j][1] - 1
			pred = 0
			if preds[user][joke] > 0:
				pred = 1
			if pred == vlabels[j]:
				correct += 1
		print("validation accuracy = " + str(float(correct)/vdata.shape[0]))
		preds[nans] = 0.0
		mse = np.linalg.norm(preds - tdata)
		print("MSE = " + str(mse))
		print('\n')
		
	# # part b
	terms = [50 * i for i in range(9, 101)]
	best_pair = None
	best_acc = 0
	for i in [11]:
		for reg_term in terms:
			print(reg_term)
			U, V = joke_svd(tdata, i)
			preds = latent_reg(U, V, tdata, nans, reg_term)
			correct = 0
			for j in range(vdata.shape[0]):
				user = vdata[j][0] - 1
				joke = vdata[j][1] - 1
				pred = 0
				if preds[user][joke] > 0:
					pred = 1
				if pred == vlabels[j]:
					correct += 1
			if str(float(correct)/vdata.shape[0]) > best_acc:
				best_pair = (i, reg_term)
				best_acc = str(float(correct)/vdata.shape[0])
			preds[nans] = 0.0
			mse = np.linalg.norm(preds - tdata)
			print("MSE = " + str(mse))
			print('\n')
	print(best_pair)
	print(best_acc)
	U, V = joke_svd(tdata, 11)
	preds = latent_reg(U, V, tdata, nans, 450)
	predictions = []
	for j in range(qdata.shape[0]):
		user = qdata[j][0] - 1
		joke = qdata[j][1] - 1
		pred = 0
		if preds[user][joke] > 0:
			pred = 1
		predictions += [pred]
	f = open("kaggle_submission.txt", "w")
	f.write("Id,Category\n")
	for i in range(len(predictions)):
		f.write(str(i+1) + ',' + str(predictions[i]) + '\n')
	f.close()


	
		


