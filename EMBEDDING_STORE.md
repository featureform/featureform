# Enter: the Embedding Store

Embeddings are dense, numerical vectors that accurately represent sparse data like clickstreams, text, and e-commerce purchases. The use of embeddings has dramatically improved the capability of machine learning powered NLP systems, recommendation engines, and computer vision models. Embeddings have begun to be deployed in earnest.

This transition from lab to production exposed real gaps in the current infrastructure. For example, traditional databases and caches don’t support operations like nearest neighbor lookups. Specialized approximate nearest neighbor indices lack durable storage and other features required for full production use. MLOps systems lack dedicated methods to manage versioning, access, and training for embeddings. Modern ML systems need an embedding store: a database built from the ground up around the machine learning workflow with embeddings. In this article, we’ll deep dive into what embeddings are, how they work, and what your options are for deploying and supporting them.

## What's an embedding?

Most machine learning algorithms run on numerical data. For example, a neural network looks like this.

[image of a NN]

Each of these input features must be numeric. That means that in domains such as recommender systems, we must transform non-numeric variables (ex. items and users) into numbers and tensors. We could try to represent items by a product ID; however, neural networks treat numerical inputs as continuous variables. That means higher numbers are “greater than” lower numbers. It also sees numbers that are similar as being similar items. This makes perfect sense for a field like “age” but is nonsensical when the numbers represent a categorical variable. Prior to embeddings, one of the most common methods used was one-hot encoding.

**One-hot Encoding**

One-hot encoding was a common method for representing categorical variables. This unsupervised technique maps a single category to a vector and generates a binary representation. The actual process is simple. We create a vector with a size equal to the number of categories, with all the values set to 0. We then set the row or rows associated with the given ID or IDs to 1.

[image -> link: https://medium.com/@michaeldelsole/what-is-one-hot-encoding-and-how-to-do-it-f0ae272f1179]

This technically works in turning a category into a set of continuous variables, but we literally end up with a huge vector of 0s with a single or a handful of 1’s. This simplicity comes with drawbacks. For variables with many unique categories, it creates an unmanageable number of dimensions. It omits context around similarity. In vector space, categories with little variance are not any closer together than those with high variance. 

This means that the terms “Hotdog” and “Hamburger” are no closer together than “Hotdog” and “Pepsi”. As a result, we have no way of evaluating the relationship between two entities. We could generate more one-to-one mappings, or attempt to group them and look for similarities. This requires extensive work and manual labeling that’s typically infeasible.

Intuitively, we want to be able to create a denser representation of the categories and maintain some of the implicit relationship information between items. We need a way to reduce the number of categorical variables so we can place items of similar categories closer together. That’s exactly what an embedding is.

**Embeddings solve the encoding problem**

Embeddings are dense numerical representations of real-world objects and relationships, expressed as a vector. The vector space quantifies the semantic similarity between categories. Embedding vectors that are close to each other are considered similar. Sometimes, they are used directly for “Similar items to this” section in an e-commerce store. Other times, embeddings are passed to other models. In those cases, the model can share learnings across similar items rather than treating them as two completely unique categories, as is the case with one-hot encodings. On the other hand, embeddings are much more expensive to compute than one-hot encodings and are far less interpretable.

**How are Embeddings created?**

A common way to create an embedding requires us to first set up a supervised machine learning problem. As a side-effect, training that model encodes categories into embedding vectors. For example, we can set up a model that predicts the next movie a user will watch based on what they are watching now. An embedding model will factorize the input into a vector and that vector will be used to predict the next movie. This means that similar vectors are movies that are commonly watched after similar movies. This makes for a great representation to be used for personalization. So even though we are solving a supervised problem, often called the surrogate problem, the actual creation of embeddings is an unsupervised process.

Defining a surrogate problem is an art, and dramatically affects the behavior of the embeddings. For example, YouTube’s recommender team realized that using the “predict the next video a user is going to click on” resulted in clickbait becoming rampantly recommended. They moved to “predict the next video and how long they are going to watch it” as a surrogate problem and achieved far better results.

## Common Embedding Models

**Principal Component Analysis (PCA)**

One method for generating embeddings is called Principal Component Analysis (PCA). PCA reduces the dimensionality of an entity by compressing variables into a smaller subset. This allows the model to behave more effectively but makes variables more difficult to interpret, and generally leads to a loss of information. A popular implementation of PCA is a technique called SVD.

**SVD**

Singular Value Decomposition, also known as SVD, is a dimensionality reduction technique. SVD reduces the quantity of data set features from N-dimensions to K-dimensions via matrix factorization. For example, let’s represent a user’s video ratings as a matrix of size (Number of users) x (Number of Items) where the value of each cell is the rating that a user gave that item. We first pick a number, k, which is our embedding vector size, and use SVD to turn it into two matrices. One will be (Number of users) x k and the other will be k x (Number of items).

In the resulting matrices, if we multiply a user vector by an item vector, we should get our predicted user rating. If we were to multiply both matrices, we’d end up with the original matrix, but densely filled with all of our predicted ratings. It follows that two items that have similar vectors would result in a similar rating from the same user. In this way, we end up creating user and item embeddings.

[image -> https://heartbeat.fritz.ai/recommender-systems-with-python-part-iii-collaborative-filtering-singular-value-decomposition-5b5dcb3f242b ]

**Word2Vec**

Word2vec generates embeddings from words. Words are encoded into one-hot vectors and fed into a hidden layer that generates hidden weights. Those hidden weights are then used to predict other nearby words. Although these hidden weights are used for training, word2vec will not use them for the task it was trained on. Instead, the hidden weights are returned as embeddings and the model is tossed out.

[insert image of Word2Vec neural network]

Words that are found in similar contexts will have similar embeddings. Beyond that, embeddings can be used to form analogies. For example, the vector from king to man is very similar to the one from queen to woman.

[insert link -> https://medium.com/analytics-vidhya/implementing-word2vec-in-tensorflow-44f93cf2665f]

One problem with Word2Vec is that single words have one vector mapping. This means that all semantic uses for a word are combined into one representation. For example, the word “play” in “I’m going to see a play” and “I want to play” will have the same embedding, without the ability to distinguish context.

**BERT**

Bidirectional Encoder Representations of Transformers, also known as BERT, is a pre-trained model that solves Word2Vec’s context problems. BERT is trained in two steps. First, it is trained across a huge corpus of data like Wikipedia to generate similar embeddings as Word2Vec. The end-user performs the second training step. They train on a corpus of data that fits their context well, for example, medical literature. BERT will be fine-tuned for that specific use case. Also, to create a word embedding, BERT takes into account the context of the word. That means that the word “play” in “I’m going to see a play” and “I want to play” will correctly have different embeddings. BERT has been shown to work so well that even Google is using it to handle long-tail search queries.

## Embeddings in the Real World
Embedding usage started in research labs and quickly became state of the art. Since then, embeddings have found themselves in production machine learning systems across a variety of different fields including NLP, recommender systems, and computer vision.

**Recommender Systems**
A recommender system predicts the preferences and ratings of users for a variety of entities/products. The two most common approaches are collaborative filtering and content-based. Collaborative filtering uses actions to train and form recommendations. Modern collaborative filtering systems almost all use embeddings. As an example, we can use the SVD method defined above to build a recommender system. In that system, multiplying a user embedding by an item embedding generates a rating prediction. This provides a clear relationship between users and products. Similar items beget similar ratings from similar users. This attribute can also be used in downstream models. For example, Youtube’s recommender will use embeddings as inputs to a neural network that predicts watch time.

[youtube NN picture]

## Semantic Search

Users expect search bars to be smarter than a regex. Whether it’s a customer support page, a blog, or Google, a search bar should understand the intent and context of a query, not just look at words. Search engines used to be built around TF-IDF, which also creates an embedding from text. This kind of semantic search worked by finding a document embedding that’s closest to the query embedding using nearest neighbor.

Today, semantic search utilizes more sophisticated embeddings like BERT and may use them in downstream models. In fact, even Google uses BERT on a large percentage of their queries: https://searchengineland.com/google-bert-used-on-almost-every-english-query-342193/ .

#Computer Vision

Embeddings are also used in computer vision for a variety of tasks. We can turn a large, dense image into lower-dimensional vectors that represent higher-level features. For example, we can create a model that builds a high-level representation of a bird, no matter how the bird looks in the image. We can then use the bird embeddings to cluster similar-looking birds, regardless of the environment they are in.

[image link -> https://medium.com/analytics-vidhya/self-supervised-representation-learning-in-computer-vision-part-2-8254aaee937c
]

## Embedding Operations

In the above examples, we see that there are a few common operations applied to embeddings. Any production system that uses embeddings should be able to implement some or all of the below.

**Averaging**

Using something like word2vec, we can end up with an embedding for each word, but we often need an embedding for a full sentence. Similarly, in recommender systems, we may know the items a user clicked on recently, but their user embedding may not have been retrained in days. In these situations, we can average embeddings to create higher-level embeddings. In the sentence example, we can create a sentence embedding by averaging each of the word embeddings. In the recommender system, we can create a user embedding by averaging the last N items they clicked.

[youtube paper snippet]

**Subtraction/Addition**

We mentioned earlier how word embeddings also encode analogies via vector differences. Adding and subtracting vectors can be used for a variety of tasks. For example, we can find the average difference between a coat from a cheap brand and a luxury brand. We can store that delta and use it whenever we want to recommend a luxury item that’s similar to the current item that a user is looking at. We can find the difference between a coke and a diet coke and apply it to other drinks, even those that don’t have diet equivalents, to find the closest thing to a diet version.

**Nearest Neighbor**

Nearest neighbor (NN) is often the most useful embedding operation. It finds things that are similar to the current embedding. In recommender systems, we can use it to recommend similar items. We can create a user embedding and find items that are most relevant to them. We can find a document that’s most similar to a search query. Nearest neighbor is a computationallyn expensive operation however. Performed naively it is O(N*K), where N is the number of items and K is the size of each embedding. However, in most cases when we need nearest neighbors, an approximation would suffice. If we recommend five items to a user, and one is technically the sixth closest item, the user probably won’t care. Approximate nearest neighbor (ANN) algorithms typically drop the complexity of a lookup to O(log(n)).


## Implementations of ANN

**Spotify’s Annoy**

In Spotify’s ANN implementation (Annoy), the embeddings are turned into a forest of trees. Each tree is built using random projections. At every intermediate node in the tree, a random hyperplane is chosen, which divides the space into two subspaces. This hyperplane is chosen by sampling two points from the subset and taking the hyperplane equidistant from them. This is performed k times to generate a forest. Lookups are done via in-order traversal of the nearest tree. Annoy's approach allows the index to be split into multiple static files, the index can be mapped in memory, and the number of trees can be tweaked to change speed and accuracy.

**Locality Sensitive Hashing (LSH)**

Locality Sensitive Hashing (LSH) is another common approach. LSH employs a hash table and stores nearby points in their respective buckets. To create the index, LSH runs many hashing functions which place similar points in the same bucket. In doing so, LSH keeps points with large distances in separate buckets. To retrieve the nearest neighbor, the point in question is hashed, a lookup is done and the closest query point is returned. Some pros include a sub-linear run time, zero reliance on data distribution, and the ability to fine-tune accuracy with the existing data structure.

**Facebook’s FAISS and Hierarchical Navigable Small World Graphs (HNSW)**

Facebook’s implementation, FAISS, uses Hierarchical Navigable Small World Graphs (HNSW). HNSW typically performs well in accuracy and recall. It utilizes a hierarchical graph to create an average path towards certain areas. This graph has a hierarchical, transitive structure with a small average distance between nodes. HNSW traverses the graph and finds the closest adjacent node in each iteration and keeps track of the “best” neighbors seen so far. HNSW has a polylogarithmic time complexity (O(logN)).


**How embeddings are operationalized today**

Getting embeddings into production isn’t easy. The most common ways we’ve seen embeddings operationalized today are via Redis, Postgres, and S3 + Annoy/FAISS. We’ll cover how each of those implementations typically looks and the challenges with each.

**Redis**

Redis is a super-fast in-memory object-store. It makes storing and getting embeddings by key very fast. However, it doesn’t have any native embedding operations. It can’t do nearest-neighbor lookups and it can’t add or average vectors. All of this must be done on the model service. It also doesn’t fit cleanly in a typical MLOps workflow. It doesn’t support versioning, rolling back, or maintaining immutability. When training, the Redis client doesn’t automatically cache embeddings and this can cause unnecessary pressure and cost. It also doesn’t support partitioning embeddings and creating sub-indices.

**Postgres**

Postgres is far more versatile and far slower than Redis. Via plugins, it can be made to perform some of the vector operations manually. However, it does not have a performant nearest neighbor index. Also, having a Postgres lookup in the critical path of a model may add too much latency. Finally, Postgres doesn’t have a great way to cache embeddings on the client when training, causing very, very slow training.

**S3 files + Annoy/FAISS**

Annoy or FAISS are often used to operationalize embeddings when nearest-neighbor lookups are required. Both of these systems are just indices. They aren’t built for durable storage or other vector operations. Alone, they only solve one problem. Typically, companies will store their embeddings in S3 or a similar object storage service. They will load the embeddings and create the ANN index directly on the model when required. Updating embeddings becomes tough, and the system typically ends up with a lot of ad-hoc rules to manage versioning and rollbacks. FAISS and Annoy are great, but they need a full embedding store built around them.

## The Embedding store

Machine learning systems that use an embedding need a type of data infrastructure that:

- Stores their embeddings durably and with high availability
- Allow for approximate nearest neighbor operations
- Enable other operations like partitioning, sub-indices, and averaging
- Manage versioning, access control, and rollbacks painlessly







