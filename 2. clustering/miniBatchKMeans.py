import pandas as pd
import numpy as np
from sklearn.cluster import MiniBatchKMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from sklearn.metrics import homogeneity_score
from sklearn.datasets import load_files

# for reproducibility
random_state = 0

DATA_DIR = "titles/"
data = load_files(DATA_DIR, encoding="utf-8", decode_error="replace", random_state=random_state)
df = pd.DataFrame(list(zip(data['data'], data['target'])), columns=['text', 'label'])
df = df.drop('text', axis=1).join(df['text'].str.split('\n', expand=True).stack().reset_index(drop=True, level=1).rename('text'))
print(df.label.unique())
vec = TfidfVectorizer(stop_words="english")
vec.fit(df.text.values)
features = vec.transform(df.text.values)
print(features)
cls = MiniBatchKMeans(n_clusters=9, random_state=random_state)
cls.fit(features)
# predict cluster labels for new dataset
cls.predict(features)
# # to get cluster labels for the dataset used while
# # training the model (used for models that does not
# # support prediction on new dataset).
# print(cls.labels_)
# reduce the features to 2D
pca = PCA(n_components=2, random_state=random_state)
reduced_features = pca.fit_transform(features.toarray())
# reduce the cluster centers to 2D
reduced_cluster_centers = pca.transform(cls.cluster_centers_)
plt.scatter(reduced_features[:,0], reduced_features[:,1], c=cls.predict(features))
plt.scatter(reduced_cluster_centers[:, 0], reduced_cluster_centers[:,1], marker='x', s=150, c='b')
plt.show()
print(homogeneity_score(df.label, cls.predict(features)))