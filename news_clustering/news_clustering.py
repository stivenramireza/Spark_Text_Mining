# Preparación datos -> Clustering
from operator import itemgetter
rdd1 = RDD.map(lambda line: ((line[0], line[1]), line[2])) # ((id, tittle),list_content)
rdd2 = rdd1.flatMap(lambda line: [(line[0], word) for word in line[1]]  ) #[ ((id, tittle_article), word),((id, tittle_article), word)) ]
rdd3 = rdd2.map(lambda line : (line, 1)) # [ (((id, tittle_article), word), 1), ((word, tittle_article), 1) ]
rdd4 = rdd3.reduceByKey(lambda accum, n: accum + n) #  [ ((((id, tittle_article), word), n), (((id, tittle_article), word), n)) ]
rdd5 = rdd4.map(lambda line: ((line[0][0][0], line[0][0][1]),(line[0][1],line[1])))#  (((id, tittle),word), n) => ((id,tittle),(word, n)) 
rdd6 = rdd5.groupByKey()
rdd7 = rdd6.map(lambda line: (line[0], sorted(list(line[1]), key=itemgetter(1), reverse=True)[:10]))
rdd7.take(10)

# Clustering Noticias - DOC2VEC
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import utils
from tqdm import tqdm


def concatenar(lista_palabras):
  cadena = []
  for i in lista_palabras:
    word = i[0]
    times = i[1]
    for j in range(times):
      cadena.append(word)
  return cadena

ids = rdd7.map(lambda line: line[0][0]).collect()
tittles = rdd7.map(lambda line: line[0][1]).collect()
texts = rdd7.map(lambda line: concatenar(line[1])).collect()

#WordCount-Total

#dictionary = corpora.Dictionary(texts)
#print(dictionary.token2id)

train_documents = [TaggedDocument(words=doc, tags=[ids[i]]) for i, doc in enumerate(texts)]

#print(train_documents[0])

max_epochs = 100
vec_size = 300
alpha = 0.025

model = Doc2Vec(dm =1,
                size=vec_size,
                min_count=1,
                alpha=alpha,
                min_alpha=0.00025,
                workers=4)


model.build_vocab(train_documents)

train_documents = utils.shuffle(train_documents)

for epoch in range(max_epochs):
    print('iteration {0}'.format(epoch))
    model.train(train_documents,
                total_examples=model.corpus_count,
                epochs=model.iter)
    # decrease the learning rate
    model.alpha -= 0.0002
    # fix the learning rate, no decay
    model.min_alpha = model.alpha

model.save("d2v.model")
print("Model Saved")    

# to find most similar doc using tags
#similar_doc = model.docvecs.most_similar('1')
#print(similar_doc)


# to find vector of doc in training data using tags or in other words, printing the vector of document at index 1 in training data
#print(model.docvecs['1'])

# Buscar un artículo por ID
from gensim.models.doc2vec import Doc2Vec

model= Doc2Vec.load("d2v.model")
#to find the vector of a document which is not in training data
#v1 = model.infer_vector(['trump','die','goverment'])
#print("V1_infer", v1)

# to find most similar doc using tags
similar_doc = model.docvecs.most_similar('17308')
print(similar_doc)

# to find vector of doc in training data using tags or in other words, printing the vector of document at index 1 in training data
#print(model.docvecs['17308'])