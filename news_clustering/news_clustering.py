# Preparación datos -> Clustering
from operator import itemgetter
rdd1 = RDD.map(lambda line: ((line[0], line[1]), line[2])) # [ ((id, tittle),list_content), ((id, tittle),list_content) ]
rdd2 = rdd1.flatMap(lambda line: [(line[0], word) for word in line[1]]  ) #[ ((id, tittle_article), word), ((id, tittle_article), word)) ]
rdd3 = rdd2.map(lambda line : (line, 1)) # [ (((id, tittle_article), word), 1), ((word, tittle_article), 1) ]
rdd4 = rdd3.reduceByKey(lambda accum, n: accum + n) #  [ ((((id, tittle_article), word), n), (((id, tittle_article), word), n)) ]
rdd5 = rdd4.map(lambda line: ((line[0][0][0], line[0][0][1]),(line[0][1],line[1])))#  [ ((id,tittle),(word, n)) ]
rdd6 = rdd5.groupByKey()
rdd7 = rdd6.map(lambda line: (line[0], sorted(list(line[1]), key=itemgetter(1), reverse=True)[:10]))
diccionario2 = rdd7.collectAsMap()
diccionario2.keys()

# Clustering Noticias - DOC2VEC
dbutils.library.installPyPI('gensim')
dbutils.library.installPyPI('tqdm')

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

train_documents = [TaggedDocument(words=doc, tags=[ids[i]]) for i, doc in enumerate(texts)]
max_epochs = 20
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

# Buscar un artículo por ID
from gensim.models.doc2vec import Doc2Vec

model= Doc2Vec.load("d2v.model")
id_busqueda = '20658'
similar_doc = model.docvecs.most_similar(id_busqueda)
print(similar_doc)
lista_final = []

for i in similar_doc:
  lista_final.append(i[0])
  
indice = ids.index(id_busqueda)
print("{}, {}, {}".format(ids[indice],tittles[indice],lista_final)
