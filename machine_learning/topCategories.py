import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
import matplotlib.patches as mpatches
import seaborn as sb

from sklearn.model_selection import train_test_split
from sklearn import model_selection
from sklearn.linear_model import LogisticRegression
import pickle
from sklearn import preprocessing
from sklearn.externals import joblib

url = f"..\\spark\\dataStreaming1000rows.csv"

df = pd.read_csv(url,sep=';')

#Nos quedamos las primeras 5 tags y las guardamos por columnas para poder pasar el encoder

dftags=df.Tags.str.split(",",5,expand=True)
df = pd.merge(df,dftags, right_index=True, left_index=True)
df = df.drop(columns=['Tags'])

df = df.rename(columns={0: 'Tag0',1: 'Tag1',2: 'Tag2',3: 'Tag3',4: 'Tag4',5: 'Tag5'})

# Suponemos que un video triunfa si tiene más visitas que la media de videos en tendencia
# por lo que las columnas que tiene relación con las visitas hay que eliminarlas.

df['test'] = np.where(df['View_count']>df['View_count'].mean(), 1, 0)

df = df.drop(columns=['Channel_subscribers','Video_title','Description', 'View_count','Like_count', 'Dislike_count','Like_count', 'Comment_count','Favorite_count'])

df = df.replace({float('NaN'):'0'})

df_encoded = MultiColumnLabelEncoder(columns=['videoID','Published_at','Channel_title','Tag0','Tag1','Tag2','Tag3','Tag4','Tag5']).fit_transform(df)
df_encoded.head()

array = df_encoded.values
X = array[:,0:9]
Y = array[:,9]
test_size = 0.33
seed = 7
X_train, X_test, Y_train, Y_test = model_selection.train_test_split(X, Y, test_size=test_size, random_state=seed)

model = LogisticRegression()
model.fit(X_train, Y_train)
model.score(X_test, Y_test)

'''
Podriamos cargar el modelo y usarlo para predecir si un video en tendencias va a triunfar

load the model from disk
loaded_model = joblib.load(filename)
result = loaded_model.score(X_test, Y_test)
print(result)
'''

class MultiColumnLabelEncoder:
    def __init__(self,columns = None):
        self.columns = columns # array of column names to encode

    def fit(self,X,y=None):
        return self # not relevant here

    def transform(self,X):
        '''
        Transforms columns of X specified in self.columns using
        LabelEncoder(). If no columns specified, transforms all
        columns in X.
        '''
        output = X.copy()
        if self.columns is not None:
            for col in self.columns:
                print (col)
                output[col] = LabelEncoder().fit_transform(output[col])
        else:
            for colname,col in output.iteritems():
                print (colname)
                output[colname] = LabelEncoder().fit_transform(col)
        return output

    def fit_transform(self,X,y=None):
        return self.fit(X,y).transform(X)