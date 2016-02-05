
import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

titanic_df = pd.read_csv('train.csv')
titanic_df.head()
titanic_df.info()

sns.factorplot(x="Pclass",y="Survived",hue='Sex',data=titanic_df)

def male_female_child(passenger):
    age,sex = passenger
    
    if age < 12:
        return 'child'
    else:
        return sex

titanic_df['person'] = titanic_df[['Age','Sex']].apply(male_female_child,axis=1)

titanic_df[0:10]

sns.factorplot(x='Pclass',y='Survived',hue='person',data=titanic_df)

sns.factorplot(x='Pclass',y='Age',hue='person',data=titanic_df)

sns.factorplot(x='person',y='Survived', col='Pclass', data=titanic_df,saturation=.5,
...                    kind="bar", ci=None, aspect=.6).set_xticklabels(["Men","Women","Children"]).set_titles("{col_name}{col_var}")


fig = sns.FacetGrid(titanic_df,hue='Sex',aspect=3)
fig.map(sns.kdeplot,'Age',shade=True)

oldest = titanic_df['Age'].max()

fig.set(xlim=(0,oldest))

fig.add_legend()

fig = sns.FacetGrid(titanic_df,hue='person',aspect=3)
fig.map(sns.kdeplot,'Age',shade=True)

oldest = titanic_df['Age'].max()

fig.set(xlim=(0,oldest))

fig.add_legend()


