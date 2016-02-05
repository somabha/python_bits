
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

titanic_df['Emabrked'] = titanic_df['Embarked'].fillna("S")
sns.factorplot('Embarked','Survived',data=titanic_df,size=4,aspect=3)

embark_perc = titanic_df[["Embarked", "Survived"]].groupby(["Embarked"],as_index=False).mean()
sns.set_style("whitegrid")
sns.barplot(x="Embarked",y="Survived",data=embark_perc,order=['S','C','Q'])

titanic_df['Alone'] = titanic_df.SibSp + titanic_df.Parch
titanic_df['Alone'].loc[titanic_df['Alone'] >0] = 'With Family'
titanic_df['Alone'].loc[titanic_df['Alone'] == 0] = 'Alone'
sns.barplot(x='Alone',y='Age',data=titanic_df)

titanic_df['Survivor'] = titanic_df.Survived.map({0:'no',1:'yes'})
sns.factorplot(x='Pclass',y='Survived',hue='person', data=titanic_df,order=[1,2,3])




