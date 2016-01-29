
# Kernel Density Estimation Plots
import numpy as np
import pandas as pd
from pandas import Series,DataFrame
from numpy.random import randn

# Stats
from scipy import stats

# Plotting
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns

# Command so tha the plots appear in the iPython Notebook
%matplotlib inline

dataset = randn(25)
sns.rugplot(dataset)
x_min = dataset.min() - 2
x_max = dataset.max() + 2

x_axis = np.linspace(x_min,x_max,100)

# Bandwidth estimation
bandwidth = ( (4*dataset.std()**5) / (3*len(dataset))) ** 0.2

kernel_list = []

for data_point in dataset:
    # Create a kernel for each point  & append it to kernel_list
    kernel = stats.norm(data_point,bandwidth).pdf(x_axis)
    kernel_list.append(kernel)
    
    # Scale for plotting
    kernel = kernel / kernel.max()
    kernel = kernel * 0.4
    
    plt.plot(x_axis, kernel,color='grey',alpha=0.5)
    
    plt.ylim(0,1)   
    
