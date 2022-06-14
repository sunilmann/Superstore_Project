import pandas as pd

chunk_size=60031

batch_no=1



for chunk in pd.read_csv('Clean_Dataset.csv',chunksize=chunk_size):

  chunk.to_csv('Clean_Dataset.csv' + str(batch_no) + '.csv', index=False)

  batch_no +=1