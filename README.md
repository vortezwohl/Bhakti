<div style="display: flex;flex-direction: column;align-items: center;">
<img src="https://github.com/vortezwohl/Bhakti/releases/download/icon/Bhakti-logo.png" width="300">
</div>

<div style="display: flex;flex-direction: column;align-items: center;">
<img src="https://github.com/vortezwohl/Bhakti/releases/download/icon/Bhakti.png" width="250">
</div>

<p align="center">
Implemented with <a href="https://github.com/numpy/numpy">Numpy</a>
</p>



## Bhakti is

  1. A light-weight vector database

  2. Easy to use

  3. Thread safe

  4. Portable

  5. Reliable

  6. Based only on Numpy

  7. Suitable for small-sized datasets

## Installation

- From [PYPI](https://pypi.org/project/bhakti/)

    ```shell
    pip install bhakti
    ```

- From [Github](https://github.com/vortezwohl/bhakti/releases)

    Download .whl first then run

    ```shell
    pip install ./bhakti-X.X.X-py3-none-any.whl
    ```

## Quick Start

**Before all, make sure you've successfully installed Bhakti :)**

- ### Run Bhakti Server

  - To begin, create a path for storing data

    ```shell
    mkdir -p /path/to/db
    ```
  
  - #### **Start server using shell command**

    1. Create configuration file (.yaml)

        ```yaml
        # bhakti.yaml
        DIMENSION: 1024
        DB_PATH: /path/to/db
        DB_ENGINE: dipamkara # optional, default to dipamkara
        CACHED: false # optional, default to false
        HOST: 0.0.0.0 # optional, default to 0.0.0.0
        PORT: 23860 # optional, default to 23860
        EOF: <eof> # optional, default to <eof>
        TIMEOUT: 4.0 # optional, default to 4.0 seconds
        BUFFER_SIZE: 256 # optional, default to 256 bytes
        VERBOSE: false # optional, default to false
        ```

    2. Run bhakti in shell

        ```shell
        # bash
        bhakti ./bhakti.yaml
        ```
    
  - #### Start server using Python

      ```python
      # main.py
      from bhakti import BhaktiServer
      from bhakti.database import DBEngine

      if __name__ == '__main__':
          bhakti_server = BhaktiServer(
              dimension=1024,  # required, only vectors with 1024 dimensions are acceptable
              db_path='/path/to/db',  # required, path where stores data, portable
              db_engine=DBEngine.DIPAMKARA,  # optional, default to dipamkara
              cached=False,  # optional, default to false
              host='0.0.0.0',  # optional, default to 0.0.0.0
              port=23860,  # optional, default to 23860
              eof=b'<eof>',  # optional, default to b'<eof>'
              timeout=4.0,  # optional, default to 4.0 seconds
              buffer_size=256,  # optional, default to 256 bytes
              verbose=False  # optional, default to false
          )
          # run server
          bhakti_server.run()
      ```

- ### Interact With A Bhakti Client

  **Currently, Python(>=3.10) is supported**

  ```python
  # main.py
  import asyncio
  import numpy as np
  from bhakti import BhaktiClient
  from bhakti.database import Metric
  from bhakti.database import DBEngine


  async def main():
      client = BhaktiClient(
          server='127.0.0.1',  # optional, default to 127.0.0.1
          port=23860,  # optional, default to 23860
          eof=b'<eof>',  # optional, default to b'<eof>'
          timeout=4.0,  # optional, default to 4.0 seconds
          buffer_size=256,  # optional, default to 256 bytes
          db_engine=DBEngine.DIPAMKARA,  # optional, default to dipamkara
          verbose=False  # optional, default to false
      )
      vector = np.random.randn(1024)
      await client.create(vector=vector, document={'age': 31, 'gender': 'male'})
      await client.create_index('age')
      await client.create_index('gender')
      results = await client.find_documents_by_vector_indexed(
          query='age <= 31 && gender != "female"', 
          vector=vector,
          metric=Metric.EUCLIDEAN_Z_SCORE, 
          top_k=3
      )
      print(results)
      

  if __name__ == '__main__':
      asyncio.run(main())
  ```
