# file uploader apis
## Configure the env
### Updata minio configurations in func `Conn`
- `endpoint`, the minio server url
- `accessKeyID` & `secretAccessKey`, minio account and password 
- `useSSL`, whether to enable ssl connection with minio server
### Specify endpoint ssl certification paths in func `main`
- `certFile`, public key
- `keyFile`, private key
## Default endpoint host
- launch http endpoint, it'll be running at `http://127.0.0.1:8080/`
- launch https endpoint, it'll be running at `https://127.0.0.1:8080/`
## API Document
### GET /?bucket=bc1&folder=fd1
| params |   type | example |
|--------|-------:|:-------:|
| bucket | string |   bc1   |
| folder | string |   fd1   |

### POST /?bucket=bc1&folder=fd1
| params |   type | example |
|--------|-------:|:-------:|
| bucket | string |   bc1   |
| folder | string |   fd1   |

### PUT /?bucket=bc1&folder=fd1&file_name=fn1
| params    |   type | example |
|-----------|-------:|:-------:|
| bucket    | string |   bc1   |
| folder    | string |   fd1   |
| file_name | string |   fn1   |
***PUT file bytes into request body***
