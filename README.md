# Eleicao-de-Lider

## TODO
- Biblioteca gRPC
- Bancos de dados
    - Commited
    - Uncommited
- Nó
    - Eleição
    - FetchRequest

## Dependências
* `pip install -r requirements.txt`

## Para rodar:
Compilar:
* python -m grpc_tools.protoc -I. --python_out=./protos --pyi_out=./protos --grpc_python_out=./protos no.proto