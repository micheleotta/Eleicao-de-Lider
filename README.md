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
* python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. no.proto

De 3 a 4 nós (n = um número de 0 a 3):
* python no.py --id=n