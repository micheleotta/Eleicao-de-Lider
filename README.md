# Eleição de Líder

Sistema distribuído de replicação de dados utilizando o modelo *Pull (Fetch-based, estilo KRaft/Kafka)*, *eleição de líder (estilo Raft)* e suporte a leitura eventual em qualquer nó. Com a comunicação entre todos os nós realizada exclusivamente via gRPC.

O ambiente é composto por 5 processos distintos: 1 Cliente e 4 nós do *Cluster*.

## Dependências
* `pip install -r requirements.txt`

## Para rodar:
Compilar:
* `python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. no.proto`

De 3 a 4 nós (n = um número de 0 a 3):
* `python no.py --id=n`
* `python cliente.py`

## Exemplificação
<img width="1244" height="628" alt="image" src="https://github.com/user-attachments/assets/7f655ff5-ff4c-438a-b404-a450f2b71850" />
