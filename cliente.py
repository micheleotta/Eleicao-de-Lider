import grpc
import no_pb2
import no_pb2_grpc
from no import calcular_porta

def call(node_id, method, request):
    '''Cria um canal gRPC para o nó especificado e chama o método desejado com o request fornecido'''
    with grpc.insecure_channel(f"127.0.0.1:{calcular_porta(node_id)}") as channel:
        stub = no_pb2_grpc.NoStub(channel)
        return getattr(stub, method)(request, timeout=3)

# Gravação com Redirecionamento: envia solicitações de escrita ao líder
# Se tentar gravar em um seguidor ou candidato -> recebe indicação de quem é o líder atual
def write(id_no, dado):
    response = call(id_no, "ClientWrite", no_pb2.WriteRequest(dado=dado))
    lider_id = response.lider_id
    if response.sucesso:
        print(f"[Nó {id_no}] {response.mensagem}")
    elif id_no != lider_id and lider_id is not None:
        print(f"[Nó {id_no}] {response.mensagem}")
        print(f"[!] Nova solicitação write -> líder {lider_id}...")
        write(lider_id, dado)
    else:
        # Se não tem quorum suficiente, ou algum outro erro
        print(f"[Nó {id_no}] {response.mensagem}")

# Consulta nos nós do cluster: permite realizar leituras
# de dados em qualquer um dos nós do cluster
def read(id_no, log_index):
    response = call(id_no, "ClientRead", no_pb2.ReadRequest(log_index=log_index))
    if response.sucesso:
        print(f"[Nó {id_no}] {response.log_index} -- termo={response.termo}, dado={response.dado!r}, commit_index={response.commit_index}")
    else:
        print(f"[Nó {id_no}] dado não encontrado")

def main():
    print("Cliente iniciado!")
    
    while True:
        print("\n====================================")
        comando = input("Digite a operação desejada: 'write', 'read' ou 'exit': ")
        match(comando):
            case "write":
                try:
                    id_no = int(input("Digite o ID do nó para enviar a solicitação de escrita: "))
                    dado = input("Digite o dado a ser gravado: ")
                    write(id_no, str(dado))
                except ValueError:
                    print("Valores inválidos! ID do nó = número inteiro")
            case "read":
                try:
                    id_no = int(input("Digite o ID do nó para enviar a solicitação de leitura: "))
                    log_index = int(input("Digite o índice do log para leitura: "))
                    read(id_no, log_index)
                except ValueError:
                    print("Valores inválidos! ID do nó e índice do log = números inteiros")
            case "exit":
                print("Saindo do cliente...")
                return
            case _:
                print("Comando inválido. Digite 'write', 'read' ou 'exit'.")

if __name__ == "__main__":
    main()