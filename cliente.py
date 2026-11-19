import grpc
import no_pb2
import no_pb2_grpc
from no import calcular_porta

def chamada_grpc(node_id, method, request, timeout=3):
    '''Cria um canal gRPC para o nó especificado e chama o método desejado com o request fornecido'''
    with grpc.insecure_channel(f"127.0.0.1:{calcular_porta(node_id)}") as channel:
        stub = no_pb2_grpc.ServidorStub(channel)
        return getattr(stub, method)(request, timeout=timeout)

# Gravação com Redirecionamento: envia solicitações de escrita ao líder
# Se tentar gravar em um seguidor ou candidato -> recebe indicação de quem é o líder atual
def write(id_no, dado, crash):
    try:
        crash = True if crash else False
        response = chamada_grpc(id_no, "ClientWrite", no_pb2.WriteRequest(dado=dado, crash=crash), timeout=15)
        lider_id = response.lider_id
        if response.sucesso:
            print(f"[Nó {id_no}] {response.mensagem}")
        elif id_no != lider_id and lider_id != -1:
            print(f"[Nó {id_no}] {response.mensagem}")
            print(f"[!] Nova solicitação write -> líder {lider_id}...")
            write(lider_id, dado, crash)
        else:
            # Se não tem quorum suficiente, ou algum outro erro
            print(f"[Nó {id_no}] {response.mensagem}")
    except:
        print(f"[Nó {id_no}] não foi possível gravar o dado")

# Consulta nos nós do cluster: permite realizar leituras
# de dados em qualquer um dos nós do cluster
def read(id_no, commit_index):
    response = chamada_grpc(id_no, "ClientRead", no_pb2.ReadRequest(commit_index=commit_index))
    if response.sucesso:
        print(f"[Nó {id_no}] \n- índice = {response.commit_index}\n- termo = {response.termo}\n- dado = {response.dado!r}")
    else:
        print(f"[Nó {id_no}] erro: dado não encontrado")

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
                    crash = int(input("Crash? (0 - Não, 1 - Sim): "))
                    write(id_no, str(dado), crash)
                except ValueError:
                    print("Valores inválidos! ID do nó = número inteiro")
            case "read":
                try:
                    id_no = int(input("Digite o ID do nó para enviar a solicitação de leitura: "))
                    commit_index = int(input("Digite o índice de commit para leitura: "))
                    read(id_no, commit_index)
                except ValueError:
                    print("Valores inválidos! ID do nó e índice do log = números inteiros")
            case "exit":
                print("Saindo do cliente...")
                return
            case _:
                print("Comando inválido. Digite 'write', 'read' ou 'exit'.")

if __name__ == "__main__":
    main()