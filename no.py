import time
import random
import threading
import argparse
import grpc
import no_pb2
import no_pb2_grpc

from concurrent import futures
from enum import StrEnum

DEBUG = True
QTD_NOS = 4
QUORUM = 3
TEMPO_HEARTBEAT = 0.1 if not DEBUG else 1
TEMPO_ELEICAO = 0.3 if not DEBUG else 3

estado = StrEnum('estado', ['seguidor', 'candidato', 'lider'])
end_event = threading.Event()

class No(no_pb2_grpc.NoServicer):
    def __init__(self, id):
        self.id = id
        self.termo_atual = 0
        self.indice_log = 0
        self.indice_commit = 0
        self.ultimo_heartbeat = time.time()
        self.votos_recebidos = 0

        self.nos = [no_pb2_grpc.NoStub(channel) for channel in channels]
        self.heartbeat_thread = threading.Thread(target=self.HeartbeatLoop, daemon=True)
        self.heartbeat_event = threading.Event()

        self.eleicao_thread = threading.Thread(target=self.EleicaoLoop, daemon=True)
        self.eleicao_event = threading.Event()
        self.eleicao_thread.start()

        self.VirarSeguidor()
    
    def __exit__(self):
        end_event.set()
        self.heartbeat_event.set()
        self.eleicao_event.set()
        self.heartbeat_thread.join()
        self.eleicao_thread.join()

    def RequestVote(self, request, context):
        if request.termo > self.termo_atual:
            self.termo_atual = request.termo
            self.VirarSeguidor(request.id)
            return no_pb2.VoteResponse(voto=True)
        
        return no_pb2.VoteResponse(voto=False)

    def Hearbeat(self, request, context):
        # Implementação do método Hearbeat
        pass

    def VirarLider(self):
        self.estado_atual = estado.lider
        self.lider = None
        print(f"\nNó {self.id} se tornou líder no termo {self.termo_atual}")

    def VirarSeguidor(self, id_lider=None):
        self.estado_atual = estado.seguidor
        self.lider = no_pb2_grpc.NoStub(channels[id_lider]) if id_lider is not None else None

        if not self.heartbeat_thread.is_alive():
            self.heartbeat_thread.start()
        
        print(f"\nNó {self.id} voltou a ser seguidor no termo {self.termo_atual}")

    def IniciarVotacao(self):
        self.estado_atual = estado.candidato
        self.termo_atual += 1
        self.votos_recebidos = 1

        for no in self.nos:
            try:
                response = no.RequestVote(no_pb2.VoteRequest(
                    id=self.id,
                    termo=self.termo_atual,
                    indice_log=self.indice_log
                ))

                if response.voto:
                    self.votos_recebidos += 1
            except grpc.RpcError as e:
                print(f"\nErro ao solicitar voto para o nó {self.nos.index(no)}")

        if self.votos_recebidos >= QUORUM:
            print("\nEleito como líder!")
            self.VirarLider()
        else:
            print("\nNão obteve votos suficientes. Voltando a ser seguidor.")
            self.VirarSeguidor()

    def HeartbeatLoop(self):
        print("\nHeartbeat iniciou")
        exibir_estado(self.estado_atual, self.termo_atual, self.indice_commit, self.lider)
        
        while not self.heartbeat_event.wait(TEMPO_HEARTBEAT) and not end_event.is_set():
            try:
                if self.estado_atual == estado.seguidor and self.lider is not None:
                    response = self.lider.Heartbeat(no_pb2.HeartbeatRequest(
                        termo=self.termo_atual,
                        indice_log=self.indice_log
                    ))
                    self.eleicao_event.set() # Reinicia o temporizador de eleição

                    # Atualizar logs            
                    exibir_estado(self.estado_atual, self.termo_atual, self.indice_commit, self.lider)
            except grpc.RpcError as e:
                print(f"\nErro ao enviar heartbeat para o líder {self.nos.index(self.lider)}")

    def EleicaoLoop(self):
        while not self.eleicao_event.wait(TEMPO_ELEICAO + random.uniform(0, 0.1)) and not end_event.is_set():
            self.IniciarVotacao()
            exibir_estado(self.estado_atual, self.termo_atual, self.indice_commit, self.lider)
    

'''
cada nó armazena:
NODE
TERM                -> representa o mandato da liderança atual
ÍNDICE DE LOG       -> SET
ÍNDICE DE COMMIT    -> o maior índice do log que já foi replicado pelo quórum
LEADER
'''

'''
Na execução da aplicação devem ser exibidos: o papel do
nó (seguidor/candidato/líder), termo atual, índice de commit.
'''
def exibir_estado(estado_atual, termo_atual, indice_commit, lider=None):
    print(f"\nPapel: {estado_atual}")
    print(f"Termo: {termo_atual}")
    print(f"Índice de Commit: {indice_commit}")
    print(f"Líder: {lider}")

def calcular_porta(id):
    return 50051 + id

# ao ser iniciado, o nó = estado.seguidor

# se não escutam de um líder -> estado.candidato

# ELEIÇÃO DE LÍDER
# requisita votos de outros nós
# respondem com os votos -> voto único por termo (se log candidato >= log próprio)
# se maioria dos votos eleito -> estado.lider

# quorum fixo para cluster 4 nós -> 3 nós


# se SEGUIDOR
# randomized to be between 150ms and 300ms

# append entries (heartbeat timeout) Fetch Request -> avisar que tá vivo ao LÍDER
# enviar termo e último índice
# ex: a cada 100 ms
# recebe resposta -> reinicia temporizador
'''
Loop de Fetch Ativo: busca ativamente entradas no líder atual e
aplica o índice de commit recebido para efetivar os dados no banco local,
tornando-os visíveis para leitura.
'''
# *******ao detectar divergências de log, o nó apaga o log local a partir do ponto conflitante e ressincroniza com o líder atual.

# se consulta de cliente -> retorna com dados comitados

# ELECTION TIMEOUT
# term += 1
# começa new election term -> envia request vote



# se LIDER
# temporizador heartbeat timeout para cada réplica -> se parar, considera-a inativa
# se último fetch request ultrapassar heartbeat timeout: replica = inativa
# se ativas >= quorum: permite escritas do cliente, se não: recusa e informa

# ESCRITA POR CLIENTE   ==== LOG REPLICATION append entries
# Each change is added as an entry in the node's log -> uncommited
# registra novas entradas com seu número de termo atual e índice do log (aumenta o log)
# replica entrada aos seguidores
# espera maioria dos nós escreverem a entrada (quorum fixo) -> commita e indice commit += 1
# novo Fetch Request dos bichinhos ao lider -> grava no banco local
# nofica seguidores entrada commitada
'''
Índice de commit: o maior índice do log que já foi replicado e
armazenado na maioria dos nós (quórum). Dados com índice do log
≤ índice do commit são considerados comitados/efetivados e aplicados
no banco de dados final.
'''

channels = [grpc.insecure_channel(f"localhost:{calcular_porta(id)}") for id in range(QTD_NOS)]

parser = argparse.ArgumentParser(description="No")
parser.add_argument("--id", type=int, required=True, help="ID do nó")

if __name__ == "__main__":
    try:
        args = parser.parse_args()

        if args.id < 0 or args.id >= QTD_NOS:
            print(f"ID do nó deve estar entre 0 e {QTD_NOS - 1}")
            exit(1)

        try:
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            no_pb2_grpc.add_NoServicer_to_server(No(id=args.id), server)
            server.add_insecure_port(f"[::]:{calcular_porta(args.id)}")
            server.start()
            server.wait_for_termination()
        except grpc.RpcError as e:
            print(f"Erro ao iniciar o servidor gRPC: {e}")
            exit(1)
    except KeyboardInterrupt:
        print("\nEncerrando o nó...")
        end_event.set()
        exit(0)
